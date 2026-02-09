set -e
source .venv/bin/activate

# 1) Ember config (forrás URL)
cat <<'YML' > configs/sources.yml
eu:
  source: entsoe
  raw_resolution: hour
  allowed_aggregations: [hour, day, week, month]

global:
  source: ember_monthly
  ember:
    url: "https://ember-energy.org/data/monthly-electricity-data/"
YML

# 2) Cache util
cat <<'PY' > src/energy_pipeline/common/cache.py
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

@dataclass(frozen=True)
class CachePaths:
    raw_dir: Path = Path("data/raw")
    processed_dir: Path = Path("data/processed")

def _hash_payload(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]

def raw_path(source: str, kind: str, params: dict[str, Any], *, ext: str) -> Path:
    h = _hash_payload(params)
    return Path("data/raw") / source / kind / f"{h}.{ext}"

def write_raw_bytes(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path

def write_raw_df_parquet(path: Path, df: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path
PY

# 3) Ember ingest (valós adat – első körben “best-effort” letöltés)
cat <<'PY' > src/energy_pipeline/ingest/ember.py
from __future__ import annotations

import re
import requests
import pandas as pd

def _extract_csv_url(html: str) -> str | None:
    # Ember oldalról kinyerünk egy CSV linket (változhat, ezért best-effort).
    # Ha nem található, később fix URL-re állunk át.
    m = re.search(r'href="([^"]+\.csv)"', html, flags=re.IGNORECASE)
    if not m:
        return None
    url = m.group(1)
    if url.startswith("/"):
        url = "https://ember-energy.org" + url
    return url

def fetch_ember_monthly() -> pd.DataFrame:
    page = requests.get("https://ember-energy.org/data/monthly-electricity-data/", timeout=60)
    page.raise_for_status()
    csv_url = _extract_csv_url(page.text)
    if not csv_url:
        raise RuntimeError("Could not find CSV URL on Ember page. We will pin a direct CSV URL next.")
    csv = requests.get(csv_url, timeout=120)
    csv.raise_for_status()
    df = pd.read_csv(pd.io.common.BytesIO(csv.content))
    return df
PY

# 4) Runner: global módban Ember -> egységes long schema + cache raw parquet
cat <<'PY' > src/energy_pipeline/runner/pipeline.py
from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from energy_pipeline.common.config import load_configs
from energy_pipeline.common.log import get_logger
from energy_pipeline.common.cache import raw_path, write_raw_df_parquet
from energy_pipeline.ingest.entsoe import ingest_entsoe_eu_stub
from energy_pipeline.ingest.ember import fetch_ember_monthly
from energy_pipeline.normalize.aggregate import DQConfig, aggregate_timeseries
from energy_pipeline.kpi.compute import KPIConfig, compute_kpis
from energy_pipeline.report.run import run_reports

Mode = Literal["eu", "global"]
AggLevel = Literal["hour", "day", "week", "month"]

log = get_logger(__name__)

def _resolve_agg_level(cfg_kpi: dict, *, mode: Mode, agg_override: Optional[str]) -> AggLevel:
    if mode == "eu":
        default_level = (cfg_kpi.get("eu", {}) or {}).get("aggregation", "day")
    else:
        default_level = (cfg_kpi.get("global", {}) or {}).get("aggregation", "month")

    level = (agg_override or default_level).strip().lower()
    allowed = {"hour", "day", "week", "month"}
    if level not in allowed:
        raise ValueError(f"Invalid agg level: {level}. Allowed: {sorted(allowed)}")
    return level  # type: ignore[return-value]

def _ember_to_long(df: pd.DataFrame) -> pd.DataFrame:
    # Ember dataset oszlopai változhatnak. Itt “defenzíven” alakítunk:
    # elvárjuk, hogy legyen idő + ország/geo + változó + érték jellegű struktúra.
    # Ha máshogy néz ki, a következő lépésben “pineljük” a konkrét CSV sémát.
    cols = [c.lower() for c in df.columns]
    df.columns = cols

    # tipikus jelölések (best-effort)
    time_col = "date" if "date" in cols else ("month" if "month" in cols else None)
    geo_col = "country" if "country" in cols else ("area" if "area" in cols else ("region" if "region" in cols else None))

    if not time_col or not geo_col:
        raise RuntimeError(f"Unexpected Ember schema: columns={df.columns.tolist()[:30]}")

    # próbáljuk megtalálni a value+variable oszlopokat
    value_col = "value" if "value" in cols else None
    var_col = "variable" if "variable" in cols else ("metric" if "metric" in cols else None)

    if not value_col or not var_col:
        # ha wide, akkor melt-eljük a számoszlopokat
        id_cols = [time_col, geo_col]
        num_cols = [c for c in cols if c not in id_cols and pd.api.types.is_numeric_dtype(df[c])]
        if not num_cols:
            # fallback: minden nem id oszlop
            num_cols = [c for c in cols if c not in id_cols]
        m = df.melt(id_vars=id_cols, value_vars=num_cols, var_name="variable", value_name="value")
        var_col = "variable"
        value_col = "value"
        df = m

    out = pd.DataFrame()
    out["ts_utc"] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    out["period"] = "month"
    out["region"] = df.get("region", "GLOBAL")
    out["country"] = df[geo_col].astype(str)
    out["metric"] = df[var_col].astype(str)
    out["energy_source"] = "all"
    out["value"] = pd.to_numeric(df[value_col], errors="coerce")
    out["unit"] = df.get("unit", "unknown")
    out["quality_flag"] = "ok"
    out["source_system"] = "ember"
    out = out.dropna(subset=["ts_utc", "value"])
    return out

def run_pipeline(*, mode: Mode, countries: list[str] | None, start: str, end: str, agg_override: str | None, configs_dir: Path) -> dict:
    sources_cfg, cfg_kpi = load_configs(configs_dir)
    agg_level = _resolve_agg_level(cfg_kpi, mode=mode, agg_override=agg_override)

    log.info("Run started: mode=%s start=%s end=%s agg=%s countries=%s", mode, start, end, agg_level, countries)

    if mode == "eu":
        # EU rész: most még stub, a következő lépésben kötjük rá ENTSO-E tokennel.
        raw_df = ingest_entsoe_eu_stub(countries=(countries or ["DE"]), start=start, end=end)
        raw_source = "entsoe_stub"
        raw_kind = "eu_stub"
    else:
        ember_df = fetch_ember_monthly()
        raw_df = _ember_to_long(ember_df)
        raw_source = "ember"
        raw_kind = "monthly"

    # raw cache (parquet)
    params = {"mode": mode, "countries": countries, "start": start, "end": end, "agg": agg_level, "kind": raw_kind}
    raw_out = raw_path(raw_source, raw_kind, params, ext="parquet")
    write_raw_df_parquet(raw_out, raw_df)

    # DQ + aggregation
    min_cov = float(((cfg_kpi.get("eu", {}) or {}).get("dq", {}) or {}).get("min_coverage_pct", 98))
    dq_cfg = DQConfig(min_coverage_pct=min_cov)

    if agg_level != "hour" and not raw_df.empty:
        ts_out, dq_bucket = aggregate_timeseries(raw_df, level=agg_level, dq=dq_cfg)
    else:
        ts_out = raw_df
        dq_bucket = pd.DataFrame()

    kpi_df = compute_kpis(ts_out, dq_bucket, cfg=KPIConfig(period=agg_level))
    outputs = run_reports(kpi_df, dq_bucket, period=agg_level)

    outputs["raw_cache"] = str(raw_out)
    log.info("Run finished. Outputs: %s", outputs)
    return outputs
PY

python -m pip install -e . -q

echo "RUN_BLOCK4 OK"
