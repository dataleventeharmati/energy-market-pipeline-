set -e
source .venv/bin/activate

# 1) Mapping config (regex alapú bucketelés)
cat <<'YML' > configs/mapping.yml
ember:
  # ha variable/metric szövegben ezek vannak -> metric = generation
  generation_match:
    - "generation"
    - "electricity generation"
    - "power generation"

  # ha ezek vannak -> metric = price (ha létezik a datasetben)
  price_match:
    - "price"
    - "wholesale"
    - "spot"

  # energy_source bucketelés variable/metric szöveg alapján
  energy_source_rules:
    nuclear:
      - "nuclear"
    renewable:
      - "renewable"
      - "wind"
      - "solar"
      - "hydro"
      - "biomass"
      - "geothermal"
    fossil:
      - "fossil"
      - "coal"
      - "gas"
      - "oil"
      - "lignite"
YML

# 2) Ember ingest: stabilabb kinyerés + mapping alkalmazás
cat <<'PY' > src/energy_pipeline/ingest/ember.py
from __future__ import annotations

import re
from pathlib import Path
import requests
import pandas as pd
import yaml

def _extract_csv_url(html: str) -> str | None:
    # best-effort: első .csv link
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
        raise RuntimeError("Could not find CSV URL on Ember page. We'll pin a direct CSV URL next.")
    csv = requests.get(csv_url, timeout=120)
    csv.raise_for_status()
    return pd.read_csv(pd.io.common.BytesIO(csv.content))

def _load_mapping(configs_dir: Path) -> dict:
    p = configs_dir / "mapping.yml"
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}

def ember_to_long_kpi(df: pd.DataFrame, *, configs_dir: Path) -> pd.DataFrame:
    # normalize columns
    cols = [c.lower().strip() for c in df.columns]
    df = df.copy()
    df.columns = cols

    mapping = _load_mapping(configs_dir).get("ember", {}) or {}
    gen_match = [s.lower() for s in (mapping.get("generation_match") or [])]
    price_match = [s.lower() for s in (mapping.get("price_match") or [])]
    rules = mapping.get("energy_source_rules") or {}

    # detect time + geo
    time_col = "date" if "date" in cols else ("month" if "month" in cols else None)
    geo_col = "country" if "country" in cols else ("area" if "area" in cols else ("region" if "region" in cols else None))
    if not time_col or not geo_col:
        raise RuntimeError(f"Unexpected Ember schema: columns={df.columns.tolist()[:40]}")

    # detect variable/value
    value_col = "value" if "value" in cols else None
    var_col = "variable" if "variable" in cols else ("metric" if "metric" in cols else None)

    if not value_col or not var_col:
        id_cols = [time_col, geo_col]
        other = [c for c in cols if c not in id_cols]
        m = df.melt(id_vars=id_cols, value_vars=other, var_name="variable", value_name="value")
        var_col = "variable"
        value_col = "value"
        df = m

    def norm_text(x) -> str:
        return str(x).strip().lower()

    def metric_bucket(var: str) -> str:
        v = var
        if any(k in v for k in gen_match):
            return "generation"
        if any(k in v for k in price_match):
            return "price"
        return "other"

    def source_bucket(var: str) -> str:
        v = var
        for bucket, patterns in rules.items():
            pats = [str(p).lower() for p in patterns or []]
            if any(p in v for p in pats):
                return bucket
        return "all"

    var_series = df[var_col].map(norm_text)
    metric = var_series.map(metric_bucket)
    energy_source = var_series.map(source_bucket)

    out = pd.DataFrame()
    out["ts_utc"] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    out["period"] = "month"
    out["region"] = "GLOBAL"
    out["country"] = df[geo_col].astype(str)
    out["metric"] = metric
    out["energy_source"] = energy_source
    out["value"] = pd.to_numeric(df[value_col], errors="coerce")
    out["unit"] = df["unit"] if "unit" in df.columns else "unknown"
    out["quality_flag"] = "ok"
    out["source_system"] = "ember"

    out = out.dropna(subset=["ts_utc", "value"])
    # only keep KPI-relevant metrics
    out = out[out["metric"].isin(["generation", "price"])].reset_index(drop=True)
    return out
PY

# 3) Pipeline runner: global ág -> ember_to_long_kpi
cat <<'PY' > src/energy_pipeline/runner/pipeline.py
from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from energy_pipeline.common.config import load_configs
from energy_pipeline.common.log import get_logger
from energy_pipeline.common.cache import raw_path, write_raw_df_parquet
from energy_pipeline.ingest.entsoe import ingest_entsoe_eu_stub
from energy_pipeline.ingest.ember import fetch_ember_monthly, ember_to_long_kpi
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

def run_pipeline(*, mode: Mode, countries: list[str] | None, start: str, end: str, agg_override: str | None, configs_dir: Path) -> dict:
    _, cfg_kpi = load_configs(configs_dir)
    agg_level = _resolve_agg_level(cfg_kpi, mode=mode, agg_override=agg_override)

    log.info("Run started: mode=%s start=%s end=%s agg=%s countries=%s", mode, start, end, agg_level, countries)

    if mode == "eu":
        raw_df = ingest_entsoe_eu_stub(countries=(countries or ["DE"]), start=start, end=end)
        raw_source, raw_kind = "entsoe_stub", "eu_stub"
    else:
        ember_df = fetch_ember_monthly()
        raw_df = ember_to_long_kpi(ember_df, configs_dir=configs_dir)
        raw_source, raw_kind = "ember", "monthly_kpi"

    params = {"mode": mode, "countries": countries, "start": start, "end": end, "agg": agg_level, "kind": raw_kind}
    raw_out = raw_path(raw_source, raw_kind, params, ext="parquet")
    write_raw_df_parquet(raw_out, raw_df)

    min_cov = float(((cfg_kpi.get("eu", {}) or {}).get("dq", {}) or {}).get("min_coverage_pct", 98))
    dq_cfg = DQConfig(min_coverage_pct=min_cov)

    if agg_level != "hour" and not raw_df.empty:
        ts_out, dq_bucket = aggregate_timeseries(raw_df, level=agg_level, dq=dq_cfg)
    else:
        ts_out, dq_bucket = raw_df, pd.DataFrame()

    kpi_df = compute_kpis(ts_out, dq_bucket, cfg=KPIConfig(period=agg_level))
    outputs = run_reports(kpi_df, dq_bucket, period=agg_level)

    outputs["raw_cache"] = str(raw_out)
    log.info("Run finished. Outputs: %s", outputs)
    return outputs
PY

python -m pip install -e . -q
echo "RUN_BLOCK6 OK"
