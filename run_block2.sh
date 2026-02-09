set -e
source .venv/bin/activate

# pyproject
cat <<'PY' > pyproject.toml
[project]
name = "energy-market-pipeline"
version = "0.1.0"
description = "Descriptive energy market pipeline (source-based, audit-friendly)."
requires-python = ">=3.10"
dependencies = [
  "pandas",
  "pyarrow",
  "requests",
  "pydantic",
  "typer",
  "rich",
  "matplotlib",
  "python-dotenv",
  "pyyaml",
]

[project.scripts]
energy = "energy_pipeline.cli:main"

[tool.ruff]
line-length = 100
target-version = "py310"
PY

# package inits
cat <<'PY' > src/energy_pipeline/__init__.py
__all__ = []
PY
for pkg in common ingest normalize kpi report runner; do
  mkdir -p "src/energy_pipeline/$pkg"
  cat <<'PY' > "src/energy_pipeline/$pkg/__init__.py"
__all__ = []
PY
done

# common/log.py
cat <<'PY' > src/energy_pipeline/common/log.py
from __future__ import annotations
import logging

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
PY

# common/config.py
cat <<'PY' > src/energy_pipeline/common/config.py
from __future__ import annotations
from pathlib import Path
import yaml

def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

def load_configs(configs_dir: Path) -> tuple[dict, dict]:
    sources = _read_yaml(configs_dir / "sources.yml")
    kpi = _read_yaml(configs_dir / "kpi.yml")
    return sources, kpi
PY

# ingest/entsoe.py
cat <<'PY' > src/energy_pipeline/ingest/entsoe.py
from __future__ import annotations
import pandas as pd

def ingest_entsoe_eu_stub(*, countries: list[str], start: str, end: str) -> pd.DataFrame:
    rows: list[dict] = []
    hours = pd.date_range(start=start, end=end, freq="H", inclusive="left", tz="UTC")[:48]
    for c in countries:
        for ts in hours:
            rows += [
                {"ts_utc": ts, "period": "hour", "region": "EU", "country": c, "metric": "generation", "energy_source": "renewable", "value": 1000.0, "unit": "MWh", "quality_flag": "ok", "source_system": "entsoe"},
                {"ts_utc": ts, "period": "hour", "region": "EU", "country": c, "metric": "generation", "energy_source": "fossil", "value": 800.0, "unit": "MWh", "quality_flag": "ok", "source_system": "entsoe"},
                {"ts_utc": ts, "period": "hour", "region": "EU", "country": c, "metric": "generation", "energy_source": "nuclear", "value": 200.0, "unit": "MWh", "quality_flag": "ok", "source_system": "entsoe"},
                {"ts_utc": ts, "period": "hour", "region": "EU", "country": c, "metric": "price", "energy_source": "all", "value": 90.0, "unit": "EUR/MWh", "quality_flag": "ok", "source_system": "entsoe"},
            ]
    return pd.DataFrame(rows)
PY

# ingest/eia.py
cat <<'PY' > src/energy_pipeline/ingest/eia.py
from __future__ import annotations
import pandas as pd

def ingest_eia_us_stub(*, start: str, end: str) -> pd.DataFrame:
    rows: list[dict] = []
    days = pd.date_range(start=start, end=end, freq="D", inclusive="left", tz="UTC")[:60]
    for ts in days:
        rows += [
            {"ts_utc": ts, "period": "day", "region": "US", "country": "US", "metric": "generation", "energy_source": "renewable", "value": 50000.0, "unit": "MWh", "quality_flag": "ok", "source_system": "eia"},
            {"ts_utc": ts, "period": "day", "region": "US", "country": "US", "metric": "generation", "energy_source": "fossil", "value": 120000.0, "unit": "MWh", "quality_flag": "ok", "source_system": "eia"},
            {"ts_utc": ts, "period": "day", "region": "US", "country": "US", "metric": "generation", "energy_source": "nuclear", "value": 30000.0, "unit": "MWh", "quality_flag": "ok", "source_system": "eia"},
            {"ts_utc": ts, "period": "day", "region": "US", "country": "US", "metric": "price", "energy_source": "all", "value": 60.0, "unit": "USD/MWh", "quality_flag": "unit_converted", "source_system": "eia"},
        ]
    return pd.DataFrame(rows)
PY

# ingest/nbs_cn.py
cat <<'PY' > src/energy_pipeline/ingest/nbs_cn.py
from __future__ import annotations
import pandas as pd

def ingest_nbs_cn_stub(*, start: str, end: str) -> pd.DataFrame:
    rows: list[dict] = []
    months = pd.date_range(start=start, end=end, freq="MS", inclusive="left", tz="UTC")[:24]
    for ts in months:
        rows += [
            {"ts_utc": ts, "period": "month", "region": "CN", "country": "CN", "metric": "generation", "energy_source": "renewable", "value": 900000.0, "unit": "MWh", "quality_flag": "low_granularity", "source_system": "nbs_cn"},
            {"ts_utc": ts, "period": "month", "region": "CN", "country": "CN", "metric": "generation", "energy_source": "fossil", "value": 2500000.0, "unit": "MWh", "quality_flag": "low_granularity", "source_system": "nbs_cn"},
            {"ts_utc": ts, "period": "month", "region": "CN", "country": "CN", "metric": "generation", "energy_source": "nuclear", "value": 300000.0, "unit": "MWh", "quality_flag": "low_granularity", "source_system": "nbs_cn"},
        ]
    return pd.DataFrame(rows)
PY

# normalize/aggregate.py
cat <<'PY' > src/energy_pipeline/normalize/aggregate.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal
import pandas as pd

AggLevel = Literal["hour", "day", "week", "month"]

FREQ_MAP: dict[AggLevel, str] = {"hour": "H", "day": "D", "week": "W-MON", "month": "MS"}
METRIC_AGG: dict[str, str] = {"generation": "sum", "load": "sum", "net_import": "sum", "price": "mean"}

@dataclass(frozen=True)
class DQConfig:
    min_coverage_pct: float = 98.0

def _ensure_utc_ts(df: pd.DataFrame, col: str = "ts_utc") -> pd.DataFrame:
    out = df.copy()
    out[col] = pd.to_datetime(out[col], utc=True)
    return out

def _expected_points(level: AggLevel, period_start: pd.Timestamp) -> int:
    if level == "hour":
        return 1
    if level == "day":
        return 24
    if level == "week":
        return 24 * 7
    if level == "month":
        next_month = (period_start + pd.offsets.MonthBegin(1))
        days = (next_month - period_start).days
        return days * 24
    raise ValueError(f"Unknown agg level: {level}")

def aggregate_timeseries(df: pd.DataFrame, *, level: AggLevel, dq: DQConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df.copy(), pd.DataFrame()

    df = _ensure_utc_ts(df, "ts_utc")
    freq = FREQ_MAP[level]

    work = df.set_index("ts_utc")
    group_cols = ["region", "country", "metric", "energy_source", "unit", "source_system"]

    parts: list[pd.DataFrame] = []
    for metric, agg in METRIC_AGG.items():
        sub = work[work["metric"] == metric]
        if sub.empty:
            continue
        g = (
            sub.groupby(group_cols)
            .resample(freq)
            .agg(value=("value", agg), actual_points=("value", "count"))
            .reset_index()
        )
        g["period"] = level
        parts.append(g)

    if not parts:
        return df.iloc[0:0].copy(), pd.DataFrame()

    out = pd.concat(parts, ignore_index=True)
    out["expected_points"] = out["ts_utc"].apply(lambda t: _expected_points(level, t))
    out["coverage_pct"] = (out["actual_points"] / out["expected_points"]) * 100.0

    out["quality_flag"] = "ok"
    out.loc[out["coverage_pct"] < dq.min_coverage_pct, "quality_flag"] = "missing_interval"

    aggregated_df = out.drop(columns=["actual_points", "expected_points", "coverage_pct"]).copy()

    dq_df = (
        out.groupby(["ts_utc", "region", "country", "period"])
        .agg(coverage_pct=("coverage_pct", "min"), issue_count=("quality_flag", lambda s: (s != "ok").sum()))
        .reset_index()
    )
    return aggregated_df, dq_df
PY

# kpi/compute.py
cat <<'PY' > src/energy_pipeline/kpi/compute.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Literal
import pandas as pd

Period = Literal["hour", "day", "week", "month"]

@dataclass(frozen=True)
class KPIConfig:
    period: Period
    price_unit_preferred: str = "EUR/MWh"

def compute_kpis(ts: pd.DataFrame, dq_bucket: pd.DataFrame | None, *, cfg: KPIConfig) -> pd.DataFrame:
    if ts.empty:
        return pd.DataFrame()

    df = ts.copy()
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

    key = ["ts_utc", "region", "country"]
    period_val = cfg.period

    gen = df[df["metric"] == "generation"].copy()
    if not gen.empty:
        gen = gen[gen["energy_source"].isin(["renewable", "fossil", "nuclear"])]
        pivot = (
            gen.groupby(key + ["energy_source"])
            .agg(mwh=("value", "sum"))
            .reset_index()
            .pivot_table(index=key, columns="energy_source", values="mwh", fill_value=0.0)
            .reset_index()
        )
        for col in ["renewable", "fossil", "nuclear"]:
            if col not in pivot.columns:
                pivot[col] = 0.0
        pivot = pivot.rename(columns={"renewable": "renewable_mwh", "fossil": "fossil_mwh", "nuclear": "nuclear_mwh"})
        pivot["total_generation_mwh"] = pivot["renewable_mwh"] + pivot["fossil_mwh"] + pivot["nuclear_mwh"]
        total = pivot["total_generation_mwh"].replace({0.0: pd.NA})
        pivot["renewable_share_pct"] = (pivot["renewable_mwh"] / total) * 100.0
        pivot["fossil_share_pct"] = (pivot["fossil_mwh"] / total) * 100.0
        pivot["nuclear_share_pct"] = (pivot["nuclear_mwh"] / total) * 100.0
    else:
        pivot = pd.DataFrame(columns=key + [
            "renewable_mwh","fossil_mwh","nuclear_mwh","total_generation_mwh",
            "renewable_share_pct","fossil_share_pct","nuclear_share_pct"
        ])

    price = df[df["metric"] == "price"].copy()
    if not price.empty:
        p = (
            price.groupby(key)
            .agg(
                avg_price=("value", "mean"),
                price_volatility=("value", "std"),
                price_unit=("unit", lambda s: s.iloc[0] if len(s) else cfg.price_unit_preferred),
            )
            .reset_index()
        )
    else:
        p = pd.DataFrame(columns=key + ["avg_price", "price_volatility", "price_unit"])

    ni = df[df["metric"] == "net_import"].copy()
    if not ni.empty:
        net = ni.groupby(key).agg(net_import_mwh=("value", "sum")).reset_index()
    else:
        net = pd.DataFrame(columns=key + ["net_import_mwh"])

    keys_union = pd.concat(
        [
            pivot[key] if not pivot.empty else pd.DataFrame(columns=key),
            p[key] if not p.empty else pd.DataFrame(columns=key),
            net[key] if not net.empty else pd.DataFrame(columns=key),
        ],
        ignore_index=True,
    ).drop_duplicates()

    out = keys_union.merge(pivot, on=key, how="left").merge(p, on=key, how="left").merge(net, on=key, how="left")
    out = out.rename(columns={"ts_utc": "period_start_utc"})
    out["period"] = period_val

    if dq_bucket is not None and not dq_bucket.empty:
        dq = dq_bucket.copy()
        dq["ts_utc"] = pd.to_datetime(dq["ts_utc"], utc=True)
        dq = dq.rename(columns={"ts_utc": "period_start_utc"})
        dq = dq[dq["period"] == period_val]
        dq_agg = (
            dq.groupby(["period_start_utc", "region", "country", "period"])
            .agg(dq_coverage_pct=("coverage_pct", "min"), dq_issue_count=("issue_count", "sum"))
            .reset_index()
        )
        out = out.merge(dq_agg, on=["period_start_utc", "region", "country", "period"], how="left")
    else:
        out["dq_coverage_pct"] = pd.NA
        out["dq_issue_count"] = pd.NA

    for col in ["renewable_mwh", "fossil_mwh", "nuclear_mwh", "total_generation_mwh", "net_import_mwh"]:
        if col in out.columns:
            out[col] = out[col].fillna(0.0)

    return out.sort_values(["region", "country", "period_start_utc"]).reset_index(drop=True)
PY

echo "BLOCK 2 OK"
