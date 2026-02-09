set -e
source .venv/bin/activate

# report/export.py
cat <<'PY' > src/energy_pipeline/report/export.py
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

@dataclass(frozen=True)
class ExportPaths:
    reports_dir: Path = Path("data/reports")
    charts_dir: Path = Path("data/reports/charts")

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

def ensure_dirs(paths: ExportPaths) -> None:
    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    paths.charts_dir.mkdir(parents=True, exist_ok=True)

def export_kpi_history_csv(kpi: pd.DataFrame, paths: ExportPaths) -> Path:
    ensure_dirs(paths)
    out = paths.reports_dir / "kpi_history.csv"
    kpi.to_csv(out, index=False)
    return out

def export_kpi_latest_json(kpi: pd.DataFrame, *, period: str, paths: ExportPaths) -> Path:
    ensure_dirs(paths)
    out = paths.reports_dir / "kpi_latest.json"

    if kpi.empty:
        payload = {"generated_at_utc": _utc_now_iso(), "period": period, "period_start_utc": None, "items": []}
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return out

    latest_ts = pd.to_datetime(kpi["period_start_utc"], utc=True).max()
    latest = kpi[pd.to_datetime(kpi["period_start_utc"], utc=True) == latest_ts].copy()

    cols = [
        "region","country",
        "total_generation_mwh",
        "renewable_share_pct","fossil_share_pct","nuclear_share_pct",
        "avg_price","price_unit","price_volatility",
        "dq_coverage_pct","dq_issue_count",
    ]
    items = latest.reindex(columns=[c for c in cols if c in latest.columns]).to_dict(orient="records")

    payload = {
        "generated_at_utc": _utc_now_iso(),
        "period": period,
        "period_start_utc": latest_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "items": items,
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out

def export_dq_latest_json(dq_bucket: pd.DataFrame | None, *, period: str, paths: ExportPaths) -> Path:
    ensure_dirs(paths)
    out = paths.reports_dir / "dq_report_latest.json"

    if dq_bucket is None or dq_bucket.empty:
        payload = {"generated_at_utc": _utc_now_iso(), "period": period, "summary": {}, "items": []}
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return out

    d = dq_bucket.copy()
    d["ts_utc"] = pd.to_datetime(d["ts_utc"], utc=True)

    latest_ts = d["ts_utc"].max()
    latest = d[(d["ts_utc"] == latest_ts) & (d["period"] == period)].copy()

    summary = {
        "min_coverage_pct": float(latest["coverage_pct"].min()) if not latest.empty else None,
        "countries_below_98pct": int((latest["coverage_pct"] < 98.0).sum()) if "coverage_pct" in latest else 0,
        "total_issue_count": int(latest["issue_count"].sum()) if "issue_count" in latest else 0,
    }

    items = latest[["region","country","coverage_pct","issue_count"]].to_dict(orient="records") if not latest.empty else []
    payload = {"generated_at_utc": _utc_now_iso(), "period": period, "summary": summary, "items": items}
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out
PY

# report/charts.py
cat <<'PY' > src/energy_pipeline/report/charts.py
from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

def _save_fig(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    plt.close()

def chart_energy_mix(kpi: pd.DataFrame, *, region: str, country: str, out_dir: Path) -> Path | None:
    df = kpi[(kpi["region"] == region) & (kpi["country"] == country)].copy()
    if df.empty:
        return None
    df["period_start_utc"] = pd.to_datetime(df["period_start_utc"], utc=True)
    df = df.sort_values("period_start_utc")

    plt.figure()
    plt.plot(df["period_start_utc"], df["renewable_share_pct"], label="renewable_share_pct")
    plt.plot(df["period_start_utc"], df["fossil_share_pct"], label="fossil_share_pct")
    plt.plot(df["period_start_utc"], df["nuclear_share_pct"], label="nuclear_share_pct")
    plt.title(f"Energy mix shares over time ({region}-{country})")
    plt.xlabel("period_start_utc")
    plt.ylabel("share_pct")
    plt.legend()

    out = out_dir / f"energy_mix_{region}_{country}.png"
    _save_fig(out)
    return out

def chart_price_vs_mix(kpi: pd.DataFrame, *, region: str, country: str, out_dir: Path) -> Path | None:
    df = kpi[(kpi["region"] == region) & (kpi["country"] == country)].copy()
    if df.empty or "avg_price" not in df.columns:
        return None

    plt.figure()
    plt.scatter(df["renewable_share_pct"], df["avg_price"])
    plt.title(f"Avg price vs renewable share ({region}-{country})")
    plt.xlabel("renewable_share_pct")
    plt.ylabel("avg_price")

    out = out_dir / f"price_vs_mix_{region}_{country}.png"
    _save_fig(out)
    return out

def chart_global_comparison(kpi: pd.DataFrame, *, metric_col: str, out_dir: Path) -> Path | None:
    df = kpi.copy()
    if df.empty or metric_col not in df.columns:
        return None

    g = df.groupby(["region", "period_start_utc"]).agg(val=(metric_col, "mean")).reset_index()
    g["period_start_utc"] = pd.to_datetime(g["period_start_utc"], utc=True)

    plt.figure()
    for region in sorted(g["region"].unique()):
        sub = g[g["region"] == region].sort_values("period_start_utc")
        plt.plot(sub["period_start_utc"], sub["val"], label=region)

    plt.title(f"Regional comparison over time ({metric_col})")
    plt.xlabel("period_start_utc")
    plt.ylabel(metric_col)
    plt.legend()

    out = out_dir / f"global_comparison_{metric_col}.png"
    _save_fig(out)
    return out
PY

# report/run.py
cat <<'PY' > src/energy_pipeline/report/run.py
from __future__ import annotations
import pandas as pd

from .export import ExportPaths, export_kpi_history_csv, export_kpi_latest_json, export_dq_latest_json
from .charts import chart_energy_mix, chart_price_vs_mix, chart_global_comparison

def run_reports(kpi: pd.DataFrame, dq_bucket: pd.DataFrame | None, *, period: str, paths: ExportPaths | None = None) -> dict:
    paths = paths or ExportPaths()
    charts_dir = paths.charts_dir

    outputs: dict[str, str] = {}
    outputs["kpi_history_csv"] = str(export_kpi_history_csv(kpi, paths))
    outputs["kpi_latest_json"] = str(export_kpi_latest_json(kpi, period=period, paths=paths))
    outputs["dq_latest_json"] = str(export_dq_latest_json(dq_bucket, period=period, paths=paths))

    for (region, country) in kpi[["region", "country"]].drop_duplicates().itertuples(index=False):
        chart_energy_mix(kpi, region=region, country=country, out_dir=charts_dir)
        chart_price_vs_mix(kpi, region=region, country=country, out_dir=charts_dir)

    chart_global_comparison(kpi, metric_col="avg_price", out_dir=charts_dir)
    chart_global_comparison(kpi, metric_col="renewable_share_pct", out_dir=charts_dir)
    chart_global_comparison(kpi, metric_col="price_volatility", out_dir=charts_dir)

    outputs["charts_dir"] = str(charts_dir)
    return outputs
PY

# runner/pipeline.py
cat <<'PY' > src/energy_pipeline/runner/pipeline.py
from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

import pandas as pd

from energy_pipeline.common.config import load_configs
from energy_pipeline.common.log import get_logger
from energy_pipeline.ingest.entsoe import ingest_entsoe_eu_stub
from energy_pipeline.ingest.eia import ingest_eia_us_stub
from energy_pipeline.ingest.nbs_cn import ingest_nbs_cn_stub
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
    else:
        eu_df = ingest_entsoe_eu_stub(countries=(countries or ["DE"]), start=start, end=end)
        us_df = ingest_eia_us_stub(start=start, end=end)
        cn_df = ingest_nbs_cn_stub(start=start, end=end)
        raw_df = pd.concat([eu_df, us_df, cn_df], ignore_index=True)

    ts_clean = raw_df.copy()

    min_cov = float(((cfg_kpi.get("eu", {}) or {}).get("dq", {}) or {}).get("min_coverage_pct", 98))
    dq_cfg = DQConfig(min_coverage_pct=min_cov)

    if agg_level != "hour":
        ts_out, dq_bucket = aggregate_timeseries(ts_clean, level=agg_level, dq=dq_cfg)
    else:
        ts_out = ts_clean
        dq_bucket = pd.DataFrame()

    kpi_df = compute_kpis(ts_out, dq_bucket, cfg=KPIConfig(period=agg_level))
    outputs = run_reports(kpi_df, dq_bucket, period=agg_level)

    log.info("Run finished. Outputs: %s", outputs)
    return outputs
PY

# cli.py
cat <<'PY' > src/energy_pipeline/cli.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from energy_pipeline.runner.pipeline import run_pipeline

app = typer.Typer(add_completion=False, help="Energy Market Pipeline (descriptive, source-based).")

@app.command()
def run(
    mode: str = typer.Option(..., "--mode", help="eu | global"),
    countries: Optional[str] = typer.Option(None, "--countries", help="Comma-separated ISO2 list, e.g. DE,FR,NL"),
    start: str = typer.Option(..., "--start", help="YYYY-MM-DD"),
    end: str = typer.Option(..., "--end", help="YYYY-MM-DD"),
    agg: Optional[str] = typer.Option(None, "--agg", help="hour|day|week|month (aggregation override)"),
    configs_dir: Path = typer.Option(Path("configs"), "--configs-dir", help="Path to configs/"),
) -> None:
    mode = mode.strip().lower()
    if mode not in {"eu", "global"}:
        raise typer.BadParameter("mode must be 'eu' or 'global'")

    country_list = None
    if countries:
        country_list = [c.strip().upper() for c in countries.split(",") if c.strip()]

    run_pipeline(
        mode=mode,  # type: ignore[arg-type]
        countries=country_list,
        start=start,
        end=end,
        agg_override=agg,
        configs_dir=configs_dir,
    )

def main() -> None:
    app()

if __name__ == "__main__":
    main()
PY

# Run
export PYTHONPATH="$(pwd)/src"

echo "=== Running EU (stub) ==="
python -m energy_pipeline.cli run --mode eu --countries DE,FR,NL --start 2024-01-01 --end 2024-01-10 --agg day

echo "=== Running GLOBAL (stub) ==="
python -m energy_pipeline.cli run --mode global --start 2024-01-01 --end 2024-04-01

echo "=== Outputs ==="
ls -la data/reports || true
ls -la data/reports/charts || true

echo "BLOCK 3 OK"
