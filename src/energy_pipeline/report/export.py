from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from energy_pipeline.config.eu import EU27_NAMES


@dataclass(frozen=True)
class ExportPaths:
    reports_dir: Path = Path("data/reports")
    charts_dir: Path = Path("data/reports/charts")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ensure_dirs(paths: ExportPaths) -> None:
    paths.reports_dir.mkdir(parents=True, exist_ok=True)
    paths.charts_dir.mkdir(parents=True, exist_ok=True)


def _enforce_eu_scope_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    EU-only:
      - keep only EU27 by DISPLAY NAME (values of EU27_NAMES)
      - force region="EU"
    """
    out = df.copy()
    allowed = set(EU27_NAMES.values())
    if "country" in out.columns:
        # NOTE: if country is ISO2, this filter would drop everything.
        # In our pipeline country should be display name; keep as-is.
        out = out[out["country"].isin(allowed)].copy()
    if "region" in out.columns:
        out["region"] = "EU"
    return out


def export_kpi_history_csv(kpi: pd.DataFrame, paths: ExportPaths) -> Path:
    ensure_dirs(paths)
    out = paths.reports_dir / "kpi_history.csv"
    kpi_eu = _enforce_eu_scope_df(kpi) if isinstance(kpi, pd.DataFrame) else kpi
    kpi_eu.to_csv(out, index=False)
    return out


def export_kpi_latest_json(kpi: pd.DataFrame, *, period: str, paths: ExportPaths) -> Path:
    """
    IMPORTANT:
    We export "latest per country" (not latest global date),
    because some countries may have shorter coverage (e.g., stub fallback).
    """
    ensure_dirs(paths)
    out = paths.reports_dir / "kpi_latest.json"

    if kpi is None or kpi.empty:
        payload = {"generated_at_utc": _utc_now_iso(), "period": period, "period_start_utc": None, "items": []}
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return out

    kpi = _enforce_eu_scope_df(kpi)
    if kpi.empty:
        payload = {"generated_at_utc": _utc_now_iso(), "period": period, "period_start_utc": None, "items": []}
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return out

    kpi = kpi.copy()
    kpi["period_start_utc"] = pd.to_datetime(kpi["period_start_utc"], errors="coerce", utc=True)
    kpi = kpi.dropna(subset=["period_start_utc"])

    if kpi.empty:
        payload = {"generated_at_utc": _utc_now_iso(), "period": period, "period_start_utc": None, "items": []}
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return out

    # Latest row per (region,country)
    latest = (
        kpi.sort_values("period_start_utc")
        .groupby(["region", "country"], as_index=False)
        .tail(1)
        .copy()
    )

    cols = [
        "region",
        "country",
        "period_start_utc",
        "total_generation_mwh",
        "renewable_share_pct",
        "fossil_share_pct",
        "nuclear_share_pct",
        "avg_price",
        "price_unit",
        "price_volatility",
        "dq_coverage_pct",
        "dq_issue_count",
    ]
    latest["period_start_utc"] = latest["period_start_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    items = latest.reindex(columns=[c for c in cols if c in latest.columns]).to_dict(orient="records")

    overall_latest_ts = pd.to_datetime(kpi["period_start_utc"], utc=True).max()
    payload = {
        "generated_at_utc": _utc_now_iso(),
        "period": period,
        "period_start_utc": overall_latest_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "items": items,
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out


def export_dq_latest_json(dq_bucket: pd.DataFrame | None, *, period: str, paths: ExportPaths) -> Path:
    """
    DQ latest is also exported as "latest per country" for the given period.
    """
    ensure_dirs(paths)
    out = paths.reports_dir / "dq_report_latest.json"

    if dq_bucket is None or dq_bucket.empty:
        payload = {"generated_at_utc": _utc_now_iso(), "period": period, "summary": {}, "items": []}
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return out

    d = dq_bucket.copy()
    d["ts_utc"] = pd.to_datetime(d["ts_utc"], errors="coerce", utc=True)
    d = d.dropna(subset=["ts_utc"])

    # period filter first
    if "period" in d.columns:
        d = d[d["period"] == period].copy()

    # EU-only
    d = _enforce_eu_scope_df(d)

    if d.empty:
        payload = {"generated_at_utc": _utc_now_iso(), "period": period, "summary": {}, "items": []}
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        return out

    # Latest ts per (region,country)
    latest = (
        d.sort_values("ts_utc")
        .groupby(["region", "country"], as_index=False)
        .tail(1)
        .copy()
    )

    overall_latest_ts = d["ts_utc"].max()

    summary = {
        "min_coverage_pct": float(latest["coverage_pct"].min()) if ("coverage_pct" in latest.columns and not latest.empty) else None,
        "countries_below_98pct": int((latest["coverage_pct"] < 98.0).sum()) if ("coverage_pct" in latest.columns and not latest.empty) else 0,
        "total_issue_count": int(latest["issue_count"].sum()) if ("issue_count" in latest.columns and not latest.empty) else 0,
    }

    cols = ["region", "country", "ts_utc", "coverage_pct", "issue_count"]
    latest["ts_utc"] = latest["ts_utc"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    items = latest.reindex(columns=[c for c in cols if c in latest.columns]).to_dict(orient="records")

    payload = {
        "generated_at_utc": _utc_now_iso(),
        "period": period,
        "ts_utc": overall_latest_ts.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "summary": summary,
        "items": items,
    }
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return out
