from __future__ import annotations

import pandas as pd

from .charts import chart_energy_mix, chart_global_comparison, chart_price_vs_mix
from .export import (
    ExportPaths,
    export_dq_latest_json,
    export_kpi_history_csv,
    export_kpi_latest_json,
)


def run_reports(kpi: pd.DataFrame, dq_bucket: pd.DataFrame | None, *, period: str, paths: ExportPaths | None = None) -> dict:
    paths = paths or ExportPaths()
    charts_dir = paths.charts_dir

    outputs: dict[str, str] = {}
    outputs["kpi_history_csv"] = str(export_kpi_history_csv(kpi, paths))
    outputs["kpi_latest_json"] = str(export_kpi_latest_json(kpi, period=period, paths=paths))
    outputs["dq_latest_json"] = str(export_dq_latest_json(dq_bucket, period=period, paths=paths))

    # Defensive: only country-level charts if columns exist
    if isinstance(kpi, pd.DataFrame) and (not kpi.empty) and {"region", "country"}.issubset(set(kpi.columns)):
        for (region, country) in kpi[["region", "country"]].drop_duplicates().itertuples(index=False):
            if region != "EU":
                continue

            chart_energy_mix(kpi, region=region, country=country, out_dir=charts_dir)
            chart_price_vs_mix(kpi, region=region, country=country, out_dir=charts_dir)

    # Global comparison charts only if metric columns exist
    if isinstance(kpi, pd.DataFrame) and (not kpi.empty):
        for metric_col in ["avg_price", "renewable_share_pct", "price_volatility"]:
            if metric_col in kpi.columns:
                chart_global_comparison(kpi, metric_col=metric_col, out_dir=charts_dir)

    outputs["charts_dir"] = str(charts_dir)
    return outputs
