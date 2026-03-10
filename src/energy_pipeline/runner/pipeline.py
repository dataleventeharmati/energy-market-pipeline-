from __future__ import annotations

import os
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
from dotenv import load_dotenv

from energy_pipeline.common.cache import raw_path, write_raw_df_parquet
from energy_pipeline.common.config import load_configs
from energy_pipeline.common.log import get_logger
from energy_pipeline.ingest.entsoe import ingest_entsoe_eu_stub
from energy_pipeline.ingest.entsoe_live import (
    fetch_entsoe_day_ahead_price_hourly,
    fetch_entsoe_generation_by_type_hourly,
)
from energy_pipeline.kpi.compute import KPIConfig, compute_kpis
from energy_pipeline.normalize.aggregate import DQConfig, aggregate_timeseries
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
        raise ValueError(f"Invalid agg level: {level}")
    return level  # type: ignore[return-value]


def _eu_live_or_stub(*, countries: list[str], start: str, end: str) -> tuple[pd.DataFrame, str, str]:
    load_dotenv(dotenv_path=Path(".env"))
    token = os.getenv("ENTSOE_TOKEN", "").strip()

    if not token:
        log.warning("ENTSOE_TOKEN missing → using stub")
        df = ingest_entsoe_eu_stub(countries=countries, start=start, end=end)
        return df, "entsoe_stub", "eu_stub"

    parts: list[pd.DataFrame] = []
    used_stub = False

    for iso2 in countries:
        collected: list[pd.DataFrame] = []

        # generation
        try:
            gen = fetch_entsoe_generation_by_type_hourly(
                iso2=iso2, start=start, end=end, token=token
            )
            if not gen.empty:
                collected.append(gen)
        except Exception as e:
            log.warning(
                "ENTSO-E generation fetch failed for %s (%s): %r",
                iso2, type(e).__name__, e
            )

        # price
        try:
            price = fetch_entsoe_day_ahead_price_hourly(
                iso2=iso2, start=start, end=end, token=token
            )
            if not price.empty:
                collected.append(price)
        except Exception as e:
            log.warning(
                "ENTSO-E price fetch failed for %s (%s): %r",
                iso2, type(e).__name__, e
            )

        # fallback only if NOTHING came back
        if not collected:
            log.info("Fallback to stub for country=%s (no live data)", iso2)
            parts.append(
                ingest_entsoe_eu_stub(countries=[iso2], start=start, end=end)
            )
            used_stub = True
        else:
            parts.extend(collected)

    if not parts:
        log.warning("ENTSO-E live returned no data → fallback to stub")
        df = ingest_entsoe_eu_stub(countries=countries, start=start, end=end)
        return df, "entsoe_stub", "eu_stub"

    df = pd.concat(parts, ignore_index=True)
    return df, "entsoe", ("eu_live_with_stub" if used_stub else "eu_live")

def run_pipeline(
    *,
    mode: Mode,
    countries: list[str] | None,
    start: str,
    end: str,
    agg_override: str | None,
    configs_dir: Path,
) -> dict:
    _, cfg_kpi = load_configs(configs_dir)
    agg_level = _resolve_agg_level(cfg_kpi, mode=mode, agg_override=agg_override)

    log.info(
        "Run started: mode=%s start=%s end=%s agg=%s countries=%s",
        mode, start, end, agg_level, countries
    )

    if mode == "eu":
        raw_df, raw_source, raw_kind = _eu_live_or_stub(
            countries=countries or ["DE"], start=start, end=end
        )
    else:
        from energy_pipeline.ingest.ember import ember_to_long_kpi, fetch_ember_monthly
        ember_df = fetch_ember_monthly()
        raw_df = ember_to_long_kpi(ember_df, configs_dir=configs_dir)
        raw_source, raw_kind = "ember", "monthly_kpi"

    params = {
        "mode": mode,
        "countries": countries,
        "start": start,
        "end": end,
        "agg": agg_level,
        "kind": raw_kind,
    }
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
    outputs["raw_source"] = raw_source
    outputs["raw_kind"] = raw_kind

    log.info("Run finished. Outputs: %s", outputs)
    return outputs
