from __future__ import annotations

import pandas as pd


def ingest_entsoe_eu_stub(*, countries: list[str], start: str, end: str) -> pd.DataFrame:
    rows: list[dict] = []
    hours = pd.date_range(start=start, end=end, freq="h", inclusive="left", tz="UTC")
    for c in countries:
        for ts in hours:
            rows += [
                {"ts_utc": ts, "period": "hour", "region": "EU", "country": c, "metric": "generation", "energy_source": "renewable", "value": 1000.0, "unit": "MWh", "quality_flag": "ok", "source_system": "entsoe"},
                {"ts_utc": ts, "period": "hour", "region": "EU", "country": c, "metric": "generation", "energy_source": "fossil", "value": 800.0, "unit": "MWh", "quality_flag": "ok", "source_system": "entsoe"},
                {"ts_utc": ts, "period": "hour", "region": "EU", "country": c, "metric": "generation", "energy_source": "nuclear", "value": 200.0, "unit": "MWh", "quality_flag": "ok", "source_system": "entsoe"},
                {"ts_utc": ts, "period": "hour", "region": "EU", "country": c, "metric": "price", "energy_source": "all", "value": 90.0, "unit": "EUR/MWh", "quality_flag": "ok", "source_system": "entsoe"},
            ]
    return pd.DataFrame(rows)
