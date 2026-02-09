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
