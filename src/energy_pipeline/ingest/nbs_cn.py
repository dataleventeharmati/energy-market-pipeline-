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
