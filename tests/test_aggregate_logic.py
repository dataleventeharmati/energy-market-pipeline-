import pandas as pd
from energy_pipeline.normalize.aggregate import DQConfig, aggregate_timeseries

def test_aggregate_timeseries_basic():
    df = pd.DataFrame({
        "ts_utc": ["2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z", "2026-01-01T00:00:00Z"],
        "region": ["EU", "EU", "EU"],
        "country": ["DE", "DE", "FR"],
        "metric": ["price", "price", "price"],
        "energy_source": ["all", "all", "all"],
        "unit": ["EUR/MWh", "EUR/MWh", "EUR/MWh"],
        "source_system": ["entsoe", "entsoe", "entsoe"],
        "value": [50.0, 100.0, 150.0],
    })

    aggregated_df, dq_df = aggregate_timeseries(df, level="day", dq=DQConfig())

    assert not aggregated_df.empty
    assert not dq_df.empty
    assert "country" in aggregated_df.columns
    assert set(aggregated_df["country"]).issubset({"Germany", "France"})
