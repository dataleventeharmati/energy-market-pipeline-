import pandas as pd
from energy_pipeline.kpi.compute import KPIConfig, compute_kpis

def test_kpi_basic():
    ts = pd.DataFrame({
        "ts_utc": pd.to_datetime(["2026-01-01T00:00:00Z", "2026-01-01T00:00:00Z"], utc=True),
        "region": ["EU", "EU"],
        "country": ["Germany", "Germany"],
        "metric": ["price", "generation"],
        "energy_source": ["all", "renewable"],
        "unit": ["EUR/MWh", "MWh"],
        "value": [100.0, 500.0],
    })

    dq_bucket = pd.DataFrame({
        "ts_utc": pd.to_datetime(["2026-01-01T00:00:00Z"], utc=True),
        "region": ["EU"],
        "country": ["Germany"],
        "period": ["day"],
        "coverage_pct": [100.0],
        "issue_count": [0],
    })

    result = compute_kpis(ts, dq_bucket, cfg=KPIConfig(period="day"))

    assert not result.empty
    assert "avg_price" in result.columns
    assert result.loc[0, "avg_price"] == 100.0
