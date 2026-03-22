from pathlib import Path

from energy_pipeline.runner.pipeline import run_pipeline

def test_pipeline_runs():
    result = run_pipeline(
        mode="eu",
        countries=["DE"],
        start="2026-01-01",
        end="2026-01-02",
        agg_override="day",
        configs_dir=Path("config"),
    )

    assert result is not None
    assert isinstance(result, dict)
