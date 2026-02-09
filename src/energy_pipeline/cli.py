from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer

from energy_pipeline.runner.pipeline import run_pipeline

app = typer.Typer(add_completion=False, help="Energy Market Pipeline (descriptive, source-based).")


@app.callback(invoke_without_command=True)
def cli(
    mode: str = typer.Option(..., "--mode", help="eu | global"),
    countries: Optional[str] = typer.Option(None, "--countries", help="Comma-separated ISO2 list, e.g. DE,FR,NL"),
    start: str = typer.Option(..., "--start", help="YYYY-MM-DD"),
    end: str = typer.Option(..., "--end", help="YYYY-MM-DD"),
    agg: Optional[str] = typer.Option(None, "--agg", help="hour|day|week|month (aggregation override)"),
    configs_dir: Path = typer.Option(Path("configs"), "--configs-dir", help="Path to configs/"),
) -> None:
    m = mode.strip().lower()
    if m not in {"eu", "global"}:
        raise typer.BadParameter("mode must be 'eu' or 'global'")

    country_list = None
    if countries:
        country_list = [c.strip().upper() for c in countries.split(",") if c.strip()]

    run_pipeline(
        mode=m,  # type: ignore[arg-type]
        countries=country_list,
        start=start,
        end=end,
        agg_override=agg,
        configs_dir=configs_dir,
    )


def main() -> None:
    # Allow the "pretty" UX: `energy run ...`
    # If argv[1] == "run", we drop it and run the same callback-based CLI.
    if len(sys.argv) > 1 and sys.argv[1] == "run":
        sys.argv.pop(1)
    app()


if __name__ == "__main__":
    main()
