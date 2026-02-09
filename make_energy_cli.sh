set -e

# 1) pyproject: setuptools + scripts
cat <<'PY' > pyproject.toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "energy-market-pipeline"
version = "0.1.0"
description = "Descriptive energy market pipeline (source-based, audit-friendly)."
requires-python = ">=3.10"
dependencies = [
  "pandas",
  "pyarrow",
  "requests",
  "pydantic",
  "typer",
  "rich",
  "matplotlib",
  "python-dotenv",
  "pyyaml",
]

[project.scripts]
energy = "energy_pipeline.cli:main"

[tool.setuptools]
package-dir = {"" = "src"}

[tool.setuptools.packages.find]
where = ["src"]
PY

# 2) CLI: real subcommand "run"
cat <<'PY' > src/energy_pipeline/cli.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from energy_pipeline.runner.pipeline import run_pipeline

app = typer.Typer(add_completion=False, help="Energy Market Pipeline (descriptive, source-based).")


@app.command("run")
def run_cmd(
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
    app()


if __name__ == "__main__":
    main()
PY

# 3) Install editable into THIS venv
python -m pip install -e .

# 4) Verify where entrypoint is
echo "VENV python: $(which python)"
echo "VENV pip: $(which pip)"
echo "VENV bin listing (energy should appear):"
ls -la .venv/bin | grep -E 'energy$' || true

echo "=== energy --help ==="
.venv/bin/energy --help

echo "=== energy run --help ==="
.venv/bin/energy run --help
