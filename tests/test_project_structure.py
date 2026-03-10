from __future__ import annotations

from pathlib import Path


def test_key_project_files_exist():
    expected = [
        Path("README.md"),
        Path("pyproject.toml"),
        Path(".github/workflows/ci.yml"),
        Path("configs/kpi.yml"),
        Path("configs/mapping.yml"),
        Path("configs/sources.yml"),
        Path("src/energy_pipeline/__init__.py"),
        Path("src/energy_pipeline/cli.py"),
    ]
    for path in expected:
        assert path.exists(), f"Missing expected file: {path}"
