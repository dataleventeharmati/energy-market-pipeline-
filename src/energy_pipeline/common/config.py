from __future__ import annotations
from pathlib import Path
import yaml

def _read_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}

def load_configs(configs_dir: Path) -> tuple[dict, dict]:
    sources = _read_yaml(configs_dir / "sources.yml")
    kpi = _read_yaml(configs_dir / "kpi.yml")
    return sources, kpi
