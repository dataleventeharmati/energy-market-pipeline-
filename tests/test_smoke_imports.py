from __future__ import annotations

import importlib


def test_core_modules_import():
    modules = [
        "energy_pipeline",
        "energy_pipeline.cli",
    ]
    for module_name in modules:
        module = importlib.import_module(module_name)
        assert module is not None
