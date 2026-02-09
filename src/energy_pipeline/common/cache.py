from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

@dataclass(frozen=True)
class CachePaths:
    raw_dir: Path = Path("data/raw")
    processed_dir: Path = Path("data/processed")

def _hash_payload(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()[:16]

def raw_path(source: str, kind: str, params: dict[str, Any], *, ext: str) -> Path:
    h = _hash_payload(params)
    return Path("data/raw") / source / kind / f"{h}.{ext}"

def write_raw_bytes(path: Path, content: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return path

def write_raw_df_parquet(path: Path, df: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    return path
