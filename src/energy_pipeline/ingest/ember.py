from __future__ import annotations

import re
from pathlib import Path

import pandas as pd
import requests
import yaml


def _extract_csv_url(html: str) -> str | None:
    # best-effort: első .csv link
    m = re.search(r'href="([^"]+\.csv)"', html, flags=re.IGNORECASE)
    if not m:
        return None
    url = m.group(1)
    if url.startswith("/"):
        url = "https://ember-energy.org" + url
    return url

def fetch_ember_monthly() -> pd.DataFrame:
    page = requests.get("https://ember-energy.org/data/monthly-electricity-data/", timeout=60)
    page.raise_for_status()
    csv_url = _extract_csv_url(page.text)
    if not csv_url:
        raise RuntimeError("Could not find CSV URL on Ember page. We'll pin a direct CSV URL next.")
    csv = requests.get(csv_url, timeout=120)
    csv.raise_for_status()
    return pd.read_csv(pd.io.common.BytesIO(csv.content))

def _load_mapping(configs_dir: Path) -> dict:
    p = configs_dir / "mapping.yml"
    if not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}

def ember_to_long_kpi(df: pd.DataFrame, *, configs_dir: Path) -> pd.DataFrame:
    # normalize columns
    cols = [c.lower().strip() for c in df.columns]
    df = df.copy()
    df.columns = cols

    mapping = _load_mapping(configs_dir).get("ember", {}) or {}
    gen_match = [s.lower() for s in (mapping.get("generation_match") or [])]
    price_match = [s.lower() for s in (mapping.get("price_match") or [])]
    rules = mapping.get("energy_source_rules") or {}

    # detect time + geo
    time_col = "date" if "date" in cols else ("month" if "month" in cols else None)
    geo_col = "country" if "country" in cols else ("area" if "area" in cols else ("region" if "region" in cols else None))
    if not time_col or not geo_col:
        raise RuntimeError(f"Unexpected Ember schema: columns={df.columns.tolist()[:40]}")

    # detect variable/value
    value_col = "value" if "value" in cols else None
    var_col = "variable" if "variable" in cols else ("metric" if "metric" in cols else None)

    if not value_col or not var_col:
        id_cols = [time_col, geo_col]
        other = [c for c in cols if c not in id_cols]
        m = df.melt(id_vars=id_cols, value_vars=other, var_name="variable", value_name="value")
        var_col = "variable"
        value_col = "value"
        df = m

    def norm_text(x) -> str:
        return str(x).strip().lower()

    def metric_bucket(var: str) -> str:
        v = var
        if any(k in v for k in gen_match):
            return "generation"
        if any(k in v for k in price_match):
            return "price"
        return "other"

    def source_bucket(var: str) -> str:
        v = var
        for bucket, patterns in rules.items():
            pats = [str(p).lower() for p in patterns or []]
            if any(p in v for p in pats):
                return bucket
        return "all"

    var_series = df[var_col].map(norm_text)
    metric = var_series.map(metric_bucket)
    energy_source = var_series.map(source_bucket)

    out = pd.DataFrame()
    out["ts_utc"] = pd.to_datetime(df[time_col], utc=True, errors="coerce")
    out["period"] = "month"
    out["region"] = "GLOBAL"
    out["country"] = df[geo_col].astype(str)
    out["metric"] = metric
    out["energy_source"] = energy_source
    out["value"] = pd.to_numeric(df[value_col], errors="coerce")
    out["unit"] = df["unit"] if "unit" in df.columns else "unknown"
    out["quality_flag"] = "ok"
    out["source_system"] = "ember"

    out = out.dropna(subset=["ts_utc", "value"])
    # only keep KPI-relevant metrics
    out = out[out["metric"].isin(["generation", "price"])].reset_index(drop=True)
    return out
