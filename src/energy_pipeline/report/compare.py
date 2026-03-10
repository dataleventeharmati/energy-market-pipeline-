from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import pandas as pd

CompareMode = Literal["abs", "pct"]


@dataclass(frozen=True)
class CompareSpec:
    metric: str               # e.g. "avg_price", "renewable_share_pct"
    year_a: int               # e.g. 2015
    year_b: int               # e.g. 2024
    mode: CompareMode = "pct" # abs or pct


def build_year_compare(df_yearly: pd.DataFrame, spec: CompareSpec) -> pd.DataFrame:
    """
    Input: yearly KPI dataframe with columns: [country_iso2, year, <metric>, ...]
    Output: [country_iso2, year_a, year_b, value_a, value_b, delta_abs, delta_pct]
    """
    required = {"country_iso2", "year", spec.metric}
    missing = required - set(df_yearly.columns)
    if missing:
        raise ValueError(f"Missing columns in yearly df: {sorted(missing)}")

    a = df_yearly[df_yearly["year"] == spec.year_a][["country_iso2", spec.metric]].rename(
        columns={spec.metric: "value_a"}
    )
    b = df_yearly[df_yearly["year"] == spec.year_b][["country_iso2", spec.metric]].rename(
        columns={spec.metric: "value_b"}
    )

    out = a.merge(b, on="country_iso2", how="inner")
    out["year_a"] = spec.year_a
    out["year_b"] = spec.year_b
    out["delta_abs"] = out["value_b"] - out["value_a"]
    out["delta_pct"] = (out["delta_abs"] / out["value_a"]) * 100.0

    # handle div by zero / missing
    out.loc[out["value_a"].isna() | (out["value_a"] == 0), "delta_pct"] = pd.NA

    cols = ["country_iso2", "year_a", "year_b", "value_a", "value_b", "delta_abs", "delta_pct"]
    return out[cols].sort_values("country_iso2").reset_index(drop=True)
