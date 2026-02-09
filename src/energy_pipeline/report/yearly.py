from __future__ import annotations

import pandas as pd


def to_yearly_kpis(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    Input: daily KPI dataframe
      required columns: [country_iso2, date, avg_price] plus optional metrics.
    Output: yearly KPI dataframe
      columns: [country_iso2, year, avg_price, price_volatility]
    """
    required = {"country_iso2", "date", "avg_price"}
    missing = required - set(df_daily.columns)
    if missing:
        raise ValueError(f"Missing columns in daily df: {sorted(missing)}")

    df = df_daily.copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce", utc=True)
    df = df.dropna(subset=["date", "country_iso2"])
    df["year"] = df["date"].dt.year

    g = df.groupby(["country_iso2", "year"], as_index=False)

    out = g.agg(
        avg_price=("avg_price", "mean"),
        price_volatility=("avg_price", "std"),
    )

    # nicer rounding for reports
    out["avg_price"] = out["avg_price"].round(2)
    out["price_volatility"] = out["price_volatility"].round(2)

    return out.sort_values(["country_iso2", "year"]).reset_index(drop=True)
