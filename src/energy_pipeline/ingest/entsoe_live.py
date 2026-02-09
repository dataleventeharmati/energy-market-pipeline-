from __future__ import annotations

import pandas as pd
from dotenv import load_dotenv
from entsoe import EntsoePandasClient


def _client(token: str) -> EntsoePandasClient:
    return EntsoePandasClient(api_key=token)


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns to strings if needed."""
    out = df.copy()
    if isinstance(out.columns, pd.MultiIndex):
        out.columns = [
            "_".join([str(x) for x in tup if x not in (None, "", " ")])
            for tup in out.columns.to_list()
        ]
    else:
        out.columns = [str(c) for c in out.columns]
    return out


def fetch_entsoe_generation_by_type_hourly(*, iso2: str, start: str, end: str, token: str) -> pd.DataFrame:
    load_dotenv()
    client = _client(token)

    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")

    df = client.query_generation(country_code=iso2, start=start_ts, end=end_ts)

    if df is None or getattr(df, "empty", True):
        return pd.DataFrame()

    if isinstance(df, pd.Series):
        df = df.to_frame("generation")

    df = df.copy()

    # Ensure datetime index UTC
    if isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)

    # Hourly aggregation
    df = df.resample("h").sum(min_count=1)

    # Flatten columns (entsoe-py can return MultiIndex columns)
    df = _flatten_columns(df)

    # Force index column name so reset_index produces 'ts_utc'
    df.index.name = "ts_utc"
    df = df.reset_index()

    if "ts_utc" not in df.columns:
        return pd.DataFrame()

    value_cols = [c for c in df.columns if c != "ts_utc"]
    if not value_cols:
        return pd.DataFrame()

    long = df.melt(
        id_vars=["ts_utc"],
        value_vars=value_cols,
        var_name="fuel_type",
        value_name="value",
    )

    long["period"] = "hour"
    long["region"] = "EU"
    long["country"] = iso2
    long["metric"] = "generation"
    long["unit"] = "MWh"
    long["quality_flag"] = "ok"
    long["source_system"] = "entsoe"

    ft = long["fuel_type"].astype(str).str.lower()
    long["energy_source"] = "all"
    long.loc[ft.str.contains("nuclear", na=False), "energy_source"] = "nuclear"
    long.loc[ft.str.contains("wind|solar|hydro|renewable|biomass|geothermal", na=False), "energy_source"] = "renewable"
    long.loc[ft.str.contains("coal|gas|oil|lignite|fossil", na=False), "energy_source"] = "fossil"

    long = long.drop(columns=["fuel_type"])
    long["ts_utc"] = pd.to_datetime(long["ts_utc"], utc=True, errors="coerce")
    long["value"] = pd.to_numeric(long["value"], errors="coerce")
    long = long.dropna(subset=["ts_utc", "value"]).reset_index(drop=True)
    return long


def fetch_entsoe_day_ahead_price_hourly(*, iso2: str, start: str, end: str, token: str) -> pd.DataFrame:
    load_dotenv()
    client = _client(token)

    start_ts = pd.Timestamp(start, tz="UTC")
    end_ts = pd.Timestamp(end, tz="UTC")

    s = client.query_day_ahead_prices(country_code=iso2, start=start_ts, end=end_ts)
    if s is None or len(s) == 0:
        return pd.DataFrame()

    if isinstance(s, pd.DataFrame):
        s = s.iloc[:, 0]

    df = s.to_frame("value").copy()

    if isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index, utc=True)

    df = df.resample("h").mean()

    df.index.name = "ts_utc"
    df = df.reset_index()

    if "ts_utc" not in df.columns:
        return pd.DataFrame()

    df["period"] = "hour"
    df["region"] = "EU"
    df["country"] = iso2
    df["metric"] = "price"
    df["energy_source"] = "all"
    df["unit"] = "EUR/MWh"
    df["quality_flag"] = "ok"
    df["source_system"] = "entsoe"

    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True, errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df = df.dropna(subset=["ts_utc", "value"]).reset_index(drop=True)
    return df
