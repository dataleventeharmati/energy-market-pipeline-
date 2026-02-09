from __future__ import annotations
from dataclasses import dataclass
from typing import Literal
import pandas as pd

Period = Literal["hour", "day", "week", "month"]

@dataclass(frozen=True)
class KPIConfig:
    period: Period
    price_unit_preferred: str = "EUR/MWh"

def compute_kpis(ts: pd.DataFrame, dq_bucket: pd.DataFrame | None, *, cfg: KPIConfig) -> pd.DataFrame:
    if ts.empty:
        return pd.DataFrame()

    df = ts.copy()
    df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)

    key = ["ts_utc", "region", "country"]
    period_val = cfg.period

    gen = df[df["metric"] == "generation"].copy()
    if not gen.empty:
        gen = gen[gen["energy_source"].isin(["renewable", "fossil", "nuclear"])]
        pivot = (
            gen.groupby(key + ["energy_source"])
            .agg(mwh=("value", "sum"))
            .reset_index()
            .pivot_table(index=key, columns="energy_source", values="mwh", fill_value=0.0)
            .reset_index()
        )
        for col in ["renewable", "fossil", "nuclear"]:
            if col not in pivot.columns:
                pivot[col] = 0.0
        pivot = pivot.rename(columns={"renewable": "renewable_mwh", "fossil": "fossil_mwh", "nuclear": "nuclear_mwh"})
        pivot["total_generation_mwh"] = pivot["renewable_mwh"] + pivot["fossil_mwh"] + pivot["nuclear_mwh"]
        total = pivot["total_generation_mwh"].replace({0.0: pd.NA})
        pivot["renewable_share_pct"] = (pivot["renewable_mwh"] / total) * 100.0
        pivot["fossil_share_pct"] = (pivot["fossil_mwh"] / total) * 100.0
        pivot["nuclear_share_pct"] = (pivot["nuclear_mwh"] / total) * 100.0
    else:
        pivot = pd.DataFrame(columns=key + [
            "renewable_mwh","fossil_mwh","nuclear_mwh","total_generation_mwh",
            "renewable_share_pct","fossil_share_pct","nuclear_share_pct"
        ])

    price = df[df["metric"] == "price"].copy()
    if not price.empty:
        p = (
            price.groupby(key)
            .agg(
                avg_price=("value", "mean"),
                price_volatility=("value", "std"),
                price_unit=("unit", lambda s: s.iloc[0] if len(s) else cfg.price_unit_preferred),
            )
            .reset_index()
        )
    else:
        p = pd.DataFrame(columns=key + ["avg_price", "price_volatility", "price_unit"])

    ni = df[df["metric"] == "net_import"].copy()
    if not ni.empty:
        net = ni.groupby(key).agg(net_import_mwh=("value", "sum")).reset_index()
    else:
        net = pd.DataFrame(columns=key + ["net_import_mwh"])

    keys_union = pd.concat(
        [
            pivot[key] if not pivot.empty else pd.DataFrame(columns=key),
            p[key] if not p.empty else pd.DataFrame(columns=key),
            net[key] if not net.empty else pd.DataFrame(columns=key),
        ],
        ignore_index=True,
    ).drop_duplicates()

    out = keys_union.merge(pivot, on=key, how="left").merge(p, on=key, how="left").merge(net, on=key, how="left")
    out = out.rename(columns={"ts_utc": "period_start_utc"})
    out["period"] = period_val

    if dq_bucket is not None and not dq_bucket.empty:
        dq = dq_bucket.copy()
        dq["ts_utc"] = pd.to_datetime(dq["ts_utc"], utc=True)
        dq = dq.rename(columns={"ts_utc": "period_start_utc"})
        dq = dq[dq["period"] == period_val]
        dq_agg = (
            dq.groupby(["period_start_utc", "region", "country", "period"])
            .agg(dq_coverage_pct=("coverage_pct", "min"), dq_issue_count=("issue_count", "sum"))
            .reset_index()
        )
        out = out.merge(dq_agg, on=["period_start_utc", "region", "country", "period"], how="left")
    else:
        out["dq_coverage_pct"] = pd.NA
        out["dq_issue_count"] = pd.NA

    for col in ["renewable_mwh", "fossil_mwh", "nuclear_mwh", "total_generation_mwh", "net_import_mwh"]:
        if col in out.columns:
            out[col] = out[col].fillna(0.0)

    return out.sort_values(["region", "country", "period_start_utc"]).reset_index(drop=True)
