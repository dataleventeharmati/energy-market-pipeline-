from __future__ import annotations

from pathlib import Path
import pandas as pd

from energy_pipeline.runner.pipeline import run_pipeline
from energy_pipeline.config.eu import EU27_ISO2

OUT_PATH = Path("data/reports/year_comparison_2015_2024.csv")

def load_kpi_history() -> pd.DataFrame:
    p = Path("data/reports/kpi_history.csv")
    if not p.exists() or p.stat().st_size < 10:
        return pd.DataFrame()
    return pd.read_csv(p, parse_dates=["period_start_utc"])

def is_stub_like(df: pd.DataFrame) -> pd.Series:
    # tipikus stub minta nálad: avg_price=90, renewables=50, total_gen=336000 (kb 48 óra * 7000-ish skála, a régi stub miatt)
    a = df.get("avg_price")
    r = df.get("renewable_share_pct")
    t = df.get("total_generation_mwh")
    cond = pd.Series(False, index=df.index)
    if a is not None and r is not None:
        cond = cond | ((a == 90.0) & (r == 50.0))
    if t is not None:
        cond = cond | (t == 336000.0)
    return cond

def run_for_year(year: int) -> pd.DataFrame:
    start = f"{year}-01-01"
    end = f"{year}-01-08"   # 7 nap
    run_pipeline(
        mode="eu",
        countries=EU27_ISO2,
        start=start,
        end=end,
        agg_override="day",
        configs_dir=Path("config"),
    )
    df = load_kpi_history()
    if df.empty:
        return df
    df = df[df["region"] == "EU"].copy()
    df["year"] = df["period_start_utc"].dt.year
    df = df[df["year"] == year].copy()
    return df

def main() -> None:
    df15 = run_for_year(2015)
    df24 = run_for_year(2024)

    df = pd.concat([df15, df24], ignore_index=True)
    if df.empty:
        raise SystemExit("No KPI data produced.")

    # stub-gyanús sorok kidobása (mindkét évből)
    df = df[~is_stub_like(df)].copy()

    # KPI-k összehasonlításhoz
    kpis = {
        "avg_price": "mean",
        "renewable_share_pct": "mean",
        "price_volatility": "mean",
        "total_generation_mwh": "sum",
    }

    g = df.groupby(["country", "year"], as_index=False).agg(kpis)

    # pivot: 2015 vs 2024
    p = g.pivot(index="country", columns="year")
    p.columns = [f"{metric}_{year}" for (metric, year) in p.columns]
    p = p.reset_index()

    # delta oszlopok (ha mindkét év megvan)
    for m in kpis.keys():
        a = f"{m}_2015"
        b = f"{m}_2024"
        if a in p.columns and b in p.columns:
            p[f"{m}_delta"] = p[b] - p[a]

    # rendezés: renewables delta, ha van
    if "renewable_share_pct_delta" in p.columns:
        p = p.sort_values("renewable_share_pct_delta", ascending=False)

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    p.to_csv(OUT_PATH, index=False)

    print("Saved:", OUT_PATH)
    print("Rows:", len(p))
    print("Columns:", list(p.columns))

if __name__ == "__main__":
    main()
