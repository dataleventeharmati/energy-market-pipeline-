from __future__ import annotations

from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

def _save_fig(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(path, dpi=160)
    plt.close()

def chart_energy_mix(kpi: pd.DataFrame, *, region: str, country: str, out_dir: Path) -> Path | None:
    df = kpi[(kpi["region"] == region) & (kpi["country"] == country)].copy()
    if df.empty:
        return None
    df["period_start_utc"] = pd.to_datetime(df["period_start_utc"], utc=True)
    df = df.sort_values("period_start_utc")

    plt.figure()
    plt.plot(df["period_start_utc"], df["renewable_share_pct"], label="renewable_share_pct")
    plt.plot(df["period_start_utc"], df["fossil_share_pct"], label="fossil_share_pct")
    plt.plot(df["period_start_utc"], df["nuclear_share_pct"], label="nuclear_share_pct")
    plt.title(f"Energy mix shares over time ({region}-{country})")
    plt.xlabel("period_start_utc")
    plt.ylabel("share_pct")
    plt.legend()

    out = out_dir / f"energy_mix_{region}_{country}.png"
    _save_fig(out)
    return out

def chart_price_vs_mix(kpi: pd.DataFrame, *, region: str, country: str, out_dir: Path) -> Path | None:
    df = kpi[(kpi["region"] == region) & (kpi["country"] == country)].copy()
    if df.empty or "avg_price" not in df.columns:
        return None

    plt.figure()
    plt.scatter(df["renewable_share_pct"], df["avg_price"])
    plt.title(f"Avg price vs renewable share ({region}-{country})")
    plt.xlabel("renewable_share_pct")
    plt.ylabel("avg_price")

    out = out_dir / f"price_vs_mix_{region}_{country}.png"
    _save_fig(out)
    return out

def chart_global_comparison(kpi: pd.DataFrame, *, metric_col: str, out_dir: Path) -> Path | None:
    df = kpi.copy()
    if df.empty or metric_col not in df.columns:
        return None

    g = df.groupby(["region", "period_start_utc"]).agg(val=(metric_col, "mean")).reset_index()
    g["period_start_utc"] = pd.to_datetime(g["period_start_utc"], utc=True)

    plt.figure()
    for region in sorted(g["region"].unique()):
        sub = g[g["region"] == region].sort_values("period_start_utc")
        plt.plot(sub["period_start_utc"], sub["val"], label=region)

    plt.title(f"Regional comparison over time ({metric_col})")
    plt.xlabel("period_start_utc")
    plt.ylabel(metric_col)
    plt.legend()

    out = out_dir / f"global_comparison_{metric_col}.png"
    _save_fig(out)
    return out
