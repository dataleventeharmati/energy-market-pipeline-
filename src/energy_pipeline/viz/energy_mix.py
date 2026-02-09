from __future__ import annotations

from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd


def has_real_timeseries_variation(
    df: pd.DataFrame,
    *,
    time_col: str = "period_start_utc",
    value_cols: list[str] | None = None,
    min_points: int = 2,
) -> bool:
    """
    Return True only if dataframe contains a meaningful time-series signal:
    - at least min_points unique timestamps
    - at least one value column varies over time (not constant / not all NaN)
    """
    if df is None or df.empty:
        return False

    if time_col not in df.columns:
        return False

    if df[time_col].nunique() < min_points:
        return False

    if not value_cols:
        return False

    for col in value_cols:
        if col not in df.columns:
            continue
        s = df[col].dropna()
        if len(s) >= min_points and s.nunique() > 1:
            return True

    return False


def plot_energy_mix_global(
    df: pd.DataFrame,
    *,
    country: str,
    out_dir: Path,
) -> None:
    """
    Create GLOBAL energy mix chart ONLY if there is real time-series variation.
    Otherwise: do nothing (no PNG created).
    """
    value_cols = [
        "renewable_share_pct",
        "fossil_share_pct",
        "nuclear_share_pct",
    ]

    if not has_real_timeseries_variation(
        df,
        time_col="period_start_utc",
        value_cols=value_cols,
        min_points=2,
    ):
        # HARD GUARD: do not generate meaningless chart
        return

    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"energy_mix_GLOBAL_{country}.png"

    plt.figure(figsize=(8, 4))
    for col in value_cols:
        if col in df.columns:
            plt.plot(
                df["period_start_utc"],
                df[col],
                label=col.replace("_share_pct", "").replace("_", " ").title(),
            )

    plt.title(f"Energy mix shares over time (GLOBAL – {country})")
    plt.ylabel("Share (%)")
    plt.xlabel("Time")
    plt.legend()
    plt.tight_layout()
    plt.savefig(out_path)
    plt.close()
