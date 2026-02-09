from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# ---------- PATHS ----------
DATA_PATH = Path("data/reports/year_comparison_2015_2024.csv")
OUT_DIR = Path("data/reports/charts")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OUT_PNG = OUT_DIR / "eu_energy_transition_scatter.png"
OUT_SVG = OUT_DIR / "eu_energy_transition_scatter.svg"

# ---------- LOAD ----------
df = pd.read_csv(DATA_PATH)

# only usable rows
df = df[
    df["renewable_share_pct_delta"].notna() &
    df["avg_price_delta"].notna() &
    df["total_generation_mwh_2024"].notna()
].copy()

print("Countries plotted:", len(df))

# ---------- BUBBLE SIZE ----------
sizes = (df["total_generation_mwh_2024"] ** 0.5)
sizes = sizes / sizes.max() * 2500

# ---------- PLOT ----------
plt.figure(figsize=(12, 9))
plt.scatter(
    df["renewable_share_pct_delta"],
    df["avg_price_delta"],
    s=sizes,
    alpha=0.7,
    edgecolors="black",
    linewidths=0.5
)

for _, r in df.iterrows():
    plt.text(
        r["renewable_share_pct_delta"] + 0.4,
        r["avg_price_delta"] + 0.4,
        r["country"],
        fontsize=9
    )

plt.axhline(0, color="grey", linestyle="--", linewidth=1)
plt.axvline(0, color="grey", linestyle="--", linewidth=1)

plt.title(
    "EU Energy Transition (2015 → 2024)\n"
    "Renewables growth vs electricity price change",
    fontsize=16
)
plt.xlabel("Renewable share change (percentage points)")
plt.ylabel("Average electricity price change (EUR/MWh)")
plt.grid(alpha=0.3)
plt.tight_layout()

plt.savefig(OUT_PNG, dpi=160)
plt.savefig(OUT_SVG)
plt.close()

print("Saved:")
print(" -", OUT_PNG)
print(" -", OUT_SVG)
