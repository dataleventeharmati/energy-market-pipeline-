from pathlib import Path
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

DATA = Path("data/reports/year_comparison_2015_2024.csv")

st.set_page_config(page_title="EU Year Comparison (2015 vs 2024)", layout="wide")
st.title("EU Year Comparison (2015 → 2024)")

if not DATA.exists():
    st.error(f"Missing file: {DATA}. Run: python scripts/make_year_comparison.py")
    st.stop()

df = pd.read_csv(DATA)

# ---- sidebar filters ----
st.sidebar.header("Filters")

min_delta = float(df["renewable_share_pct_delta"].dropna().min()) if df["renewable_share_pct_delta"].notna().any() else -100.0
max_delta = float(df["renewable_share_pct_delta"].dropna().max()) if df["renewable_share_pct_delta"].notna().any() else 100.0
x_range = st.sidebar.slider("Renewable share Δ (pp)", min_value=min_delta, max_value=max_delta, value=(min_delta, max_delta))

min_price = float(df["avg_price_delta"].dropna().min()) if df["avg_price_delta"].notna().any() else -500.0
max_price = float(df["avg_price_delta"].dropna().max()) if df["avg_price_delta"].notna().any() else 500.0
y_range = st.sidebar.slider("Avg price Δ (EUR/MWh)", min_value=min_price, max_value=max_price, value=(min_price, max_price))

only_price = st.sidebar.checkbox("Only countries with price data", value=True)

# filter
f = df.copy()
if only_price:
    f = f[f["avg_price_delta"].notna()].copy()

f = f[
    f["renewable_share_pct_delta"].fillna(0).between(x_range[0], x_range[1]) &
    f["avg_price_delta"].fillna(0).between(y_range[0], y_range[1])
].copy()

# ---- KPIs ----
c1, c2, c3, c4 = st.columns(4)
c1.metric("Countries (filtered)", int(len(f)))
c2.metric("Max renewables Δ (pp)", f["renewable_share_pct_delta"].max() if "renewable_share_pct_delta" in f else None)
c3.metric("Max price Δ (EUR/MWh)", f["avg_price_delta"].max() if "avg_price_delta" in f else None)
c4.metric("Min price Δ (EUR/MWh)", f["avg_price_delta"].min() if "avg_price_delta" in f else None)

st.divider()

# ---- Scatter plot ----
st.subheader("Scatter: renewables Δ vs avg price Δ (bubble = 2024 total generation)")

plot_df = f[
    f["renewable_share_pct_delta"].notna() &
    f["avg_price_delta"].notna() &
    f["total_generation_mwh_2024"].notna()
].copy()

if plot_df.empty:
    st.warning("No rows to plot with current filters.")
else:
    sizes = (plot_df["total_generation_mwh_2024"] ** 0.5)
    sizes = sizes / sizes.max() * 2500

    fig = plt.figure(figsize=(12, 9))
    plt.scatter(
        plot_df["renewable_share_pct_delta"],
        plot_df["avg_price_delta"],
        s=sizes,
        alpha=0.7,
        edgecolors="black",
        linewidths=0.5,
    )

    for _, r in plot_df.iterrows():
        plt.text(
            r["renewable_share_pct_delta"] + 0.4,
            r["avg_price_delta"] + 0.4,
            str(r["country"]),
            fontsize=9,
        )

    plt.axhline(0, color="grey", linestyle="--", linewidth=1)
    plt.axvline(0, color="grey", linestyle="--", linewidth=1)
    plt.xlabel("Renewable share change (percentage points)")
    plt.ylabel("Average price change (EUR/MWh)")
    plt.grid(alpha=0.3)
    plt.tight_layout()
    st.pyplot(fig, clear_figure=True)

st.divider()

# ---- Tables ----
st.subheader("Top movers")

colA, colB = st.columns(2)

with colA:
    st.markdown("**Top 10 – Renewable share Δ**")
    if "renewable_share_pct_delta" in f:
        t = f.sort_values("renewable_share_pct_delta", ascending=False).head(10)
        st.dataframe(t[["country","renewable_share_pct_2015","renewable_share_pct_2024","renewable_share_pct_delta"]], use_container_width=True)

with colB:
    st.markdown("**Top 10 – Avg price Δ**")
    if "avg_price_delta" in f:
        t = f.sort_values("avg_price_delta", ascending=False).head(10)
        st.dataframe(t[["country","avg_price_2015","avg_price_2024","avg_price_delta"]], use_container_width=True)

st.divider()

st.subheader("Full table (filtered)")
st.dataframe(f, use_container_width=True)

# download
st.download_button(
    "Download filtered CSV",
    data=f.to_csv(index=False).encode("utf-8"),
    file_name="year_comparison_2015_2024_filtered.csv",
    mime="text/csv",
)
