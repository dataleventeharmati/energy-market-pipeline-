from __future__ import annotations

import json
import os
import subprocess
from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st


def _dash_clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """Dashboard display helper: hide all-empty cols, replace NaN with '—'."""
    if df is None or df.empty:
        return df
    out = df.copy()
    # drop all-empty columns
    out = out.dropna(axis=1, how="all")
    # drop all-empty rows
    out = out.dropna(axis=0, how="all")
    # replace NaN for display
    out = out.where(out.notna(), "—")
    return out


from PIL import Image


def _is_valid_png(path: Path) -> bool:
    try:
        if path.stat().st_size == 0:
            return False
        with Image.open(path) as im:
            im.verify()
        return True
    except Exception:
        return False


PROJECT_ROOT = Path(__file__).resolve().parents[3]
REPORTS_DIR = PROJECT_ROOT / "data" / "reports"
CHARTS_DIR = REPORTS_DIR / "charts"
KPI_LATEST = REPORTS_DIR / "kpi_latest.json"
DQ_LATEST = REPORTS_DIR / "dq_report_latest.json"
KPI_HISTORY = REPORTS_DIR / "kpi_history.csv"


def _read_json(p: Path) -> dict | None:
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_csv(p: Path) -> pd.DataFrame | None:
    if not p.exists():
        return None
    try:
        return pd.read_csv(p)
    except Exception:
        return None


def _normalize_countries_for_entsoe_ui(countries: str) -> str:
    """
    For live ENTSO-E via entsoe-py, Germany is typically DE_LU.
    We keep this helper purely as UX hint; pipeline already works with DE_LU.
    """
    parts = [c.strip() for c in countries.split(",") if c.strip()]
    out = []
    for c in parts:
        if c.upper() == "DE":
            out.append("DE_LU")
        else:
            out.append(c)
    return ",".join(out)


def _run_pipeline(mode: str, countries: str, start: str, end: str, agg: str) -> tuple[int, str]:
    cmd = [
        str(PROJECT_ROOT / ".venv" / "bin" / "energy"),
        "run",
        "--mode",
        mode,
        "--start",
        start,
        "--end",
        end,
        "--agg",
        agg,
    ]
    if mode == "eu":
        cmd += ["--countries", countries]

    env = os.environ.copy()
    # Make sure imports resolve in editable mode
    env["PYTHONPATH"] = str(PROJECT_ROOT / "src")

    p = subprocess.run(cmd, cwd=str(PROJECT_ROOT), capture_output=True, text=True, env=env)
    out = (p.stdout or "") + ("\n" + (p.stderr or "") if p.stderr else "")
    return p.returncode, out


st.set_page_config(page_title="Energy Market Pipeline Dashboard", layout="wide")

st.title("⚡ Energy Market Pipeline — Dashboard")
st.caption("Data-only, reproducible pipeline runs + latest reports/charts (ENTSO-E EU live + Ember global).")

# --- Sidebar controls ---
with st.sidebar:
    st.header("Run controls")
    mode = st.selectbox("Mode", ["eu", "global"], index=0)

    today = date.today()
    default_start = "2024-01-01"
    default_end = "2024-01-10"

    if mode == "eu":
        st.markdown("**Countries** (comma-separated). For Germany use **DE_LU** (ENTSO-E bidding zone).")
        countries_in = st.text_input("Countries", value="DE_LU,FR,NL")
    else:
        countries_in = ""

    start = st.text_input("Start (YYYY-MM-DD)", value=default_start)
    end = st.text_input("End (YYYY-MM-DD)", value=default_end)

    agg = st.selectbox("Aggregation", ["hour", "day", "week", "month"], index=1)

    st.divider()
    st.markdown("**Tip:** If you type `DE`, we recommend `DE_LU` for live ENTSO-E.")
    if mode == "eu" and countries_in:
        st.code(_normalize_countries_for_entsoe_ui(countries_in), language="text")

    run_btn = st.button("▶ Run pipeline", use_container_width=True)

# --- Run action ---
if run_btn:
    with st.spinner("Running pipeline..."):
        countries_arg = _normalize_countries_for_entsoe_ui(countries_in) if mode == "eu" else ""
        rc, logs = _run_pipeline(mode, countries_arg, start, end, agg)
    if rc == 0:
        st.success("Run finished ✅")
    else:
        st.error(f"Run failed (exit code {rc}) ❌")

    st.subheader("Run logs")
    st.code(logs[-12000:] if len(logs) > 12000 else logs, language="text")

st.divider()

# --- Load latest outputs ---
kpi_latest = _read_json(KPI_LATEST)
dq_latest = _read_json(DQ_LATEST)
kpi_hist = _read_csv(KPI_HISTORY)

# Layout: KPI cards + DQ + Meta
colA, colB, colC = st.columns([1.2, 1.0, 1.0])

with colA:
    st.subheader("Latest KPI snapshot")
    if not kpi_latest:
        st.info("No kpi_latest.json yet. Run the pipeline once.")
    else:
        # Try to display a few headline numbers if present
        # We stay defensive: show what exists.
        keys = list(kpi_latest.keys())
        # Some common keys we produced earlier in the project
        candidates = [
            "avg_price_eur_mwh",
            "price_volatility",
            "renewable_share_pct",
            "nuclear_share_pct",
            "fossil_share_pct",
            "coverage_pct",
        ]
        shown = 0
        cols = st.columns(3)
        for i, k in enumerate(candidates):
            if k in kpi_latest:
                v = kpi_latest[k]
                cols[i % 3].metric(k.replace("_", " ").title(), f"{v}")
                shown += 1

        if shown == 0:
            st.json(kpi_latest)

with colB:
    st.subheader("Data quality (DQ)")
    if not dq_latest:
        st.info("No dq_report_latest.json yet. Run the pipeline once.")
    else:
        # Show a compact summary
        if isinstance(dq_latest, dict):
            # common keys
            for k in ["min_coverage_pct", "overall_status", "notes", "missing_points", "total_points"]:
                if k in dq_latest:
                    st.write(f"**{k}**: {dq_latest[k]}")
            st.caption("Full DQ JSON below")
            st.json(dq_latest)
        else:
            st.write(dq_latest)

with colC:
    st.subheader("Last run meta")
    # We can parse it from kpi_latest if stored; otherwise show filesystem info
    st.write(f"**Reports dir:** `{REPORTS_DIR}`")
    st.write(f"**Charts dir:** `{CHARTS_DIR}`")
    if KPI_LATEST.exists():
        st.write(f"**kpi_latest.json:** {KPI_LATEST.stat().st_mtime}")
    if DQ_LATEST.exists():
        st.write(f"**dq_report_latest.json:** {DQ_LATEST.stat().st_mtime}")
    if KPI_HISTORY.exists():
        st.write(f"**kpi_history.csv:** {KPI_HISTORY.stat().st_mtime}")

st.divider()

# KPI history table
st.subheader("KPI History")
if kpi_hist is None or kpi_hist.empty:
    st.info("No kpi_history.csv yet (or it's empty). Run the pipeline a few times.")
else:
    st.dataframe(kpi_hist.tail(50), use_container_width=True)

st.divider()

# Charts gallery

st.subheader("Charts gallery")


def _is_valid_png(path: Path) -> bool:
    try:
        if not path.exists() or path.stat().st_size == 0:
            return False
        with Image.open(path) as im:
            im.verify()
        return True
    except Exception:
        return False

# Mode-based chart filtering:
# - EU mode: show only files containing "_EU_"
# - GLOBAL mode: show only files containing "_GLOBAL_"
mode_tag = "_EU_"  # EU-only

if not CHARTS_DIR.exists():
    st.info("No charts directory yet.")
else:
    imgs = sorted([p for p in CHARTS_DIR.glob("*.png") if mode_tag in p.name])
    imgs = [p for p in imgs if _is_valid_png(p)]

    if not imgs:
        st.info(f"No charts found for mode={mode}. Run the pipeline to generate charts.")
    else:
        q = st.text_input("Filter charts (filename contains)", value="")
        if q.strip():
            imgs = [p for p in imgs if q.lower() in p.name.lower()]

        ncols = 3
        rows = [imgs[i:i+ncols] for i in range(0, len(imgs), ncols)]
        for row in rows:
            cols = st.columns(ncols)
            for col, img in zip(cols, row):
                with col:
                    st.image(
                        str(img),
                        caption=img.name,
                        use_container_width=True
                    )
