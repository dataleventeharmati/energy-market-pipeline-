set -e
source .venv/bin/activate

python - <<'PY'
from pathlib import Path
import pandas as pd
import json

raw_root = Path("data/raw/ember/monthly")
files = sorted(raw_root.glob("*.parquet"), key=lambda p: p.stat().st_mtime, reverse=True)
print("Ember raw parquet dir:", raw_root.resolve())
print("Found parquet files:", len(files))
if not files:
    raise SystemExit("No ember parquet found yet. Run: .venv/bin/energy run --mode global ... first")

p = files[0]
print("Using latest:", p.name)

df = pd.read_parquet(p)
print("Shape:", df.shape)
print("Columns:", list(df.columns))

# head sample
sample = df.head(10)
print("\nHEAD(10):")
print(sample.to_string(index=False))

# uniques for key cols (if exist)
summary = {
    "file": p.name,
    "shape": [int(df.shape[0]), int(df.shape[1])],
    "columns": list(df.columns),
}
for col in ["metric", "unit", "country", "region", "period", "energy_source", "source_system"]:
    if col in df.columns:
        vals = df[col].dropna().astype(str).unique().tolist()
        summary[f"unique_{col}_count"] = len(vals)
        summary[f"unique_{col}_sample"] = vals[:50]

out = Path("data/reports/ember_schema_summary.json")
out.parent.mkdir(parents=True, exist_ok=True)
out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
print("\nWrote:", out.resolve())
PY
