set -e
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -U pip
pip install pandas pyarrow requests pydantic typer rich matplotlib python-dotenv pytest ruff pyyaml
mkdir -p src/energy_pipeline/{common,ingest,normalize,kpi,report,runner} configs data/{raw,processed,reports,reports/charts} tests
cat <<'YML' > configs/sources.yml
eu:
  source: entsoe
  raw_resolution: hour
  allowed_aggregations: [hour, day, week, month]
global:
  sources:
    eu: entsoe
    us: eia
    cn: nbs_cn
YML
cat <<'YML' > configs/kpi.yml
eu:
  aggregation: day
  dq:
    min_coverage_pct: 98
global:
  aggregation: month
YML
echo "BLOCK 1 OK"
