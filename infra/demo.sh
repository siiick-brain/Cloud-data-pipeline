#!/usr/bin/env bash
# Full flow on a running stack: catalog, produce, drain, ETL (forced fail once), ELT plus Spark, FinOps.
# Usage: bash infra/demo.sh [events]. Prints timings and counts you can paste into the README.
set -euo pipefail

EVENTS="${1:-100000}"
set -a; source .env; set +a
AIRFLOW="http://127.0.0.1:${AIRFLOW_PORT:-8080}/api/v1"
PROM="http://127.0.0.1:${PROMETHEUS_PORT:-9090}/api/v1/query"
AUTH="${AIRFLOW_ADMIN_USER}:${AIRFLOW_ADMIN_PASSWORD}"
exec_c() { docker compose exec -T "$@"; }
psql_q() { exec_c postgres psql -U postgres -d warehouse -Atc "$1"; }
prom() { curl -fsG "$PROM" --data-urlencode "query=$1" | python3 -c "import json,sys; r=json.load(sys.stdin)['data']['result']; print(int(float(r[0]['value'][1])) if r else 0)"; }
stamp() { echo "[$(( $(date +%s) - START ))s] $*"; }
START=$(date +%s)

stamp "seed master data and load the catalog"
exec_c consumer python -m ingest.seed
exec_c consumer python -m governance.catalog

stamp "produce ${EVENTS} events (2 percent duplicates, 1 percent malformed)"
exec_c consumer python -m ingest.producer --events "$EVENTS" --dup-rate 0.02 --bad-rate 0.01

stamp "wait for the consumer to drain the topic"
prev=-1
while true; do
  now=$(psql_q "SELECT count(*) FROM raw.order_events WHERE load_date = (now() AT TIME ZONE 'UTC')::date")
  [[ "$now" -gt 0 && "$now" == "$prev" ]] && break
  prev=$now; sleep 5
done
stamp "landed ${now} events"

LOGICAL="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
RUN_ID="demo_$(date -u +%Y%m%dT%H%M%S)"
stamp "set FORCE_FAIL=1 so the first ETL try fails on purpose"
curl -fs -u "$AUTH" -H 'Content-Type: application/json' -X POST "$AIRFLOW/variables" \
  -d '{"key": "FORCE_FAIL", "value": "1"}' > /dev/null

for dag in etl_orders elt_gold; do
  curl -fs -u "$AUTH" -H 'Content-Type: application/json' -X POST "$AIRFLOW/dags/$dag/dagRuns" \
    -d "{\"dag_run_id\": \"$RUN_ID\", \"logical_date\": \"$LOGICAL\"}" > /dev/null
done
stamp "triggered etl_orders and elt_gold as ${RUN_ID}"

for dag in etl_orders elt_gold; do
  while true; do
    state=$(curl -fs -u "$AUTH" "$AIRFLOW/dags/$dag/dagRuns/$RUN_ID" | python3 -c "import json,sys; print(json.load(sys.stdin)['state'])")
    [[ "$state" == "success" || "$state" == "failed" ]] && break
    sleep 10
  done
  stamp "$dag finished: $state"
  [[ "$state" == "success" ]] || exit 1
done

stamp "finops: seed floci, cost report, cleanup dry run"
exec_c consumer python -m finops.seed
exec_c consumer python -m finops.cost_report
exec_c consumer python -m finops.cleanup
mkdir -p finops/out
docker compose cp consumer:/app/finops/out/. finops/out/

echo
echo "== results"
echo "events produced:          ${EVENTS}"
echo "raw rows landed:          ${now}"
echo "duplicates rejected:      $(prom 'sum(pipeline_rows_processed_total{stage="consumer",outcome="duplicate"})')"
echo "dead letters:             $(prom 'sum(pipeline_rows_processed_total{stage="consumer",outcome="dead_letter"})')"
echo "fact rows:                $(psql_q "SELECT count(*) FROM dw.fact_order_line")"
echo "orphan fact keys:         $(psql_q "SELECT count(*) FROM dw.fact_order_line f LEFT JOIN dw.dim_customer c USING (customer_key) LEFT JOIN dw.dim_product p USING (product_key) LEFT JOIN dw.dim_store s USING (store_key) LEFT JOIN dw.dim_date d USING (date_key) WHERE c.customer_key IS NULL OR p.product_key IS NULL OR s.store_key IS NULL OR d.date_key IS NULL")"
echo "failed quality checks:    $(psql_q "SELECT count(*) FROM quality.dq_results WHERE NOT passed AND checked_at > now() - interval '1 hour'")"
echo "total seconds:            $(( $(date +%s) - START ))"
