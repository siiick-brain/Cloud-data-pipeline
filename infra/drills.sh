#!/usr/bin/env bash
# Recovery drills on a running stack after `make demo`. Each drill uses fresh event ids (its own --seed)
# and checks exact counts. Results go in docs/recovery.md.
set -euo pipefail
set -a; source .env; set +a

exec_c() { docker compose exec -T "$@"; }
psql_q() { exec_c postgres psql -U postgres -d warehouse -Atc "$1"; }
raw_count() { psql_q "SELECT count(*) FROM raw.order_events"; }
now_s() { date +%s; }
# fresh seeds per run so every drill sends event ids the pipeline has never seen
BASE=$(( $(date +%s) % 1000000 )); S1=$((BASE + 1)); S2=$((BASE + 2)); S3=$((BASE + 3))
all_healthy() { [[ $(docker compose ps --format '{{.Status}}' | grep -vc "(healthy)") -eq 0 ]]; }
wait_stable() {  # once the consumer is healthy, wait for 3 equal counts 5s apart, print the count
  local prev=-1 cur same=0
  until docker compose ps consumer --format '{{.Status}}' | grep -q "(healthy)"; do sleep 2; done
  while [[ $same -lt 2 ]]; do
    sleep 5
    cur=$(raw_count 2>/dev/null || echo -1)
    if [[ "$cur" -ge 0 && "$cur" == "$prev" ]]; then same=$((same + 1)); else same=0; fi
    prev=$cur
  done
  echo "$cur"
}
ids_landed() {  # how many of the event ids for (count, seed) are in raw
  exec_c consumer python -c "
import os, psycopg2
from ingest import generator
ids = [e['event_id'] for e in generator.events($1, seed=$2)]
with psycopg2.connect(os.environ['WAREHOUSE_URL']) as c, c.cursor() as cur:
    cur.execute('SELECT count(*) FROM raw.order_events WHERE event_id = ANY(%s::uuid[])', (ids,))
    print(cur.fetchone()[0])"
}

echo "== drill 1: kill the consumer mid batch"
before=$(wait_stable)
exec_c consumer python -m ingest.producer --events 50000 --seed "$S1" > /dev/null
docker compose restart consumer > /dev/null  # fresh process so the kill lands mid stream
until [[ $(raw_count) -gt $((before + 2000)) ]]; do sleep 1; done
docker compose kill consumer > /dev/null
at_kill=$(raw_count)
docker compose start consumer > /dev/null
t0=$(now_s)
after=$(wait_stable)
echo "caught up $(( $(now_s) - t0 ))s after the restart"
echo "landed before kill: $((at_kill - before)) of 50000"
echo "landed after restart: $((after - before)) (expected 50000)"
echo "drill ids found: $(ids_landed 50000 "$S1")"

echo "== drill 2: stop PostgreSQL for 60 seconds"
before=$(wait_stable)
exec_c consumer python -m ingest.producer --events 20000 --seed "$S2" > /dev/null
docker compose stop postgres > /dev/null
sleep 60
t0=$(now_s)
docker compose start postgres > /dev/null
after=$(wait_stable)
echo "recovered in $(( $(now_s) - t0 ))s after postgres came back, landed $((after - before)) (expected 20000)"
until all_healthy; do sleep 2; done
echo "all services healthy $(( $(now_s) - t0 ))s after postgres came back, with no manual restarts"

echo "== drill 3: schema problem, fix and replay the dead-letter topic"
exec_c consumer python -m ingest.producer --events 500 --seed "$S3" --currency usd > /dev/null
sleep 15
echo "landed before replay: $(ids_landed 500 "$S3") (expected 0, lowercase currency fails the schema)"
exec_c consumer python -m ingest.replay_dlq --uppercase-currency
sleep 15
first=$(ids_landed 500 "$S3")
total_first=$(raw_count)
exec_c consumer python -m ingest.replay_dlq --uppercase-currency > /dev/null
sleep 15
echo "after first replay: $first (expected 500)"
echo "after second replay: $(ids_landed 500 "$S3"), raw rows changed by $(( $(raw_count) - total_first )) (expected 0)"

echo "== drill 4: break a Delta table and restore it with time travel"
# one-off container so the trigger JVM keeps its own memory
docker compose run --rm --no-deps -T spark python3 -c "
import time
from delta.tables import DeltaTable
from spark import jobs
spark = jobs.get_spark()
path = jobs.table_path('silver.order_lines')
table = DeltaTable.forPath(spark, path)
good = table.history(1).collect()[0]['version']
rows = spark.read.format('delta').load(path).count()
table.delete()
broken = spark.read.format('delta').load(path).count()
t = time.monotonic()
table.restoreToVersion(good)
restored = spark.read.format('delta').load(path).count()
print(f'silver rows {rows}, after break {broken}, after restore to v{good} {restored} in {time.monotonic() - t:.1f}s')
"
