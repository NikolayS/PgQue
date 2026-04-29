#!/usr/bin/env bash
# Run one (workload, scenario) cell.
# Args: WORKLOAD SCENARIO DURATION_SEC
#   WORKLOAD = skiplocked | pgque
#   SCENARIO = s1 | s2
set -Eeuo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "${HERE}/.." && pwd)"

WORKLOAD="${1:?workload required}"
SCENARIO="${2:?scenario required}"
DURATION="${3:-300}"

PGURI="${PGURI:-postgresql://bench:bench@127.0.0.1:55435/bench}"
PRODUCERS="${PRODUCERS:-4}"
CONSUMERS="${CONSUMERS:-4}"
BYSTANDERS="${BYSTANDERS:-2}"
ENQUEUE_RATE="${ENQUEUE_RATE:-200}"  # per producer client per second cap

OUT_DIR="${ROOT}/results/${SCENARIO}-${WORKLOAD}"
rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR"

echo "[run-scenario] workload=$WORKLOAD scenario=$SCENARIO duration=${DURATION}s out=$OUT_DIR"

# Reset counters and the queue / table state.
case "$WORKLOAD" in
  skiplocked)
    psql "$PGURI" -v ON_ERROR_STOP=1 <<'SQL'
truncate table jobs;
update bench_counters set enqueued = 0, dequeued = 0 where workload = 'skiplocked';
SQL
    ;;
  pgque)
    psql "$PGURI" -v ON_ERROR_STOP=1 <<'SQL'
update bench_counters set enqueued = 0, dequeued = 0 where workload = 'pgque';
SQL
    ;;
esac

# Optionally start the xmin holder for S2.
RR_PID=""
if [[ "$SCENARIO" == "s2" ]]; then
  RR_PID=$(bash "${HERE}/block-xmin-rr.sh")
  echo "[run-scenario] xmin RR holder pid=$RR_PID"
  sleep 2
fi

# Start collector.
"${HERE}/collect.sh" "$WORKLOAD" "$OUT_DIR" >"$OUT_DIR/collect.log" 2>&1 &
COLLECT_PID=$!

# Producer + consumer + bystander pgbench workers.
PRODUCER_LOG="$OUT_DIR/producer.log"
CONSUMER_LOG="$OUT_DIR/consumer.log"
BYSTANDER_LOG="$OUT_DIR/bystander.log"

PRODUCER_SCRIPT="${ROOT}/scripts/${WORKLOAD}-producer.bench"
CONSUMER_SCRIPT="${ROOT}/scripts/${WORKLOAD}-consumer.bench"

# Producer: rate-limited to keep enqueue/dequeue near steady state.
pgbench "$PGURI" \
  -f "$PRODUCER_SCRIPT" \
  -c "$PRODUCERS" -j "$PRODUCERS" \
  -T "$DURATION" \
  -P 5 \
  -R "$((PRODUCERS * ENQUEUE_RATE))" \
  >"$PRODUCER_LOG" 2>&1 &
PRODUCER_PID=$!

# Consumer: no rate limit; drain as fast as possible.
pgbench "$PGURI" \
  -f "$CONSUMER_SCRIPT" \
  -c "$CONSUMERS" -j "$CONSUMERS" \
  -T "$DURATION" \
  -P 5 \
  >"$CONSUMER_LOG" 2>&1 &
CONSUMER_PID=$!

# Bystander: measures app query latency on an unrelated table.
pgbench "$PGURI" \
  -f "${ROOT}/scripts/bystander.bench" \
  -c "$BYSTANDERS" -j "$BYSTANDERS" \
  -T "$DURATION" \
  -P 5 \
  -R "$((BYSTANDERS * 50))" \
  --latency-limit=200 \
  >"$BYSTANDER_LOG" 2>&1 &
BYSTANDER_PID=$!

# pgque needs the ticker fired regularly to make events available.
TICKER_PID=""
if [[ "$WORKLOAD" == "pgque" ]]; then
  (
    end=$(( $(date +%s) + DURATION ))
    while [[ $(date +%s) -lt $end ]]; do
      psql "$PGURI" -At -c "select pgque.ticker();" >/dev/null 2>&1 || true
      sleep 1
    done
  ) &
  TICKER_PID=$!
fi

cleanup() {
  set +e
  [[ -n "$RR_PID" ]] && kill "$RR_PID" 2>/dev/null
  [[ -n "$TICKER_PID" ]] && kill "$TICKER_PID" 2>/dev/null
  kill "$COLLECT_PID" 2>/dev/null
  kill "$PRODUCER_PID" 2>/dev/null
  kill "$CONSUMER_PID" 2>/dev/null
  kill "$BYSTANDER_PID" 2>/dev/null
  set -e
}
trap cleanup EXIT

# Wait for the workload to finish (pgbench will exit on -T).
wait "$PRODUCER_PID" || true
wait "$CONSUMER_PID" || true
wait "$BYSTANDER_PID" || true

# Stop collector + ticker + RR holder (cleanup trap will also fire).
[[ -n "$TICKER_PID" ]] && kill "$TICKER_PID" 2>/dev/null || true
kill "$COLLECT_PID" 2>/dev/null || true
[[ -n "$RR_PID" ]] && kill "$RR_PID" 2>/dev/null || true

# Final snapshot of bloat.
psql "$PGURI" -At -F',' -v ON_ERROR_STOP=1 <<SQL >"$OUT_DIR/final-bloat.csv"
select 'workload,table,n_live_tup,n_dead_tup,vacuum_count,autovacuum_count,total_size_bytes' as header
union all
select '${WORKLOAD},' || schemaname || '.' || relname || ',' ||
       n_live_tup::text || ',' ||
       n_dead_tup::text || ',' ||
       vacuum_count::text || ',' ||
       autovacuum_count::text || ',' ||
       pg_total_relation_size(relid)::text
from pg_stat_user_tables
where (schemaname = 'public' and relname = 'jobs')
   or (schemaname = 'pgque' and relname like 'event_%')
order by 1;
SQL

# Final counters.
psql "$PGURI" -At -F',' -v ON_ERROR_STOP=1 \
  -c "select workload, enqueued, dequeued from bench_counters where workload = '${WORKLOAD}'" \
  >"$OUT_DIR/final-counters.csv"

echo "[run-scenario] done. Results in $OUT_DIR"
