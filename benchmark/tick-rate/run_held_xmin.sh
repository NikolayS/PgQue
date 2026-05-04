#!/usr/bin/env bash
# Run the latency bench with and without a held-xmin RR transaction.
# Exits 0 on success.
set -Eeuo pipefail

DSN="${DSN:-host=/var/run/postgresql dbname=pgque_bench user=postgres}"
TICK_PERIOD_MS="${TICK_PERIOD_MS:-100}"
RATE_EPS="${RATE_EPS:-1000}"
DURATION_S="${DURATION_S:-300}"
BASELINE_DURATION_S="${BASELINE_DURATION_S:-60}"

cd "$(dirname "$0")"

echo "=== Baseline: no held-xmin, ${BASELINE_DURATION_S}s @ ${RATE_EPS} ev/s, tick=${TICK_PERIOD_MS}ms ==="
sudo -u postgres python3 bench.py \
  --dsn "$DSN" --scenario held-xmin \
  --tick-period-ms "$TICK_PERIOD_MS" \
  --rate-eps "$RATE_EPS" \
  --duration-s "$BASELINE_DURATION_S" \
  --queue bench_baseline --consumer bench_baseline_c \
  --out-json results-baseline.json

echo ""
echo "=== Starting RR-held-xmin holder in background ==="
sudo -u postgres psql -X -d "$DSN" -v ON_ERROR_STOP=1 \
  -c "BEGIN ISOLATION LEVEL REPEATABLE READ; SELECT pg_backend_pid() AS holder_pid; SELECT pg_sleep($((DURATION_S + 30)));" \
  > holder.log 2>&1 &
HOLDER_PID=$!
echo "holder shell pid: $HOLDER_PID; sleeping 3s for it to settle ..."
sleep 3
echo "backend snapshot of holder:"
sudo -u postgres psql -d "$DSN" -c "select pid, state, query_start, xact_start from pg_stat_activity where application_name like '%psql%' and state in ('idle in transaction','active');" || true

echo ""
echo "=== Held-xmin: ${DURATION_S}s @ ${RATE_EPS} ev/s, tick=${TICK_PERIOD_MS}ms ==="
sudo -u postgres python3 bench.py \
  --dsn "$DSN" --scenario held-xmin \
  --tick-period-ms "$TICK_PERIOD_MS" \
  --rate-eps "$RATE_EPS" \
  --duration-s "$DURATION_S" \
  --queue bench_xmin --consumer bench_xmin_c \
  --out-json results-held-xmin.json

echo ""
echo "=== Killing holder ==="
sudo -u postgres psql -d "$DSN" -c \
  "select pg_terminate_backend(pid) from pg_stat_activity where state = 'idle in transaction' and pid != pg_backend_pid();" || true
wait "$HOLDER_PID" 2>/dev/null || true

echo ""
echo "=== Done. ==="
