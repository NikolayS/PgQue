#!/usr/bin/env bash
set -Eeuo pipefail

# Drives the coop_scaling benchmark across a sweep of subconsumer counts
# and renders a chart from the resulting CSV.
#
# Usage:
#   PGQUE_TEST_DSN=postgres://... ./run.sh
#
# Optional env:
#   N_VALUES         space-separated list of subconsumer counts (default "1 2 4 8 16")
#   EVENTS           events per run (default 2000)
#   PAYLOAD          payload bytes (default 64)
#   RUNS             runs per N for the median (default 3)
#   HANDLER_WORK_MS  simulated per-message handler work in ms (default 1.0).
#                    Set to 0 to benchmark the lock-contention-only curve;
#                    a non-zero value is what the docs claim measures.
#   PG_NOTE          free-form text printed in the chart footer

: "${PGQUE_TEST_DSN:?PGQUE_TEST_DSN must be set}"
: "${N_VALUES:=1 2 4 8 16}"
: "${EVENTS:=2000}"
: "${PAYLOAD:=64}"
: "${RUNS:=3}"
: "${HANDLER_WORK_MS:=1.0}"
: "${PG_NOTE:=}"

cd "$(dirname "$0")"

CSV="coop_scaling.csv"
PNG="coop_scaling.png"

echo "subconsumers,events_per_sec,seconds" > "$CSV"

for n in $N_VALUES; do
  echo "running -subconsumers=${n} -events=${EVENTS} -payload=${PAYLOAD} -runs=${RUNS} -handler-work-ms=${HANDLER_WORK_MS}" >&2
  ( cd ../.. && go run ./bench/coop_scaling \
      -subconsumers="$n" -events="$EVENTS" -payload="$PAYLOAD" -runs="$RUNS" \
      -handler-work-ms="$HANDLER_WORK_MS" ) \
      | tee -a "$CSV"
done

echo "" >&2
echo "CSV written to $(pwd)/${CSV}" >&2

# Derive the PG-version + machine note for the chart footer.
PG_VERSION="$(psql "$PGQUE_TEST_DSN" -tAc 'show server_version' 2>/dev/null || echo unknown)"
MACHINE="$(uname -s) $(uname -m)"
FOOTER="PostgreSQL ${PG_VERSION} on ${MACHINE} -- ${EVENTS} events, ${PAYLOAD} B payload, ${HANDLER_WORK_MS} ms/msg handler work"
if [ -n "$PG_NOTE" ]; then
  FOOTER="${FOOTER} -- ${PG_NOTE}"
fi

PYTHON_BIN="${PYTHON:-python3}"
"$PYTHON_BIN" plot.py "$CSV" "$PNG" "$FOOTER"

echo "PNG written to $(pwd)/${PNG}" >&2
