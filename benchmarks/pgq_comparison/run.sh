#!/usr/bin/env bash
set -Eeuo pipefail

# PgQue vs PgQ throughput comparison benchmark
#
# Prerequisites:
#   - PostgreSQL 18+ with pg_cron in shared_preload_libraries
#   - cron.database_name = 'bench_pgque'
#   - PgQ source (for pgq_pl_only.sql): git clone https://github.com/pgq/pgq
#   - Recommended tuning applied (see below)
#
# Tuning (restart required for shared_buffers, wal_level):
#   alter system set synchronous_commit = off;
#   alter system set shared_buffers = '2GB';
#   alter system set max_wal_size = '4GB';
#   alter system set wal_level = minimal;
#   alter system set max_wal_senders = 0;
#   alter system set wal_compression = lz4;

DURATION="${1:-600}"  # default 10 minutes
CLIENTS="${2:-8}"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "================================================================"
echo "PgQue vs PgQ — ${DURATION}s per test, ${CLIENTS} clients"
echo "pg_cron ticker every 2s, rotation every 2 min"
echo "================================================================"
echo ""

run_bench() {
  local db="$1" file="$2" label="$3"
  echo "--- ${label} ---"
  pgbench -d "${db}" -f "${file}" \
    -c "${CLIENTS}" -j "${CLIENTS}" \
    -T "${DURATION}" -n -M prepared -P 60 2>&1 \
  | grep -E "^progress:|^tps" \
  | while IFS= read -r line; do
      if echo "$line" | grep -q "^progress:"; then
        local t tps
        t=$(echo "$line" | awk '{print $2}')
        tps=$(echo "$line" | awk '{print $4}')
        printf "  t=%5ss  %8.0f ev/s\n" "$t" "$tps"
      else
        echo "  FINAL: $line"
      fi
    done
  echo ""
}

run_bench bench_pgq   "${DIR}/pgq_insert_2k.sql"   "PgQ PL-only: insert_event() ~2KiB"
run_bench bench_pgque "${DIR}/pgque_insert_2k.sql"  "PgQue: insert_event() ~2KiB"
run_bench bench_pgque "${DIR}/pgque_send_1k.sql"    "PgQue: send() ~1KiB jsonb"

echo "Disk: $(df -h / | tail -1 | awk '{print $4}') free"
