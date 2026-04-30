#!/usr/bin/env bash
# Open a long-running REPEATABLE READ transaction that holds xmin horizon
# until killed. Background job; prints PID to stdout.
set -Eeuo pipefail

PGURI="${PGURI:-postgresql://bench:bench@127.0.0.1:55435/bench}"

psql "$PGURI" -v ON_ERROR_STOP=1 <<'SQL' >/tmp/bench-rr.log 2>&1 &
begin isolation level repeatable read;
select pg_sleep(36000);
commit;
SQL

PID=$!
echo "$PID"
