#!/usr/bin/env bash
# Hold a REPEATABLE READ tx open via psql + pg_sleep. No Python deps.
# SIGTERM / pg_terminate_backend ends it cleanly.
set -u
psql -X -d "host=127.0.0.1 dbname=bench user=postgres application_name=idle_in_tx" -v ON_ERROR_STOP=1 <<'SQL'
\echo '[idle_in_tx] opening tx'
BEGIN ISOLATION LEVEL REPEATABLE READ;
SELECT pg_backend_pid() AS backend_pid \gset
\echo backend_pid = :backend_pid
SELECT 1;
SELECT pg_sleep(999999);
ROLLBACK;
SQL
