#!/usr/bin/env bash
# Snapshot per-cell metrics into CSV every 5s. Args: WORKLOAD, OUT_DIR.
# WORKLOAD = skiplocked | pgque
set -Eeuo pipefail

WORKLOAD="${1:?workload required}"
OUT_DIR="${2:?out dir required}"
PGURI="${PGURI:-postgresql://bench:bench@127.0.0.1:55435/bench}"
INTERVAL="${INTERVAL:-5}"

mkdir -p "$OUT_DIR"
CSV="${OUT_DIR}/metrics.csv"

if [[ ! -f "$CSV" ]]; then
  echo "ts,workload,enqueued,dequeued,n_live_tup,n_dead_tup,vacuum_count,autovacuum_count,total_size_bytes,oldest_xmin_age,slot_xmin" >"$CSV"
fi

# Tables to inspect for bloat: jobs (skiplocked) or pgque.event_* (pgque).
case "$WORKLOAD" in
  skiplocked)
    SQL_BLOAT="
      coalesce(sum(n_live_tup),0)::text || ',' ||
      coalesce(sum(n_dead_tup),0)::text || ',' ||
      coalesce(sum(vacuum_count),0)::text || ',' ||
      coalesce(sum(autovacuum_count),0)::text || ',' ||
      coalesce(sum(pg_total_relation_size(relid)),0)::text
      from pg_stat_user_tables
      where schemaname = 'public' and relname = 'jobs'
    "
    ;;
  pgque)
    SQL_BLOAT="
      coalesce(sum(n_live_tup),0)::text || ',' ||
      coalesce(sum(n_dead_tup),0)::text || ',' ||
      coalesce(sum(vacuum_count),0)::text || ',' ||
      coalesce(sum(autovacuum_count),0)::text || ',' ||
      coalesce(sum(pg_total_relation_size(relid)),0)::text
      from pg_stat_user_tables
      where schemaname = 'pgque' and relname like 'event_%'
    "
    ;;
  *)
    echo "unknown workload: $WORKLOAD" >&2
    exit 1
    ;;
esac

while true; do
  ROW=$(psql "$PGURI" -At -F',' -v ON_ERROR_STOP=1 <<SQL
select
  to_char(now(), 'YYYY-MM-DD"T"HH24:MI:SS') || ',' ||
  '${WORKLOAD}' || ',' ||
  coalesce((select enqueued::text from bench_counters where workload = '${WORKLOAD}'), '0') || ',' ||
  coalesce((select dequeued::text from bench_counters where workload = '${WORKLOAD}'), '0') || ',' ||
  (select ${SQL_BLOAT}) || ',' ||
  coalesce((select extract(epoch from now() - xact_start)::text
            from pg_stat_activity
            where backend_xmin is not null
            order by xact_start asc limit 1), '0') || ',' ||
  coalesce((select max(xmin::text::bigint)::text
            from pg_replication_slots where xmin is not null), 'NULL')
SQL
  )
  echo "$ROW" >>"$CSV"
  sleep "$INTERVAL"
done
