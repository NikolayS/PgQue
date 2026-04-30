#!/usr/bin/env bash
# run_r7.sh <system> [duration_s]
# run_r7.sh: 1.5h bench (30m clean + 30m idle-in-tx + 30m recovery)
# sys_metrics_sampler.py runs alongside bloat_sampler.py.
# pg_ash ASH + pgfr snapshots copied to CSV at end (schemas preserved).
# Consumer NOTICE format: NOTICE: ev ts=<epoch_s> n=<events>
set -Eeuo pipefail
SYS=${1:?system}
DUR=${2:-5400}  # 1.5h = 5400s

R7_DIR=${R7_DIR:-/tmp/r7}
R6_DIR=${R6_DIR:-/tmp/r6}
# Consumer SQL may be in either /tmp/consumer.sql (pushed), $R7_DIR/consumer_<sys>.sql, or $R6_DIR/consumer_<sys>.sql
for c in "$R7_DIR/consumer_${SYS}.sql" "/tmp/consumer.sql" "$R6_DIR/consumer_${SYS}.sql"; do
  if [[ -f "$c" ]]; then CONSUMER_SQL="$c"; break; fi
done
if [[ -z "${CONSUMER_SQL:-}" ]]; then
  echo "ERROR: no consumer SQL found for $SYS" >&2
  exit 1
fi
echo "consumer: $CONSUMER_SQL"

mkdir -p /tmp/bench && chmod 777 /tmp/bench
rm -f /tmp/bench/*

case $SYS in
  pgque|pgq) CONS_C=1 ;;
  *)         CONS_C=4 ;;
esac

# System-specific pre-run nudges
if [[ "$SYS" == "pgque" ]]; then
  sudo -u postgres psql -d bench -c "UPDATE pgque.queue SET queue_rotation_period='30 seconds'::interval WHERE queue_name='bench_queue';" >/dev/null
fi
if [[ "$SYS" == "pgq" ]]; then
  sudo -u postgres psql -d bench -c "UPDATE pgq.queue SET queue_rotation_period='30 seconds'::interval WHERE queue_name='bench_queue';" >/dev/null
  [[ -f /tmp/pgq_ticker_daemon.py ]] && sudo -u postgres nohup python3 /tmp/pgq_ticker_daemon.py > /tmp/bench/pgq_ticker.log 2>&1 < /dev/null &
  sleep 1
fi

sudo -u postgres psql -d bench -c "SELECT pg_stat_statements_reset()" >/dev/null

# --- samplers ---
python3 /tmp/bloat_sampler.py --system "$SYS" --interval 30 --duration "$DUR" > /tmp/bench/bloat.csv &
BLOAT_PID=$!

# sys_metrics_sampler.py — CPU/mem/disk every 10s, NVMe instance-store device
python3 "$R7_DIR/sys_metrics_sampler.py" --interval 10 --duration "$DUR" --device nvme1n1 --out /tmp/bench/sys_metrics.csv > /tmp/bench/sys_metrics.log 2>&1 &
SYSM_PID=$!

# pgss snapshot cross-check
python3 "$R6_DIR/pg_stat_statements_snapshot.py" \
  --dsn "host=127.0.0.1 dbname=bench user=postgres" \
  --interval 10 --duration "$DUR" \
  --out /tmp/bench/pgss_timeseries.csv \
  > /tmp/bench/pgss_snapshotter.log 2>&1 &
PGSS_PID=$!

# Producer — full run: -R 5000
pgbench -h 127.0.0.1 -U postgres -d bench -n -f /tmp/producer.sql \
  -c 1 -j 1 -R 5000 -T "$DUR" -P 30 \
  --aggregate-interval=10 --log --log-prefix=/tmp/bench/producer_agg \
  > /tmp/bench/producer.log 2>&1 &
PROD_PID=$!

# Consumer (NOTICE-instrumented SQL)
pgbench -h 127.0.0.1 -U postgres -d bench -n -f "$CONSUMER_SQL" \
  -c $CONS_C -j $CONS_C -T "$DUR" -P 30 \
  --aggregate-interval=10 --log --log-prefix=/tmp/bench/consumer_agg \
  > /tmp/bench/consumer.log 2>&1 &
CONS_PID=$!

# Phase scheduler — only open idle_in_tx for long runs. For --duration < 3000s (smoke), skip.
if [[ "$DUR" -ge 3000 ]]; then
(
  sleep 1800
  echo "[$(date -u +%FT%TZ)] opening idle_in_tx" >> /tmp/bench/phases.log
  python3 /tmp/idle_in_tx.py > /tmp/bench/idle.log 2>&1 &
  IDLE_PID=$!
  sudo -u postgres psql -d bench -c "VACUUM VERBOSE" > /tmp/bench/vacuum_preTX.txt 2>&1
  sleep 1800
  echo "[$(date -u +%FT%TZ)] closing idle_in_tx" >> /tmp/bench/phases.log
  kill $IDLE_PID 2>/dev/null
  sudo -u postgres psql -d bench -c "VACUUM VERBOSE" > /tmp/bench/vacuum_postTX.txt 2>&1
) &
fi

wait $PROD_PID
wait $CONS_PID 2>/dev/null
kill $BLOAT_PID 2>/dev/null
kill $SYSM_PID  2>/dev/null
kill $PGSS_PID  2>/dev/null
sudo pkill -f idle_in_tx 2>/dev/null || true
[[ "$SYS" == "pgq" ]] && sudo pkill -f pgq_ticker_daemon 2>/dev/null || true

# End-of-run copies
sudo -u postgres psql -d bench -c "COPY (SELECT sample_time, database_name, active_backends, wait_event, query_id FROM ash.samples(p_interval => '2 hour'::interval, p_limit => 2000000)) TO '/tmp/bench/ash.csv' CSV HEADER" 2>&1 | tee /tmp/bench/ash_copy.log

# pgfr: dump the main snapshot tables (partitioned & v2 forms)
sudo -u postgres psql -d bench -c "COPY (SELECT * FROM pgfr_record.snapshots) TO '/tmp/bench/pgfr_snapshots.csv' CSV HEADER" 2>&1 | tee /tmp/bench/pgfr_snapshots_copy.log
sudo -u postgres psql -d bench -c "COPY (SELECT * FROM pgfr_record.table_snapshots) TO '/tmp/bench/pgfr_table_snapshots.csv' CSV HEADER" 2>&1 | tee /tmp/bench/pgfr_tables_copy.log || true
sudo -u postgres psql -d bench -c "COPY (SELECT * FROM pgfr_record.statement_snapshots) TO '/tmp/bench/pgfr_statement_snapshots.csv' CSV HEADER" 2>&1 | tee /tmp/bench/pgfr_stmts_copy.log || true

sudo -u postgres psql -d bench -c "COPY (SELECT query, calls, total_exec_time::bigint, rows FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 100) TO '/tmp/bench/pgss.csv' CSV HEADER"

# Post-process: produce events_consumed_per_sec.csv
python3 "$R6_DIR/parse_events_consumed.py" \
  --bench-dir /tmp/bench \
  --bucket 1 \
  --system "$SYS" \
  > /tmp/bench/events_consumed_parse.log 2>&1 || true

echo "=== bench done: $SYS ==="
