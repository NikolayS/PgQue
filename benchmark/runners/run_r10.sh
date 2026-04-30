#!/usr/bin/env bash
# run_r10.sh <system> [duration_s]
# 10m clean + 30m idle-in-tx + 10m recovery = 50m default. -R 2000 producer.
# Adds awa branch (Python producer + consumer) on top of the 7 R9 SQL systems.
set -uo pipefail
SYS=${1:?system}
DUR=${2:-3000}
CLEAN=600
TX=1800

mkdir -p /tmp/bench && chmod 777 /tmp/bench
rm -f /tmp/bench/*

# Per-system pre-run tweaks
case $SYS in
  pgque|pgq) CONS_C=1 ;;
  *)         CONS_C=4 ;;
esac
if [[ "$SYS" == "pgque" ]]; then
  sudo -u postgres psql -d bench -c "UPDATE pgque.queue SET queue_rotation_period='30 seconds'::interval WHERE queue_name='bench_queue';" >/dev/null
fi
if [[ "$SYS" == "pgq" ]]; then
  sudo -u postgres psql -d bench -c "UPDATE pgq.queue SET queue_rotation_period='30 seconds'::interval WHERE queue_name='bench_queue';" >/dev/null
  [[ -f /tmp/pgq_ticker_daemon.py ]] && sudo -u postgres nohup python3 /tmp/pgq_ticker_daemon.py > /tmp/bench/pgq_ticker.log 2>&1 < /dev/null &
  sleep 1
fi

sudo -u postgres psql -d bench -c "SELECT pg_stat_statements_reset()" >/dev/null
sudo -u postgres psql -d bench -c "TRUNCATE ash.sample_0" >/dev/null 2>&1 || true
sudo -u postgres psql -d bench -c "TRUNCATE ash.sample_1" >/dev/null 2>&1 || true

# CPU/IO sampler
python3 /tmp/sys_metrics_sampler.py --interval 10 --duration "$DUR" --device nvme1n1 --out /tmp/bench/sys_metrics.csv > /tmp/bench/sys_metrics.log 2>&1 &
SYSM_PID=$!

# --- Branch: awa uses Python producer/consumer; everyone else uses pgbench --
if [[ "$SYS" == "awa" ]]; then
  export DATABASE_URL='postgres://postgres@127.0.0.1:5432/bench'
  export DURATION="$DUR"
  export RATE=2000
  export WORKERS=4
  sudo -E /opt/awa-venv/bin/python /tmp/producer.py > /tmp/bench/producer.log 2>&1 &
  PROD_PID=$!
  sudo -E /opt/awa-venv/bin/python /tmp/consumer.py > /tmp/bench/consumer.log 2>&1 &
  CONS_PID=$!
else
  pgbench -h 127.0.0.1 -U postgres -d bench -n -f /tmp/producer.sql \
    -c 1 -j 1 -R 2000 -T "$DUR" -P 30 \
    --aggregate-interval=10 --log --log-prefix=/tmp/bench/producer_agg \
    > /tmp/bench/producer.log 2>&1 &
  PROD_PID=$!
  pgbench -h 127.0.0.1 -U postgres -d bench -n -f /tmp/consumer.sql \
    -c $CONS_C -j $CONS_C -T "$DUR" -P 30 \
    --aggregate-interval=10 --log --log-prefix=/tmp/bench/consumer_agg \
    > /tmp/bench/consumer.log 2>&1 &
  CONS_PID=$!
fi

# Phase scheduler: clean → TX → recovery
(
  sleep $CLEAN
  echo "[$(date -u +%FT%TZ)] opening idle_in_tx" >> /tmp/bench/phases.log
  bash /tmp/idle_in_tx.sh > /tmp/bench/idle.log 2>&1 &
  IDLE_PID=$!
  sleep 2
  sudo -u postgres psql -d bench -Atc "SELECT count(*) FROM pg_stat_activity WHERE application_name='idle_in_tx'" >> /tmp/bench/phases.log
  sudo -u postgres psql -d bench -c "VACUUM" > /tmp/bench/vacuum_preTX.txt 2>&1
  sleep $TX
  echo "[$(date -u +%FT%TZ)] closing idle_in_tx" >> /tmp/bench/phases.log
  sudo -u postgres psql -d bench -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='idle_in_tx'" >> /tmp/bench/phases.log
  kill $IDLE_PID 2>/dev/null
  sudo -u postgres psql -d bench -c "VACUUM" > /tmp/bench/vacuum_postTX.txt 2>&1
) &

wait $PROD_PID
wait $CONS_PID 2>/dev/null
kill $SYSM_PID 2>/dev/null
sudo pkill -f idle_in_tx 2>/dev/null || true
[[ "$SYS" == "pgq" ]] && sudo pkill -f pgq_ticker_daemon 2>/dev/null || true

# Dump pg_ash
sudo -u postgres psql -d bench -c "COPY (SELECT sample_time, wait_event, database_name, active_backends, query_id FROM ash.samples(p_interval => '2 hour'::interval, p_limit => 5000000)) TO '/tmp/bench/ash.csv' CSV HEADER" 2>&1 | tee /tmp/bench/ash_copy.log

sudo -u postgres psql -d bench -c "COPY (SELECT query, calls, total_exec_time::bigint, rows FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 100) TO '/tmp/bench/pgss.csv' CSV HEADER"

# events_consumed parsing — works for both pgbench NOTICE and Python NOTICE format
python3 /tmp/parse_events_consumed.py --bench-dir /tmp/bench --bucket 1 --system "$SYS" \
  > /tmp/bench/events_consumed_parse.log 2>&1 || true

echo "=== R10 done: $SYS ==="
touch /tmp/bench/run_done
