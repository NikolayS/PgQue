#!/usr/bin/env bash
set -euo pipefail
echo "=== install pgq v3.5.1 ==="

sudo rm -rf /tmp/pgq
sudo git clone --branch v3.5.1 --depth 1 https://github.com/pgq/pgq /tmp/pgq
cd /tmp/pgq
sudo make USE_PGXS=1 PG_CONFIG=/usr/lib/postgresql/18/bin/pg_config >/dev/null
sudo make USE_PGXS=1 install >/dev/null

sudo -u postgres psql -d bench -c "CREATE EXTENSION pgq;"
# Switch to PL-only mode — replace C insert_event_raw with PL/pgSQL version
sudo -u postgres psql -d bench -f /tmp/pgq/sql/switch_plonly.sql

# Verify PL-only
lang=$(sudo -u postgres psql -d bench -Atc \
  "SELECT l.lanname FROM pg_proc p JOIN pg_language l ON p.prolang=l.oid WHERE proname='insert_event_raw' AND pronamespace=(SELECT oid FROM pg_namespace WHERE nspname='pgq');")
if [ "$lang" != "plpgsql" ]; then
  echo "ERROR: pgq still on C (lang=$lang)"; exit 1
fi
echo "pgq PL-only confirmed (lang=$lang)"

sudo -u postgres psql -d bench -c "SELECT pgq.create_queue('bench_queue');"
sudo -u postgres psql -d bench -c "SELECT pgq.ticker('bench_queue');"
sudo -u postgres psql -d bench -c "SELECT pgq.register_consumer('bench_queue', 'bench_consumer');"
# Ticker via pg_cron (60s interval, bench.py also ticks inline)
sudo -u postgres psql -d bench -c "SELECT cron.schedule('pgq-ticker', '* * * * *', \$\$SELECT pgq.ticker()\$\$);"
# And maint (rotation etc)
sudo -u postgres psql -d bench -c "SELECT cron.schedule('pgq-maint', '* * * * *', \$\$SELECT pgq.maint_operations()\$\$);" || true
echo "=== pgq installed ==="
