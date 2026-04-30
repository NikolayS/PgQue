#!/usr/bin/env bash
set -Eeuo pipefail
export DEBIAN_FRONTEND=noninteractive

echo "=== [$(hostname)] bootstrap start $(date -u +%FT%TZ) ==="

# Wait for any cloud-init apt locks to clear
for i in $(seq 1 60); do
  if ! sudo fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; then break; fi
  echo "  waiting for dpkg lock..."
  sleep 5
done

sudo apt-get update -qq
sudo apt-get install -y curl gnupg lsb-release build-essential git python3-pip python3-psycopg2 tmux jq -qq

# PGDG
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | sudo gpg --dearmor -o /usr/share/keyrings/postgresql.gpg
echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  | sudo tee /etc/apt/sources.list.d/postgresql.list >/dev/null

sudo apt-get update -qq
sudo apt-get install -y postgresql-18 postgresql-server-dev-18 postgresql-18-cron -qq

sudo pip3 install psycopg2-binary --break-system-packages >/dev/null 2>&1 || true

# pg_ash (pure SQL, no make)
sudo rm -rf /tmp/pg_ash
sudo git clone https://github.com/NikolayS/pg_ash /tmp/pg_ash

# pg-flight-recorder (pure SQL, no make)
sudo rm -rf /tmp/pgfr
sudo git clone https://github.com/NikolayS/pg-flight-recorder /tmp/pgfr
sudo ln -sfn /tmp/pgfr/_record/sql /record_sql
sudo ln -sfn /tmp/pgfr/_analyze/sql /analyze_sql

sudo mkdir -p /data
sudo chown postgres:postgres /data

# ── postgresql.conf tuning (see methodology notes) ───────────────────────────
sudo tee -a /etc/postgresql/18/main/postgresql.conf >/dev/null <<'CONF'

# ── pgq bench tuning (see methodology notes) ─────────────────────────────────
shared_preload_libraries = 'pg_stat_statements,pg_cron'
cron.database_name = 'bench'

shared_buffers = 4GB
effective_cache_size = 12GB

synchronous_commit = off
wal_level = minimal
wal_compression = lz4
max_wal_size = 16GB
checkpoint_completion_target = 0.9

bgwriter_delay = 50ms
bgwriter_lru_maxpages = 400
bgwriter_lru_multiplier = 4.0

random_page_cost = 1.1
effective_io_concurrency = 200
max_connections = 200

max_wal_senders = 0

autovacuum_vacuum_scale_factor   = 0.01
autovacuum_analyze_scale_factor  = 0.01
autovacuum_vacuum_cost_delay     = 2ms

jit = off

listen_addresses = 'localhost'
CONF

# trust local for bench workers
sudo sed -i '/^host.*127.0.0.1.*scram-sha-256/i host all postgres 127.0.0.1/32 trust' \
  /etc/postgresql/18/main/pg_hba.conf

sudo systemctl restart postgresql@18-main

# create bench DB
sudo -u postgres psql -c "CREATE DATABASE bench;"
sudo -u postgres psql -d bench -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements; CREATE EXTENSION IF NOT EXISTS pg_cron;"

# pg_ash install + start sampling
sudo -u postgres psql -d bench -f /tmp/pg_ash/sql/ash-install.sql >/dev/null
sudo -u postgres psql -d bench -c "SELECT * FROM ash.start();" >/dev/null

# pgfr install
sudo -u postgres psql -d bench -f /tmp/pgfr/_record/install.sql >/dev/null 2>&1 || true
sudo -u postgres psql -d bench -f /tmp/pgfr/_analyze/install.sql >/dev/null 2>&1 || true
sudo -u postgres psql -d bench -f /tmp/pgfr/_control/install.sql >/dev/null 2>&1 || true

echo "=== [$(hostname)] bootstrap DONE $(date -u +%FT%TZ) ==="
