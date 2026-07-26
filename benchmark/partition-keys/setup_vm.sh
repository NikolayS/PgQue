#!/usr/bin/env bash
# setup_vm.sh -- minimal bootstrap for the partition-keys bench on a fresh
# Hetzner CCX43 (16 dedicated cores, 64 GiB, local NVMe, Ubuntu 24.04).
#
# Derived from ../install/bootstrap.sh but stripped of AWS-isms: no NVMe
# re-mount, no pg_ash/pgfr. Installs PostgreSQL 18 (PGDG), tunes it for the
# bench, installs bun, and loads pgque from the LOCAL repo checkout into db
# 'bench'. The repo (including sql/pgque.sql and benchmark/) is expected to be
# rsynced to $REPO_DIR first; run this as root over ssh.
#
#   rsync -a --exclude .git ./ root@VM:/root/pgque/
#   ssh root@VM 'bash /root/pgque/benchmark/partition-keys/setup_vm.sh'
set -Eeuo pipefail
export DEBIAN_FRONTEND=noninteractive

REPO_DIR="${REPO_DIR:-/root/pgque}"
PGQUE_SQL="${PGQUE_SQL:-$REPO_DIR/sql/pgque.sql}"
PG_MAJOR="${PG_MAJOR:-18}"
PGDATABASE="${PGDATABASE:-bench}"
CONF="/etc/postgresql/$PG_MAJOR/main/postgresql.conf"
HBA="/etc/postgresql/$PG_MAJOR/main/pg_hba.conf"

echo "=== [$(hostname)] partition-keys setup_vm start $(date -u +%FT%TZ) ==="
[[ -f "$PGQUE_SQL" ]] || { echo "cannot find $PGQUE_SQL -- rsync the repo to $REPO_DIR first" >&2; exit 1; }

# Wait for any cloud-init apt locks to clear.
for _ in $(seq 1 60); do
  if ! fuser /var/lib/dpkg/lock-frontend >/dev/null 2>&1; then break; fi
  echo "  waiting for dpkg lock..."
  sleep 5
done

apt-get update -qq
apt-get install -y -qq curl gnupg lsb-release git python3 python3-psycopg2 unzip

# PGDG
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | gpg --batch --yes --dearmor -o /usr/share/keyrings/postgresql.gpg
echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
  | tee /etc/apt/sources.list.d/postgresql.list >/dev/null
apt-get update -qq
apt-get install -y -qq "postgresql-$PG_MAJOR"

# bun (for the slot-worker driver)
if [[ ! -x "$HOME/.bun/bin/bun" ]]; then
  curl -fsSL https://bun.sh/install | bash
fi
export PATH="$HOME/.bun/bin:$PATH"

# --- postgresql.conf tuning ------------------------------------------------
tee -a "$CONF" >/dev/null <<'CONF'

# ── partition-keys bench tuning ──────────────────────────────────────────────
shared_preload_libraries = 'pg_stat_statements'
shared_buffers = 16GB
effective_cache_size = 48GB
max_connections = 200

synchronous_commit = off
wal_level = minimal
max_wal_senders = 0
max_wal_size = 16GB
checkpoint_timeout = 15min
wal_compression = on

random_page_cost = 1.1
effective_io_concurrency = 200
jit = off
listen_addresses = 'localhost'
CONF

# trust local for bench workers (postgres over 127.0.0.1)
sed -i '/^host.*127.0.0.1.*scram-sha-256/i host all postgres 127.0.0.1/32 trust' "$HBA" || true

systemctl restart "postgresql@$PG_MAJOR-main"

# --- database + pgque ------------------------------------------------------
sudo -u postgres psql -tAc "select 1 from pg_database where datname='$PGDATABASE'" | grep -q 1 \
  || sudo -u postgres psql -c "create database $PGDATABASE;"
sudo -u postgres psql -d "$PGDATABASE" -c "create extension if not exists pg_stat_statements;"

echo "==> installing pgque from $PGQUE_SQL"
# copy to a postgres-readable path: $PGQUE_SQL may live under /root
install -m 0644 "$PGQUE_SQL" /tmp/pgque_install.sql
sudo -u postgres psql -v ON_ERROR_STOP=1 -q -d "$PGDATABASE" -f /tmp/pgque_install.sql >/dev/null
rm -f /tmp/pgque_install.sql

echo "=== [$(hostname)] partition-keys setup_vm DONE $(date -u +%FT%TZ) ==="
echo "Next: cd $REPO_DIR/benchmark/partition-keys && bun install && \\"
echo "      PGUSER=postgres bash run_bench.sh"
