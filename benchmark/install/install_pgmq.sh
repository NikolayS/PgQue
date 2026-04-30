#!/usr/bin/env bash
set -Eeuo pipefail
echo "=== install pgmq v1.11.0 (SQL-only) ==="

curl -fsSL \
  https://raw.githubusercontent.com/tembo-io/pgmq/v1.11.0/pgmq-extension/sql/pgmq.sql \
  -o /tmp/pgmq.sql

sudo -u postgres psql -d bench -f /tmp/pgmq.sql
sudo -u postgres psql -d bench -c "SELECT pgmq.create('bench_queue');"
echo "=== pgmq installed ==="
