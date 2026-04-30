#!/usr/bin/env bash
# install_awa.sh -- install awa (Postgres-native job queue, Python+Rust)
# Uses Python worker. Awa creates its own schema via `awa migrate`.
set -euo pipefail
echo "=== install awa (Python workers) ==="

# Awa needs Python 3.10+ (Ubuntu 24.04 has 3.12 by default)
sudo apt-get install -y -qq python3-pip python3-venv

# Install in a system venv (simpler than user-venv for SSH-orchestrated bench)
sudo python3 -m venv /opt/awa-venv
sudo /opt/awa-venv/bin/pip install --quiet awa-pg awa-cli

# Run migrations against the bench DB (uses local trust auth; no password needed)
export DATABASE_URL='postgres://postgres@127.0.0.1:5432/bench'
sudo -E /opt/awa-venv/bin/awa --database-url "$DATABASE_URL" migrate

# Sanity check
sudo -u postgres psql -d bench -c "\\dn awa" || echo "(awa schema present)"

echo "=== awa installed ==="
