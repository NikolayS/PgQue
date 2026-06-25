#!/usr/bin/env bash
# End-to-end: provision, then run both Fabrizio cases and print reports.
# Override any knob via env, e.g.  A_WORKERS=16 B_SLOTS=16 bash run.sh
set -Eeuo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB="${PGQUE_DB:-pgque_repro}"

PGQUE_DB="$DB" bash "$HERE/setup.sh"

export PATH="$HOME/.bun/bin:$PATH"
export PGHOST="${PGHOST:-/var/run/postgresql}"
export PGDATABASE="$DB"
export PGUSER="${PGUSER:-$(id -un)}"

drive() { ( cd "$HERE" && bun driver.ts "$@" ); }

echo
echo "############ CASE 1 — migrations: producer idempotency + mutual exclusion ############"
echo "--- 1a: producer dedup OFF — duplicates collapse at consume (advisory lock) ---"
drive --tier a --tenants "${A_TENANTS:-1000}" --producers "${A_PRODUCERS:-4}" \
  --dups "${A_DUPS:-3}" --dedup-ttl 0 --workers "${A_WORKERS:-8}" --work-ms "${A_WORK_MS:-1}"
echo
echo "--- 1b: producer dedup ON (TTL window) — duplicates never inserted ---"
drive --tier a --tenants "${A_TENANTS:-1000}" --producers "${A_PRODUCERS:-4}" \
  --dups "${A_DUPS:-3}" --dedup-ttl "${A_DEDUP_TTL:-60}" --workers "${A_WORKERS:-8}" --work-ms "${A_WORK_MS:-1}"

echo
echo "############ CASE 2 — lifecycle: ordered per tenant, parallel across tenants ############"
drive --tier b --tenants "${B_TENANTS:-500}" --events-per-tenant "${B_EPT:-20}" \
  --slots "${B_SLOTS:-8}"

echo
echo "Done. Tweak scale with A_*/B_* env vars (see README.md)."
