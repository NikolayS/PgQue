#!/usr/bin/env bash
# Provision a throwaway Postgres + bun project, install pgque core + demo
# schema. Idempotent. Targets Debian/Ubuntu (the common fresh-VM case).
# Run with sudo on a fresh VM.
set -Eeuo pipefail

DB="${PGQUE_DB:-pgque_repro}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"          # repo root
PGQUE_SQL="$ROOT/sql/pgque.sql"
OSUSER="$(id -un)"                            # role for peer auth

[[ -f "$PGQUE_SQL" ]] || { echo "cannot find $PGQUE_SQL — run from inside the pgque repo" >&2; exit 1; }

if ! command -v psql >/dev/null 2>&1; then
  echo "==> installing PostgreSQL (apt)"
  export DEBIAN_FRONTEND=noninteractive
  apt-get update -qq
  apt-get install -y -qq postgresql postgresql-contrib
fi

if ! command -v bun >/dev/null 2>&1 && [[ ! -x "$HOME/.bun/bin/bun" ]]; then
  echo "==> installing bun"
  curl -fsSL https://bun.sh/install | bash
fi
export PATH="$HOME/.bun/bin:$PATH"

# make sure a cluster is running
pg_lsclusters -h 2>/dev/null | grep -q online || pg_ctlcluster "$(pg_lsclusters -h | awk 'NR==1{print $1}')" main start || service postgresql start || true

echo "==> (re)creating database '$DB' and a superuser role for '$OSUSER'"
sudo -u postgres psql -v ON_ERROR_STOP=1 -q <<SQL
select 'create database ${DB}'
 where not exists (select from pg_database where datname = '${DB}')\gexec
do \$\$ begin
  if not exists (select from pg_roles where rolname = '${OSUSER}') then
    execute format('create role %I login superuser', '${OSUSER}');
  end if;
end \$\$;
SQL

echo "==> installing pgque core + demo schema"
sudo -u postgres psql -v ON_ERROR_STOP=1 -q -d "$DB" -f "$PGQUE_SQL" >/dev/null
sudo -u postgres psql -v ON_ERROR_STOP=1 -q -d "$DB" -f "$HERE/schema.sql"

echo "==> bun install"
( cd "$HERE" && bun install >/dev/null )

echo "==> done. database='$DB', role='$OSUSER'. Run: bash $HERE/run.sh"
