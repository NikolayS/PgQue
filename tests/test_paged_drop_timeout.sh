#!/usr/bin/env bash
# Force-drop must preserve unrelated lock-timeout SQLSTATEs.
set -Eeuo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="pgque-paged-drop-timeout-$$"
password="$(openssl rand -hex 24)"
tmpdir="$(mktemp -d)"
postgres_image="${PGQUE_TEST_IMAGE:-postgres:18}"
case "${postgres_image}" in
  postgres:1[4-7]*) data_mount=/var/lib/postgresql/data ;;
  *) data_mount=/var/lib/postgresql ;;
esac

cleanup() {
  docker rm -f --volumes "${container}" >/dev/null 2>&1 || true
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for file in "${tmpdir}"/*.out "${tmpdir}"/*.err; do
    [[ -e "${file}" ]] || continue
    echo "--- ${file##*/} ---" >&2
    sed -n '1,240p' "${file}" >&2
  done
  exit 1
}

docker run --detach --name "${container}" \
  --env POSTGRES_PASSWORD="${password}" \
  --env POSTGRES_DB=pgque_test \
  --env POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
  --volume "${repo}:/repo:ro" \
  --tmpfs "${data_mount}:rw,size=512m" \
  "${postgres_image}" \
  -c listen_addresses=localhost \
  -c log_connections=on >/dev/null

psql_test() {
  docker exec --interactive --env PGPASSWORD="${password}" \
    --env 'PGOPTIONS=-c statement_timeout=15s' \
    --workdir /repo "${container}" \
    psql -X -qAt -h 127.0.0.1 -U postgres -d pgque_test \
    -v ON_ERROR_STOP=1 "$@"
}

ready=0
for _ in $(seq 1 30); do
  if psql_test -c 'select 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" = 1 ]] || fail 'Postgres did not become ready'

psql_test -f devel/sql/pgque.sql >"${tmpdir}/install.out" \
  2>"${tmpdir}/install.err" || fail 'installer failed'

# A queue-row lock is not a subscription/slot NOWAIT conflict.
psql_test -c "select pgque.create_queue('drop_timeout')" >/dev/null
psql_test -c "
  begin;
  select queue_id from pgque.queue where queue_name = 'drop_timeout' for update;
  set application_name = 'pgque_drop_timeout_holder';
  select pg_sleep(5);
  rollback;
" >"${tmpdir}/holder.out" 2>"${tmpdir}/holder.err" &
holder_pid=$!
ready=0
for _ in $(seq 1 50); do
  if psql_test -c "
    select exists (
      select 1 from pg_stat_activity
      where application_name = 'pgque_drop_timeout_holder'
        and wait_event = 'PgSleep'
    )
  " | grep -qx t; then
    ready=1
    break
  fi
  sleep 0.1
done
[[ "${ready}" = 1 ]] || fail 'lock holder did not become ready'
psql_test <<'SQL' >"${tmpdir}/timeout.out" 2>"${tmpdir}/timeout.err" || fail 'lock timeout SQLSTATE changed'
set lock_timeout = '100ms';
do $$
declare
    force_drop boolean;
begin
    foreach force_drop in array array[false, true] loop
        begin
            perform pgque.drop_queue('drop_timeout', force_drop);
            raise exception 'drop unexpectedly acquired the queue lock';
        exception when lock_not_available then
            null;
        end;
    end loop;
    assert exists (select 1 from pgque.queue where queue_name = 'drop_timeout');
end $$;
SQL
wait "${holder_pid}" || fail 'lock holder failed'
psql_test -c "select pgque.drop_queue('drop_timeout', true)" >/dev/null
echo 'PASS: force and non-force queue lock timeouts preserve SQLSTATE 55P03'
