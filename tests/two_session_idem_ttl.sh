#!/usr/bin/env bash
# Regression coverage for idempotency TTL after a conflicting row-lock wait.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_DSN=postgresql://postgres:***@localhost/pgque_test \
#     tests/two_session_idem_ttl.sh
#
# The target database must already have devel/sql/pgque.sql installed.

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
suffix="${$}_$(date +%s)"
queue_name="idem_ttl_lock_${suffix}"
idem_key="lock_wait_${suffix}"
holder_app="idem_ttl_holder_${suffix}"
contender_app="idem_ttl_contender_${suffix}"
workdir="$(mktemp -d)"

cleanup() {
  local cleanup_status=0
  local residue

  set +e
  "${psql_base[@]}" -qAtc "
    select pg_terminate_backend(pid)
    from pg_stat_activity
    where pid <> pg_backend_pid()
      and application_name in ('${holder_app}', '${contender_app}');
  " >/dev/null 2>&1 || cleanup_status=1
  "${psql_base[@]}" -qAtc \
    "select pgque.drop_queue('${queue_name}', true)" \
    >/dev/null 2>&1 || cleanup_status=1

  residue="$("${psql_base[@]}" -qAtc "
    select count(*)
    from pgque.queue
    where queue_name = '${queue_name}'
  " 2>/dev/null)"
  if [[ "${residue}" != "0" ]]; then
    echo "FAIL: idempotency TTL cleanup left ${residue:-unknown} queue rows" >&2
    cleanup_status=1
  fi
  rm -rf "${workdir}"
  set -e
  return "${cleanup_status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

print_debug() {
  local file
  for file in "${workdir}"/*; do
    [[ -f "${file}" ]] || continue
    echo "--- $(basename "${file}") ---" >&2
    cat "${file}" >&2 || true
  done
}

database_name="$("${psql_base[@]}" -qAtc 'select current_database()')"
if [[ ! "${database_name}" =~ (^|_)test($|_) ]] \
   && [[ "${PGQUE_ALLOW_TEST_MUTATION:-}" != "1" ]]; then
  echo "FAIL: refusing concurrency test mutations in database '${database_name}'" >&2
  echo "      use a database name containing 'test' or set PGQUE_ALLOW_TEST_MUTATION=1" >&2
  exit 2
fi

"${psql_base[@]}" >"${workdir}/setup.out" 2>"${workdir}/setup.err" <<SQL
select pgque.create_queue('${queue_name}');
select event_id, deduped
from pgque.send_idem(
  '${queue_name}', 'seed', '{}', '${idem_key}', interval '1 hour');
update pgque.idem as k
set expires_at = clock_timestamp() - interval '1 second'
from pgque.queue as q
where q.queue_id = k.queue_id
  and q.queue_name = '${queue_name}'
  and k.idem_key = '${idem_key}';
SQL

# Hold the expired claim row. The contender starts its statement while this
# lock is held, then waits longer than its requested TTL before takeover.
PGAPPNAME="${holder_app}" "${psql_base[@]}" \
  >"${workdir}/holder.out" 2>"${workdir}/holder.err" <<SQL &
begin;
select 1
from pgque.idem as k
inner join pgque.queue as q on q.queue_id = k.queue_id
where q.queue_name = '${queue_name}'
  and k.idem_key = '${idem_key}'
for update of k;
select pg_sleep(4);
commit;
SQL
holder_pid=$!

holder_ready=0
for _ in $(seq 1 100); do
  if "${psql_base[@]}" -qAtc "
    select 1
    from pg_stat_activity
    where application_name = '${holder_app}'
      and wait_event_type = 'Timeout'
      and wait_event = 'PgSleep'
  " | grep -qx 1; then
    holder_ready=1
    break
  fi
  sleep 0.05
done
if (( holder_ready != 1 )); then
  echo "FAIL: claim-row holder did not reach its lock barrier" >&2
  print_debug
  exit 1
fi

PGAPPNAME="${contender_app}" "${psql_base[@]}" \
  >"${workdir}/contender.out" 2>"${workdir}/contender.err" <<SQL &
set statement_timeout = '15s';
select event_id, deduped
from pgque.send_idem(
  '${queue_name}', 'takeover', '{}', '${idem_key}', interval '2 seconds');

do \$\$
declare
  v_remaining interval;
begin
  select k.expires_at - clock_timestamp()
  into strict v_remaining
  from pgque.idem as k
  inner join pgque.queue as q on q.queue_id = k.queue_id
  where q.queue_name = '${queue_name}'
    and k.idem_key = '${idem_key}';

  assert v_remaining > interval '1 second', format(
    'takeover TTL was not measured from lock acquisition: remaining=%s',
    v_remaining);
end
\$\$;

do \$\$
declare
  v_deduped boolean;
begin
  select s.deduped
  into strict v_deduped
  from pgque.send_idem(
    '${queue_name}', 'retry', '{}', '${idem_key}', interval '2 seconds') as s;
  assert v_deduped,
    'immediate retry after lock-wait takeover must deduplicate';
end
\$\$;
SQL
contender_pid=$!

contender_waiting=0
for _ in $(seq 1 100); do
  if "${psql_base[@]}" -qAtc "
    select 1
    from pg_stat_activity
    where application_name = '${contender_app}'
      and wait_event_type = 'Lock'
  " | grep -qx 1; then
    contender_waiting=1
    break
  fi
  sleep 0.05
done
if (( contender_waiting != 1 )); then
  echo "FAIL: takeover did not wait on the conflicting idempotency row lock" >&2
  print_debug
  exit 1
fi

set +e
wait "${contender_pid}"
contender_status=$?
wait "${holder_pid}"
holder_status=$?
set -e
if (( contender_status != 0 || holder_status != 0 )); then
  echo "FAIL: lock-wait takeover did not receive a fresh TTL window" >&2
  print_debug
  exit 1
fi

if ! cleanup; then
  trap - EXIT INT TERM HUP
  exit 1
fi
trap - EXIT INT TERM HUP
echo "PASS: idempotency takeover TTL starts after the conflicting row-lock wait"
