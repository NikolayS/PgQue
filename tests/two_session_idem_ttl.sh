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

# Refuse the database before arming any mutating cleanup.
database_name="$("${psql_base[@]}" -qAtc 'select current_database()')"
if [[ ! "${database_name}" =~ (^|_)test($|_) ]] \
   && [[ "${PGQUE_ALLOW_TEST_MUTATION:-}" != "1" ]]; then
  echo "FAIL: refusing concurrency test mutations in database '${database_name}'" >&2
  echo "      use a database name containing 'test' or set PGQUE_ALLOW_TEST_MUTATION=1" >&2
  exit 2
fi

suffix="${$}_$(date +%s)"
queue_name="idem_ttl_lock_${suffix}"
idem_key="lock_wait_${suffix}"
holder_app="idem_ttl_holder_${suffix}"
contender_app="idem_ttl_contender_${suffix}"
workdir="$(mktemp -d)"
holder_input_open=0

cleanup() {
  local cleanup_status=0
  local residue

  set +e
  if (( holder_input_open )); then
    exec {holder_input}>&-
    holder_input_open=0
  fi
  "${psql_base[@]}" -qAtc "
    select pg_terminate_backend(pid)
    from pg_stat_activity
    where pid <> pg_backend_pid()
      and datname = current_database()
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

# Keep the transaction open under explicit control. The acknowledgment is
# produced only after the conflicting row lock has actually been acquired.
mkfifo "${workdir}/holder.in"
PGAPPNAME="${holder_app}" timeout --kill-after=2s 25s "${psql_base[@]}" -qAt \
  <"${workdir}/holder.in" >"${workdir}/holder.out" 2>"${workdir}/holder.err" &
holder_pid=$!
exec {holder_input}>"${workdir}/holder.in"
holder_input_open=1
cat >&"${holder_input}" <<SQL
begin;
set local statement_timeout = '15s';
set local idle_in_transaction_session_timeout = '20s';
select 1
from pgque.idem as k
inner join pgque.queue as q on q.queue_id = k.queue_id
where q.queue_name = '${queue_name}'
  and k.idem_key = '${idem_key}'
for update of k;
select 'holder-ready:${suffix}:' || pg_backend_pid();
SQL

holder_ready=0
for _ in $(seq 1 100); do
  if grep -Eq "^holder-ready:${suffix}:[0-9]+$" "${workdir}/holder.out"; then
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
holder_backend_pid="$(sed -n "s/^holder-ready:${suffix}:\([0-9][0-9]*\)$/\1/p" "${workdir}/holder.out")"
if [[ ! "${holder_backend_pid}" =~ ^[0-9]+$ ]]; then
  echo "FAIL: invalid holder acknowledgment" >&2
  print_debug
  exit 1
fi
echo "barrier: holder backend ${holder_backend_pid} acknowledged the row lock"

# Bound client lifetime as well as server statements, including startup.
PGAPPNAME="${contender_app}" timeout --kill-after=2s 25s "${psql_base[@]}" \
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

  if (v_remaining > interval '1 second') is distinct from true then
    raise exception '%', format(
      'takeover TTL was not measured from lock acquisition: remaining=%s',
      v_remaining);
  end if;
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
  if v_deduped is distinct from true then
    raise exception 'immediate retry after lock-wait takeover must deduplicate';
  end if;
end
\$\$;
SQL
contender_pid=$!

contender_waiting=0
for _ in $(seq 1 100); do
  probe="$("${psql_base[@]}" -qAtc "
    select pid, extract(epoch from clock_timestamp())
    from pg_stat_activity
    where application_name = '${contender_app}'
      and datname = current_database()
      and wait_event_type = 'Lock'
      and ${holder_backend_pid} = any(pg_blocking_pids(pid))
  ")"
  if [[ "${probe}" =~ ^[0-9]+\|[0-9]+([.][0-9]+)?$ ]]; then
    contender_backend_pid="${probe%%|*}"
    blocked_at="${probe#*|}"
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

echo "barrier: contender backend ${contender_backend_pid} blocked by holder ${holder_backend_pid} at ${blocked_at}"
# Start the TTL-aging delay at the observed specific blocker, not at holder
# startup. Recheck the same relationship before releasing the transaction.
sleep 2.25
release_probe="$("${psql_base[@]}" -qAtc "
  select pid, extract(epoch from clock_timestamp()) - ${blocked_at}::numeric,
         extract(epoch from clock_timestamp())
  from pg_stat_activity
  where pid = ${contender_backend_pid}
    and application_name = '${contender_app}'
    and datname = current_database()
    and wait_event_type = 'Lock'
    and ${holder_backend_pid} = any(pg_blocking_pids(pid))
    and extract(epoch from clock_timestamp()) - ${blocked_at}::numeric > 2
")"
if [[ -z "${release_probe}" ]]; then
  echo "FAIL: contender did not remain blocked by this holder for its full requested TTL" >&2
  print_debug
  exit 1
fi
IFS='|' read -r checked_pid observed_wait release_epoch <<<"${release_probe}"
echo "release: holder=${holder_backend_pid} contender=${checked_pid} observed_wait_seconds=${observed_wait} requested_ttl_seconds=2 release_epoch=${release_epoch}"
printf 'commit;\n\\q\n' >&"${holder_input}"
exec {holder_input}>&-
holder_input_open=0

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
