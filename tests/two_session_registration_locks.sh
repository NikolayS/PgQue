#!/usr/bin/env bash
# Regression coverage for consumer registration lock ordering and first-create races.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_DSN=postgresql://postgres:***@localhost/pgque_test \
#     tests/two_session_registration_locks.sh
#
# The target database must already have devel/sql/pgque.sql installed.

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
suffix="${$}_$(date +%s)"
race_queue="registration_race_${suffix}"
coop_queue="registration_coop_race_${suffix}"
lock_queue="registration_lock_edge_${suffix}"
race_consumer="race_${suffix}"
coop_consumer="coop_${suffix}"
lock_consumer="lock_${suffix}"
trigger_name="registration_race_barrier_${$}"
trigger_function="registration_race_barrier_${$}"
barrier_table="registration_barrier_${$}"
race_lock_key=$((357000000 + ($$ % 100000) * 2))
coop_lock_key=$((race_lock_key + 1))
workdir="$(mktemp -d)"
locker_start_delay="${PGQUE_TEST_LOCKER_START_DELAY:-0}"

if [[ ! "${locker_start_delay}" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "PGQUE_TEST_LOCKER_START_DELAY must be a non-negative number" >&2
  exit 2
fi

cleanup() {
  local cleanup_status=0
  local residue

  set +e
  "${psql_base[@]}" -qAtc "
    select pg_terminate_backend(pid)
    from pg_stat_activity
    where pid <> pg_backend_pid()
      and application_name like '%${suffix}';
  " >/dev/null 2>&1 || cleanup_status=1
  "${psql_base[@]}" -qAtc \
    "drop trigger if exists ${trigger_name} on pgque.consumer" \
    >/dev/null 2>&1 || cleanup_status=1
  "${psql_base[@]}" -qAtc \
    "drop function if exists pgque.${trigger_function}()" \
    >/dev/null 2>&1 || cleanup_status=1
  for member in $(seq 1 4); do
    "${psql_base[@]}" -qAtc \
      "select pgque.unregister_subconsumer('${coop_queue}', '${coop_consumer}', 'm${member}')" \
      >/dev/null 2>&1 || cleanup_status=1
  done
  "${psql_base[@]}" -qAtc \
    "select pgque.drop_queue('${race_queue}', true)" \
    >/dev/null 2>&1 || cleanup_status=1
  "${psql_base[@]}" -qAtc \
    "select pgque.drop_queue('${coop_queue}', true)" \
    >/dev/null 2>&1 || cleanup_status=1
  for slot in 0 1; do
    "${psql_base[@]}" -qAtc \
      "select pgque.unsubscribe_slot('${lock_queue}', '${lock_consumer}', ${slot})" \
      >/dev/null 2>&1 || cleanup_status=1
  done
  "${psql_base[@]}" -qAtc \
    "select pgque.drop_queue('${lock_queue}', true)" \
    >/dev/null 2>&1 || cleanup_status=1
  "${psql_base[@]}" -qAtc \
    "drop table if exists pgque.${barrier_table}" \
    >/dev/null 2>&1 || cleanup_status=1

  residue="$("${psql_base[@]}" -qAtc "
    select
      (
        select count(*)
        from pg_trigger
        where tgname = '${trigger_name}'
          and not tgisinternal
      )
      + (
        select count(*)
        from pg_proc
        where pronamespace = 'pgque'::regnamespace
          and proname = '${trigger_function}'
      )
      + (
        select count(*)
        from pgque.queue
        where queue_name in ('${race_queue}', '${coop_queue}', '${lock_queue}')
      )
      + (
        select count(*)
        from pg_class
        where relnamespace = 'pgque'::regnamespace
          and relname = '${barrier_table}'
      )
  " 2>/dev/null)"
  if [[ "${residue}" != "0" ]]; then
    echo "FAIL: registration concurrency cleanup left ${residue:-unknown} database objects" >&2
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
  for f in "${workdir}"/*; do
    [[ -f "${f}" ]] || continue
    echo "--- $(basename "${f}") ---" >&2
    cat "${f}" >&2 || true
  done
}

wait_for_advisory_waiters() {
  local app_prefix="$1"
  local expected="$2"
  local count

  for _ in $(seq 1 50); do
    count="$("${psql_base[@]}" -qAtc "
      select count(*)
      from pg_stat_activity
      where application_name like '${app_prefix}%'
        and wait_event_type = 'Lock'
        and wait_event = 'advisory'
    ")"
    if (( count >= expected )); then
      return 0
    fi
    sleep 0.1
  done

  echo "FAIL: expected ${expected} ${app_prefix} sessions at the registration barrier, saw ${count}" >&2
  print_debug
  return 1
}

wait_for_locker_ready() {
  local lock_key="$1"
  local ready

  for _ in $(seq 1 100); do
    ready="$("${psql_base[@]}" -qAtc "
      select ready
      from pgque.${barrier_table}
      where lock_key = ${lock_key}
    ")"
    if [[ "${ready}" == "t" ]]; then
      return 0
    fi
    sleep 0.1
  done

  echo "FAIL: advisory barrier ${lock_key} was not acquired" >&2
  print_debug
  return 1
}

release_barrier() {
  local lock_key="$1"

  "${psql_base[@]}" -qAtc "
    update pgque.${barrier_table}
    set release = true
    where lock_key = ${lock_key}
  " >/dev/null
}

run_barrier_locker() {
  local lock_key="$1"
  local app_name="$2"

  PGAPPNAME="${app_name}" "${psql_base[@]}" -qAt >"${workdir}/${app_name}.out" 2>"${workdir}/${app_name}.err" <<SQL &
select pg_sleep(${locker_start_delay});
select pg_advisory_lock(${lock_key});
insert into pgque.${barrier_table} (lock_key, ready, release)
values (${lock_key}, true, false);
do \$\$
declare
  v_release boolean;
begin
  for i in 1..600 loop
    select release
    into v_release
    from pgque.${barrier_table}
    where lock_key = ${lock_key};
    exit when v_release;
    perform pg_sleep(0.05);
  end loop;
  if not v_release then
    raise exception 'timed out waiting to release advisory barrier ${lock_key}';
  end if;
end
\$\$;
select pg_advisory_unlock(${lock_key});
SQL
  barrier_locker_pid=$!
}

# Installing a trigger on pgque.consumer is only appropriate in a disposable
# test database. Allow an explicit override for unusual local naming schemes.
database_name="$("${psql_base[@]}" -qAtc "select current_database()")"
if [[ ! "${database_name}" =~ (^|_)test($|_) ]] \
   && [[ "${PGQUE_ALLOW_TEST_MUTATION:-}" != "1" ]]; then
  echo "FAIL: refusing to install a test trigger in database '${database_name}'" >&2
  echo "      use a database name containing 'test' or set PGQUE_ALLOW_TEST_MUTATION=1" >&2
  exit 2
fi

# A test-only trigger puts every matching consumer INSERT behind an advisory
# barrier. Both callers therefore complete their initial "row not found"
# lookup before either INSERT can commit.
"${psql_base[@]}" >"${workdir}/setup.out" 2>"${workdir}/setup.err" <<SQL
select pgque.create_queue('${race_queue}');
select pgque.create_queue('${coop_queue}');
select pgque.create_queue('${lock_queue}');

create table pgque.${barrier_table} (
  lock_key bigint primary key,
  ready boolean not null,
  release boolean not null
);

create function pgque.${trigger_function}()
returns trigger
language plpgsql
as \$\$
begin
  if new.co_name like 'race_${suffix}%'
     or new.co_name like 'coop_${suffix}%' then
    perform pg_advisory_xact_lock(
      case
        when new.co_name like 'race_${suffix}%' then ${race_lock_key}
        else ${coop_lock_key}
      end
    );
  end if;
  return new;
end
\$\$;

create trigger ${trigger_name}
before insert on pgque.consumer
for each row execute function pgque.${trigger_function}();
SQL

# Case 1: two ordinary first registrations must not collide.
run_barrier_locker "${race_lock_key}" race_locker
locker_pid="${barrier_locker_pid}"
wait_for_locker_ready "${race_lock_key}"
race_pids=()
for worker in $(seq 1 8); do
  PGAPPNAME="race_worker_${worker}_${suffix}" \
    "${psql_base[@]}" -qAtc \
      "select pgque.register_consumer('${race_queue}', '${race_consumer}')" \
      >"${workdir}/race_worker_${worker}.out" \
      2>"${workdir}/race_worker_${worker}.err" &
  race_pids+=("$!")
done
wait_for_advisory_waiters "race_worker_%_${suffix}" 8
release_barrier "${race_lock_key}"

set +e
race_status=0
for pid in "${race_pids[@]}"; do
  wait "${pid}" || race_status=$?
done
wait "${locker_pid}"; locker_status=$?
set -e
if (( race_status != 0 || locker_status != 0 )); then
  echo "FAIL: concurrent first registration raised an error" >&2
  print_debug
  exit 1
fi
fresh_total="$(
  awk '/^[01]$/ { sum += $1 } END { print sum + 0 }' "${workdir}"/race_worker_*.out
)"
if (( fresh_total != 1 )); then
  echo "FAIL: exactly one ordinary registration must be fresh, got ${fresh_total}" >&2
  print_debug
  exit 1
fi

# Case 2: register_consumer_at racing register_subconsumer must safely form
# one cooperative main plus the requested member.
run_barrier_locker "${coop_lock_key}" coop_locker
locker_pid="${barrier_locker_pid}"
wait_for_locker_ready "${coop_lock_key}"
PGAPPNAME="coop_anchor_${suffix}" \
  "${psql_base[@]}" -qAtc "
    select pgque.register_consumer_at(
      '${coop_queue}',
      '${coop_consumer}',
      (
        select min(t.tick_id)
        from pgque.tick t
        join pgque.queue q on q.queue_id = t.tick_queue
        where q.queue_name = '${coop_queue}'
      )
    )
  " >"${workdir}/coop_anchor.out" 2>"${workdir}/coop_anchor.err" &
coop_anchor_pid=$!
coop_member_pids=()
for member in $(seq 1 4); do
  PGAPPNAME="coop_member_${member}_${suffix}" \
    "${psql_base[@]}" -qAtc \
      "select pgque.register_subconsumer('${coop_queue}', '${coop_consumer}', 'm${member}', true)" \
      >"${workdir}/coop_member_${member}.out" 2>"${workdir}/coop_member_${member}.err" &
  coop_member_pids+=("$!")
done
wait_for_advisory_waiters "coop_%_${suffix}" 5
release_barrier "${coop_lock_key}"

set +e
wait "${coop_anchor_pid}"; coop_anchor_status=$?
coop_member_status=0
for pid in "${coop_member_pids[@]}"; do
  wait "${pid}" || coop_member_status=$?
done
wait "${locker_pid}"; coop_locker_status=$?
set -e
if (( coop_anchor_status != 0 || coop_member_status != 0 || coop_locker_status != 0 )); then
  echo "FAIL: concurrent cooperative group formation raised an error" >&2
  print_debug
  exit 1
fi
"${psql_base[@]}" -qAt >"${workdir}/coop_assert.out" 2>"${workdir}/coop_assert.err" <<SQL
do \$\$
declare
  v_main_count integer;
  v_member_count integer;
  v_main_role text;
begin
  select count(*) into v_main_count
  from pgque.consumer
  where co_name = '${coop_consumer}';
  assert v_main_count = 1, format('expected one cooperative main consumer row, got %s', v_main_count);

  select count(*) into v_member_count
  from pgque.consumer
  where co_name like '${coop_consumer}.m%';
  assert v_member_count = 4, format('expected four cooperative member consumer rows, got %s', v_member_count);

  select s.sub_role into v_main_role
  from pgque.subscription s
  join pgque.consumer c on c.co_id = s.sub_consumer
  join pgque.queue q on q.queue_id = s.sub_queue
  where q.queue_name = '${coop_queue}'
    and c.co_name = '${coop_consumer}';
  assert v_main_role = 'coop_main', format('expected coop_main role, got %s', v_main_role);
end
\$\$;
SQL

# Case 3: an open registrar transaction may serialize other registrars, but it
# must not block the FK trigger's FOR KEY SHARE during receive_partitioned's
# empty-batch finish path.
"${psql_base[@]}" >"${workdir}/lock_setup.out" 2>"${workdir}/lock_setup.err" <<SQL
select pgque.register_consumer('${lock_queue}', '${lock_consumer}#0/2');
select pgque.register_consumer('${lock_queue}', '${lock_consumer}#1/2');
select pgque.subscribe_slot('${lock_queue}', '${lock_consumer}', 0, 2);
select pgque.subscribe_slot('${lock_queue}', '${lock_consumer}', 1, 2);
select pgque.claim_slot('${lock_queue}', '${lock_consumer}', 0, 'w0', interval '60 seconds');

with keys as (
  select key
  from (
    select 'k' || s as key
    from generate_series(1, 64) s
  ) candidates
  /* Keep this predicate aligned with partition_keys.sql's slot routing. */
  where ((pg_catalog.hashtextextended(key, 0) % 2) + 2) % 2 = 1
  limit 5
)
select pgque.insert_event('${lock_queue}', 't', '{}', key, null, null, null)
from keys;
select pgque.force_tick('${lock_queue}');
select pgque.ticker('${lock_queue}');
SQL

PGAPPNAME="registration_holder_${suffix}" \
  "${psql_base[@]}" >"${workdir}/registration_holder.out" 2>"${workdir}/registration_holder.err" <<SQL &
begin;
select pgque.register_consumer_at('${lock_queue}', '${lock_consumer}#0/2', null);
select pg_sleep(5);
rollback;
SQL
registration_holder_pid=$!

holder_ready=0
for _ in $(seq 1 50); do
  if "${psql_base[@]}" -qAtc "
    select 1
    from pg_stat_activity
    where application_name = 'registration_holder_${suffix}'
      and state = 'active'
      and wait_event_type = 'Timeout'
      and wait_event = 'PgSleep'
      and query like 'select pg_sleep(%'
  " | grep -q 1; then
    holder_ready=1
    break
  fi
  sleep 0.1
done
if (( holder_ready != 1 )); then
  echo "FAIL: registration holder did not reach pg_sleep" >&2
  print_debug
  exit 1
fi

set +e
"${psql_base[@]}" >"${workdir}/receiver.out" 2>"${workdir}/receiver.err" <<SQL
set statement_timeout = '3s';
do \$\$
declare
  v_count integer;
begin
  select count(*) into v_count
  from pgque.receive_partitioned(
    '${lock_queue}',
    '${lock_consumer}',
    0,
    2,
    'w0',
    50
  );
  assert v_count = 0, format('expected an empty filtered batch, got %s events', v_count);
end
\$\$;
SQL
receiver_status=$?
wait "${registration_holder_pid}"
holder_status=$?
set -e
if (( receiver_status != 0 || holder_status != 0 )); then
  echo "FAIL: open registration blocked the partition receive/finish path" >&2
  print_debug
  exit 1
fi

if ! cleanup; then
  trap - EXIT INT TERM HUP
  exit 1
fi
trap - EXIT INT TERM HUP
echo "PASS: first registrations serialize without unique violations; registration locks remain compatible with partition receive FK checks"
