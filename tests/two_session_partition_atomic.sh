#!/usr/bin/env bash
# Atomic setup races and same-key delivery across inverse producer commits.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_DSN=postgresql:///pgque_test tests/two_session_partition_atomic.sh
# Requires an installed PgQue database and its install/test-owner privileges.
# Gate rows control commits. PgSleep and pg_blocking_pids are the barriers;
# short sleeps only poll observed state, never decide when a producer commits.

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(env "PGOPTIONS=${PGOPTIONS:+${PGOPTIONS} }-c statement_timeout=60s"
  psql --no-psqlrc -X -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
run_id="${$}_$(date +%s)"
race_queue="partition_atomic_${run_id}"
order_queue="partition_inverse_${run_id}"
slot_queue="partition_unsub_slot_${run_id}"
whole_queue="partition_unsub_all_${run_id}"
consumer="workers_${run_id}"
race_consumer="race_workers_${run_id}"
fixture="partition_gate_${run_id}"
first_app="partition_first_${run_id}"
second_app="partition_second_${run_id}"
producer_app="partition_low_${run_id}"
receive_app="partition_receive_${run_id}"
teardown_app="partition_teardown_${run_id}"
workdir="$(mktemp -d)"

sql_value() {
  "${psql_base[@]}" -qAtc "$1"
}

print_debug() {
  local file
  for file in "${workdir}"/*; do
    [[ -f "${file}" ]] || continue
    echo "--- $(basename "${file}") ---" >&2
    cat "${file}" >&2 || true
  done
}

cleanup() {
  local status=$?
  local cleanup_failed=0
  local pid queue_name remaining deadline
  trap - EXIT
  set +e
  if (( status != 0 )); then
    print_debug
  fi
  sql_value "select pg_terminate_backend(pid) from pg_stat_activity
    where datname = current_database() and pid <> pg_backend_pid()
      and application_name in ('${first_app}', '${second_app}', '${producer_app}',
        '${receive_app}', '${teardown_app}')" \
    >/dev/null 2>&1 || cleanup_failed=1
  while read -r pid; do
    kill "${pid}" >/dev/null 2>&1 || true
    wait "${pid}" >/dev/null 2>&1 || true
  done < <(jobs -pr)
  deadline=$((SECONDS + 10))
  while :; do
    remaining="$(sql_value "select count(*) from pg_stat_activity
      where datname = current_database()
        and application_name in ('${first_app}', '${second_app}', '${producer_app}',
          '${receive_app}', '${teardown_app}')" 2>/dev/null)"
    [[ "${remaining}" = "0" ]] && break
    if (( SECONDS >= deadline )); then
      cleanup_failed=1
      break
    fi
    sleep 0.05
  done
  for queue_name in "${race_queue}" "${order_queue}" "${slot_queue}" "${whole_queue}"; do
    sql_value "select pgque.drop_queue('${queue_name}', true)
      where exists (select 1 from pgque.queue where queue_name = '${queue_name}')" \
      >/dev/null 2>&1 || cleanup_failed=1
  done
  sql_value "drop schema if exists ${fixture} cascade" >/dev/null 2>&1 || cleanup_failed=1
  if (( cleanup_failed != 0 )); then
    echo "FAIL: atomic partition harness cleanup did not complete" >&2
    status=1
  fi
  rm -rf -- "${workdir}"
  exit "${status}"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP

wait_for_state() {
  local label=$1 query=$2 deadline
  deadline=$((SECONDS + 30))
  until [[ "$(sql_value "${query}")" = "t" ]]; do
    if (( SECONDS >= deadline )); then
      echo "FAIL: timed out observing ${label}" >&2
      exit 1
    fi
    sleep 0.05
  done
  echo "barrier: ${label}"
}

wait_for_sleep() {
  local app=$1
  wait_for_state "${app} reached its transaction gate" \
    "select exists (select 1 from pg_stat_activity
       where datname = current_database() and application_name = '${app}'
         and state = 'active' and wait_event_type = 'Timeout' and wait_event = 'PgSleep')"
}

wait_for_child() {
  local pid=$1 label=$2
  if ! wait "${pid}"; then
    echo "FAIL: ${label} backend failed" >&2
    exit 1
  fi
}

"${psql_base[@]}" -q >"${workdir}/setup.out" 2>"${workdir}/setup.err" <<SQL
create schema ${fixture};
create table ${fixture}.gate (name text primary key, is_open boolean not null default false);
insert into ${fixture}.gate (name) values ('setup'), ('producer'), ('slot'), ('whole');
create table ${fixture}.seen (
  ordinal bigint generated always as identity,
  phase int not null, slot int not null, msg_id bigint not null,
  batch_id bigint not null, partition_key text not null
);
do \$\$
begin
  perform pgque.create_queue('${race_queue}');
  perform pgque.create_queue('${order_queue}');
  -- Keep unrelated global ticker calls out of these scoped fixtures.
  update pgque.queue set queue_ticker_paused = true
  where queue_name in ('${race_queue}', '${order_queue}');
  perform pgque.subscribe_partitioned('${order_queue}', '${consumer}', 3);
end \$\$;
SQL

# First setup is complete inside its transaction, but not yet committed.
PGAPPNAME="${first_app}" "${psql_base[@]}" -q \
  >"${workdir}/first.out" 2>"${workdir}/first.err" <<SQL &
begin;
select pgque.subscribe_partitioned('${race_queue}', '${race_consumer}', 3);
do \$\$
begin
  while not (select is_open from ${fixture}.gate where name = 'setup') loop
    perform pg_sleep(0.05);
  end loop;
end \$\$;
commit;
SQL
first_pid=$!
wait_for_sleep "${first_app}"

PGAPPNAME="${second_app}" "${psql_base[@]}" -q \
  >"${workdir}/second.out" 2>"${workdir}/second.err" \
  -c "select pgque.subscribe_partitioned('${race_queue}', '${race_consumer}', 3)" &
second_pid=$!
wait_for_state 'second first-setup call waits on the first backend' \
  "select exists (select 1 from pg_stat_activity waiter
    join pg_stat_activity holder on holder.pid = any(pg_blocking_pids(waiter.pid))
    where waiter.datname = current_database() and holder.datname = current_database()
      and waiter.application_name = '${second_app}'
      and holder.application_name = '${first_app}' and waiter.wait_event_type = 'Lock')"

"${psql_base[@]}" -q >>"${workdir}/race-check.out" 2>"${workdir}/race-check.err" <<SQL
do \$\$
begin
  assert not exists (select 1 from pgque.partition_slot_status
    where queue_name = '${race_queue}'), 'uncommitted setup exposed partial slots';
  assert not exists (select 1 from pgque.partition_consumer pc
    join pgque.queue q on q.queue_id = pc.queue_id where q.queue_name = '${race_queue}'),
    'uncommitted setup exposed partition metadata';
  assert not exists (select 1 from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue where q.queue_name = '${race_queue}'),
    'uncommitted setup exposed subscriptions';
end \$\$;
update ${fixture}.gate set is_open = true where name = 'setup';
SQL
wait_for_child "${first_pid}" 'first setup'
wait_for_child "${second_pid}" 'second setup'

"${psql_base[@]}" -q >>"${workdir}/race-check.out" 2>>"${workdir}/race-check.err" <<SQL
do \$\$
begin
  assert (select count(*) = 3 and bool_and(subscribed)
    and count(distinct last_tick) = 1 and bool_and(epoch = 0)
    from pgque.partition_slot_status where queue_name = '${race_queue}'),
    'concurrent setup must leave three complete slots at one shared cursor';
  assert (select count(*) = 3 from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue where q.queue_name = '${race_queue}'),
    'concurrent setup left duplicate or missing subscriptions';
  assert (select count(*) = 3 from pgque.partition_slot ps
    join pgque.queue q on q.queue_id = ps.queue_id where q.queue_name = '${race_queue}'),
    'concurrent setup left duplicate or missing lease rows';
end \$\$;
SQL
echo 'PASS: concurrent first setup serialized and committed one complete shared-cursor setup'

# Allocate the lower ID first and keep its transaction uncommitted. Higher
# IDs for the same key commit before the first tick/receive/ack window.
PGAPPNAME="${producer_app}" "${psql_base[@]}" -qAt \
  >"${workdir}/low.out" 2>"${workdir}/low.err" <<SQL &
begin;
select pgque.send('${order_queue}', 'inverse.low', 'low', 'same-key');
do \$\$
begin
  while not (select is_open from ${fixture}.gate where name = 'producer') loop
    perform pg_sleep(0.05);
  end loop;
end \$\$;
commit;
SQL
producer_pid=$!
wait_for_sleep "${producer_app}"
high_one="$(sql_value "select pgque.send('${order_queue}', 'inverse.high', 'high-1', 'same-key')")"
high_two="$(sql_value "select pgque.send('${order_queue}', 'inverse.high', 'high-2', 'same-key')")"

tick_queue() {
  local queue_name=$1
  # Other transactions always see paused=true. This transaction alone sees
  # the temporary unpause and creates the next normal ticker snapshot.
  sql_value "do \$\$ begin
    update pgque.queue set queue_ticker_paused = false where queue_name = '${queue_name}';
    perform pgque.force_next_tick('${queue_name}');
    assert pgque.ticker('${queue_name}') is not null, 'expected a new snapshot tick';
    update pgque.queue set queue_ticker_paused = true where queue_name = '${queue_name}';
  end \$\$;" >/dev/null
}

consume_phase() {
  local phase=$1 expected_ids=$2
  "${psql_base[@]}" -q >>"${workdir}/consume.out" 2>>"${workdir}/consume.err" <<SQL
do \$\$
declare
  v_slot int;
  v_msg pgque.message;
  v_ids bigint[];
begin
  for v_slot in 0..2 loop
    assert pgque.claim_slot('${order_queue}', '${consumer}', v_slot, 'owner') is not null,
      'inverse-commit fixture must acquire each slot';
    for v_msg in select * from pgque.receive_partitioned(
      '${order_queue}', '${consumer}', v_slot, 3, 'owner', 10)
    loop
      assert v_msg.extra1 = 'same-key', 'unexpected partition key';
      assert (pg_catalog.hashtextextended(v_msg.extra1, 0) % 3 + 3) % 3 = v_slot,
        'same-key message reached the wrong slot';
      insert into ${fixture}.seen (phase, slot, msg_id, batch_id, partition_key)
      values (${phase}, v_slot, v_msg.msg_id, v_msg.batch_id, v_msg.extra1);
    end loop;
    perform pgque.ack_partitioned('${order_queue}', '${consumer}', v_slot, 3, 'owner');
    assert pgque.release_slot('${order_queue}', '${consumer}', v_slot, 'owner'),
      'finished snapshot window must allow release';
  end loop;
  -- No ORDER BY is added to receive: ordinal records the actual stream order.
  select array_agg(msg_id order by ordinal) into v_ids
  from ${fixture}.seen where phase = ${phase};
  assert v_ids is not distinct from array[${expected_ids}]::bigint[],
    format('phase ${phase}: expected [${expected_ids}], got %s', v_ids);
  assert (select count(distinct slot) = 1 and count(distinct batch_id) = 1
    from ${fixture}.seen where phase = ${phase}),
    'same-key events must stay in one slot and one snapshot batch per phase';
end \$\$;
SQL
}

tick_queue "${order_queue}"
consume_phase 1 "${high_one},${high_two}"
wait_for_sleep "${producer_app}"
echo "barrier: higher IDs ${high_one},${high_two} were received and acked while the lower-ID producer remained uncommitted"
sql_value "update ${fixture}.gate set is_open = true where name = 'producer'" >/dev/null
wait_for_child "${producer_pid}" 'lower-ID producer'
low_id="$(tr -d '[:space:]' <"${workdir}/low.out")"
if [[ ! "${low_id}" =~ ^[0-9]+$ ]]; then
  echo 'FAIL: lower-ID producer did not return one event ID' >&2
  exit 1
fi
tick_queue "${order_queue}"
consume_phase 2 "${low_id}"

sql_value "do \$\$ begin
  assert ${low_id}::bigint < ${high_one}::bigint and ${high_one}::bigint < ${high_two}::bigint,
    'fixture did not reverse allocation and commit order';
  assert (select count(*) = 3 and count(distinct msg_id) = 3
    and count(distinct slot) = 1 from ${fixture}.seen),
    'two snapshot windows must deliver every message once to the same slot';
end \$\$;" >/dev/null
echo "PASS: same-key snapshot delivery was [${high_one},${high_two}] then [${low_id}]; per-window order and affinity hold, global ev_id FIFO does not"

# Force the receive interleaving immediately after its slot guard, before it
# locks the subscription. Teardown must wait without taking that subscription
# first, or the resumed receive and teardown deadlock. Cover both entry points.
for mode in slot whole; do
  if [[ "${mode}" = slot ]]; then
    teardown_queue=${slot_queue}
    teardown_sql="select pgque.unsubscribe_slot('${teardown_queue}', '${consumer}', 0)"
  else
    teardown_queue=${whole_queue}
    teardown_sql="select pgque.unsubscribe_partitioned('${teardown_queue}', '${consumer}')"
  fi
  sql_value "do \$\$ begin
    perform pgque.create_queue('${teardown_queue}');
    update pgque.queue set queue_ticker_paused = true where queue_name = '${teardown_queue}';
    perform pgque.subscribe_partitioned('${teardown_queue}', '${consumer}', 1);
  end \$\$;" >/dev/null
  sql_value "select pgque.send('${teardown_queue}', 'teardown', 'payload', 'same-key')" >/dev/null
  tick_queue "${teardown_queue}"
  sql_value "select pgque.claim_slot('${teardown_queue}', '${consumer}', 0, 'owner')" >/dev/null

  PGAPPNAME="${receive_app}" "${psql_base[@]}" -q \
    >"${workdir}/receive-${mode}.out" 2>"${workdir}/receive-${mode}.err" <<SQL &
begin;
set local deadlock_timeout = '100ms';
do \$\$
begin
  perform pgque._slot_guard('${teardown_queue}', '${consumer}', 0, 1, 'owner');
  while not (select is_open from ${fixture}.gate where name = '${mode}') loop
    perform pg_sleep(0.05);
  end loop;
  assert (select count(*) = 1 from pgque.receive_partitioned(
    '${teardown_queue}', '${consumer}', 0, 1, 'owner', 10)),
    'receive must finish while teardown waits for its slot';
  assert pgque.ack_partitioned('${teardown_queue}', '${consumer}', 0, 1, 'owner') = 1,
    'receive must ack before teardown';
end \$\$;
commit;
SQL
  receive_pid=$!
  wait_for_sleep "${receive_app}"
  PGAPPNAME="${teardown_app}" "${psql_base[@]}" -q \
    >"${workdir}/teardown-${mode}.out" 2>"${workdir}/teardown-${mode}.err" \
    -c "${teardown_sql}" &
  teardown_pid=$!
  wait_for_state "${mode} teardown waits for the receive backend" \
    "select exists (select 1 from pg_stat_activity waiter
      join pg_stat_activity holder on holder.pid = any(pg_blocking_pids(waiter.pid))
      where waiter.datname = current_database() and holder.datname = current_database()
        and waiter.application_name = '${teardown_app}'
        and holder.application_name = '${receive_app}' and waiter.wait_event_type = 'Lock')"
  sql_value "update ${fixture}.gate set is_open = true where name = '${mode}'" >/dev/null
  wait_for_child "${receive_pid}" "${mode} guarded receive"
  wait_for_child "${teardown_pid}" "${mode} teardown"
  sql_value "do \$\$ begin
    assert not exists (select 1 from pgque.partition_consumer pc
      join pgque.queue q on q.queue_id = pc.queue_id where q.queue_name = '${teardown_queue}'),
      'teardown left partition metadata';
    assert not exists (select 1 from pgque.subscription s
      join pgque.queue q on q.queue_id = s.sub_queue where q.queue_name = '${teardown_queue}'),
      'teardown left an engine subscription';
  end \$\$;" >/dev/null
  echo "PASS: ${mode} teardown waited slot-first; receive/ack completed without deadlock"
done
