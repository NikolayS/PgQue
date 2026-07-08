#!/usr/bin/env bash
# Validate dlq_replay() serialization with two real sessions.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_DSN=postgresql://postgres:***@localhost/pgque_test \
#     tests/two_session_dlq_replay_race.sh
#
# The target database must already have sql/pgque.sql installed. The harness
# dead-letters one event, then has session 1 call pgque.dlq_replay(dl_id)
# inside an open transaction (commit delayed by pg_sleep) while session 2
# calls pgque.dlq_replay(dl_id) for the same id concurrently. With the
# row lock in dlq_replay, session 2 must block behind session 1's locked
# dead_letter row, re-evaluate after commit, find the row gone, and raise
# 'dead letter entry not found'. Pre-fix code fails this harness: session 2's
# unlocked select still sees the row, so both sessions call insert_event and
# the event is re-enqueued twice.

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
queue_name="two_session_dlq_replay_${$}_$(date +%s)"
session1_app="pgque_dlq_replay_s1_${$}_$(date +%s)"
hold_seconds=4
workdir="$(mktemp -d)"
cleanup() {
  "${psql_base[@]}" -qAtc "
    select pgque.unregister_consumer('${queue_name}', 'c1');
    select pgque.drop_queue('${queue_name}', true);
  " >/dev/null 2>&1 || true
  rm -rf "${workdir}"
}
trap cleanup EXIT

cat >"${workdir}/setup.sql" <<SQL
select pgque.create_queue('${queue_name}');
select pgque.set_queue_config('${queue_name}', 'max_retries', '0');
select pgque.register_consumer('${queue_name}', 'c1');
select pgque.insert_event('${queue_name}', 'dlq.race', '{"n":1}');
select pgque.force_tick('${queue_name}');
select pgque.ticker();
SQL

cat >"${workdir}/dead_letter.sql" <<SQL
do \$\$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('${queue_name}', 'c1', 1) limit 1;
  assert v_msg.msg_id is not null, 'expected one message to dead-letter';
  perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'force dlq');
  perform pgque.ack(v_msg.batch_id);
end \$\$;
SQL

"${psql_base[@]}" -f "${workdir}/setup.sql" >/dev/null
"${psql_base[@]}" -f "${workdir}/dead_letter.sql" >/dev/null

dl_id=$("${psql_base[@]}" -qAtc "
  select dl.dl_id
  from pgque.dead_letter dl
  join pgque.queue q on q.queue_id = dl.dl_queue_id
  where q.queue_name = '${queue_name}'
")
if [[ -z "${dl_id}" ]]; then
  echo "FAIL: no dead_letter row created during setup" >&2
  exit 1
fi

cat >"${workdir}/session1.sql" <<SQL
begin;
select 's1_new_eid=' || pgque.dlq_replay(${dl_id});
select pg_sleep(${hold_seconds});
commit;
SQL

cat >"${workdir}/session2.sql" <<SQL
select 's2_new_eid=' || pgque.dlq_replay(${dl_id});
SQL

PGAPPNAME="${session1_app}" "${psql_base[@]}" -f "${workdir}/session1.sql" >"${workdir}/session1.out" 2>"${workdir}/session1.err" &
session1_pid=$!

print_debug() {
  echo "--- session1.out ---" >&2
  cat "${workdir}/session1.out" >&2 || true
  echo "--- session1.err ---" >&2
  cat "${workdir}/session1.err" >&2 || true
  echo "--- session2.out ---" >&2
  cat "${workdir}/session2.out" >&2 || true
  echo "--- session2.err ---" >&2
  cat "${workdir}/session2.err" >&2 || true
}

# Wait until session 1 is visibly inside pg_sleep() with its replay
# transaction still open (delete of the dead_letter row uncommitted).
session1_ready=0
for _ in $(seq 1 50); do
  if "${psql_base[@]}" -tAc "
    select 1
    from pg_stat_activity
    where application_name = '${session1_app}'
      and state = 'active'
      and wait_event_type = 'Timeout'
      and wait_event = 'PgSleep'
      and query like 'select pg_sleep(%'
    limit 1
  " | grep -q 1; then
    session1_ready=1
    break
  fi
  sleep 0.2
done
if (( session1_ready != 1 )); then
  echo "FAIL: session1 did not reach pg_sleep() while holding the replay transaction" >&2
  print_debug
  exit 1
fi

set +e
"${psql_base[@]}" -f "${workdir}/session2.sql" >"${workdir}/session2.out" 2>"${workdir}/session2.err"
session2_status=$?
wait "${session1_pid}"
session1_status=$?
set -e

if (( session1_status != 0 )); then
  echo "FAIL: session1 replay failed unexpectedly" >&2
  print_debug
  exit 1
fi

if (( session2_status == 0 )); then
  echo "FAIL: session2 replay succeeded; expected 'dead letter entry not found' after waiting on session1" >&2
  print_debug
  exit 1
fi
if ! grep -q "dead letter entry not found: ${dl_id}" "${workdir}/session2.err"; then
  echo "FAIL: session2 failed with an unexpected error" >&2
  print_debug
  exit 1
fi

# The event must be re-enqueued exactly once (pre-fix code enqueues it twice).
"${psql_base[@]}" -qAtc "
  select pgque.force_tick('${queue_name}');
  select pgque.ticker();
" >/dev/null
replayed_count=$("${psql_base[@]}" -qAtc "
  select count(*)
  from pgque.receive('${queue_name}', 'c1', 100)
  where type = 'dlq.race'
")
if [[ "${replayed_count}" != "1" ]]; then
  echo "FAIL: expected exactly 1 replayed event, got ${replayed_count}" >&2
  print_debug
  exit 1
fi

dlq_count=$("${psql_base[@]}" -qAtc "
  select count(*)
  from pgque.dead_letter dl
  join pgque.queue q on q.queue_id = dl.dl_queue_id
  where q.queue_name = '${queue_name}'
")
if [[ "${dlq_count}" != "0" ]]; then
  echo "FAIL: expected empty DLQ after replay, got ${dlq_count} rows" >&2
  print_debug
  exit 1
fi

echo "PASS: concurrent dlq_replay serialized; second caller got 'dead letter entry not found' and the event was re-enqueued exactly once"
