#!/usr/bin/env bash
# Validate same-consumer receive serialization with two real sessions.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_DSN=postgresql://postgres:***@localhost/pgque_test \
#     tests/two_session_receive_lock.sh
#
# The target database must already have sql/pgque.sql installed. The harness
# creates one temporary queue name, inserts one event, then proves that a second
# concurrent pgque.receive(queue, consumer) call blocks behind the first session
# and does not receive a different batch while the first batch remains active.
# It is intentionally useful as a red/green validator for the #97/#125 fix:
# pre-fix code should fail by returning too quickly and/or duplicating the row;
# the row-lock fix should make it wait and idempotently return the same batch.

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
queue_name="two_session_receive_${$}_$(date +%s)"
hold_seconds=4
min_wait_seconds=$((hold_seconds - 1))
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
select pgque.register_consumer('${queue_name}', 'c1');
select pgque.insert_event('${queue_name}', 'test.concurrent', '{"n":1}');
select pgque.force_tick('${queue_name}');
select pgque.ticker();
SQL

cat >"${workdir}/session1.sql" <<SQL
begin;
create temp table s1_receive as
  select * from pgque.receive('${queue_name}', 'c1', 10);
do \$\$
declare
  v_count integer;
begin
  select count(*) into v_count from s1_receive;
  assert v_count = 1, format('session1 expected 1 message, got %s', v_count);
end \$\$;
select 's1_batch_id=' || batch_id from s1_receive limit 1;
select pg_sleep(${hold_seconds});
commit;
SQL

cat >"${workdir}/session2.sql" <<SQL
\timing on
begin;
create temp table s2_receive as
  select * from pgque.receive('${queue_name}', 'c1', 10);
do \$\$
declare
  v_count integer;
begin
  select count(*) into v_count from s2_receive;
  assert v_count = 1, format('session2 expected idempotent re-fetch (1 row), got %s', v_count);
end \$\$;
select 's2_batch_id=' || batch_id from s2_receive limit 1;
commit;
SQL

"${psql_base[@]}" -f "${workdir}/setup.sql"
"${psql_base[@]}" -f "${workdir}/session1.sql" >"${workdir}/session1.out" 2>"${workdir}/session1.err" &
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

# Wait until session 1 has entered receive() and recorded an active batch.
for _ in $(seq 1 50); do
  if "${psql_base[@]}" -tAc "
    select 1
      from pgque.subscription s
      join pgque.queue q on q.queue_id = s.sub_queue
     where q.queue_name = '${queue_name}'
       and s.sub_batch is not null
     limit 1
  " | grep -q 1; then
    break
  fi
  sleep 0.2
done
start_epoch=$(date +%s)
set +e
"${psql_base[@]}" -f "${workdir}/session2.sql" >"${workdir}/session2.out" 2>"${workdir}/session2.err"
session2_status=$?
end_epoch=$(date +%s)
wait "${session1_pid}"
session1_status=$?
set -e

if (( session1_status != 0 || session2_status != 0 )); then
  echo "FAIL: two-session receive harness failed (session1=${session1_status}, session2=${session2_status})" >&2
  print_debug
  exit 1
fi

s1_batch_id=$(grep -Eo 's1_batch_id=[0-9]+' "${workdir}/session1.out" | tail -n 1 | cut -d= -f2 || true)
s2_batch_id=$(grep -Eo 's2_batch_id=[0-9]+' "${workdir}/session2.out" | tail -n 1 | cut -d= -f2 || true)
if [[ -z "${s1_batch_id}" || -z "${s2_batch_id}" || "${s1_batch_id}" != "${s2_batch_id}" ]]; then
  echo "FAIL: session2 returned batch ${s2_batch_id:-<none>}; expected session1 batch ${s1_batch_id:-<none>}" >&2
  print_debug
  exit 1
fi

elapsed=$((end_epoch - start_epoch))
if (( elapsed < min_wait_seconds )); then
  echo "FAIL: session2 returned too quickly (${elapsed}s); expected it to wait on the session1 row lock" >&2
  print_debug
  exit 1
fi

echo "PASS: concurrent same-consumer receive serialized; session2 waited ${elapsed}s and idempotently returned batch ${s2_batch_id}"
