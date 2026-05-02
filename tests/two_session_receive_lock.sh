#!/usr/bin/env bash
set -Eeuo pipefail

# Validate same-consumer receive serialization with two real sessions.
#
# Usage:
#   PGQUE_TEST_DSN=postgresql://postgres:***@localhost/pgque_test \
#     tests/two_session_receive_lock.sh
#
# The target database must already have sql/pgque.sql installed. The harness
# creates one temporary queue name, inserts one event, then proves that a second
# concurrent pgque.receive(queue, consumer) call blocks behind the first session
# and does not receive the same message while the first batch remains active.
# It is intentionally useful as a red/green validator for the #97/#125 fix:
# pre-fix code should fail by returning too quickly and/or duplicating the row;
# the row-lock fix should make it wait and return zero duplicate rows.

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
queue_name="two_session_receive_${$}_$(date +%s)"
workdir="$(mktemp -d)"
cleanup() {
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
select pg_sleep(4);
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
  assert v_count = 0, format('session2 must not receive duplicate message, got %s', v_count);
end \$\$;
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

# Give session 1 enough time to enter receive() and hold its transaction open.
sleep 1
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

elapsed=$((end_epoch - start_epoch))
if (( elapsed < 2 )); then
  echo "FAIL: session2 returned too quickly (${elapsed}s); expected it to wait on the session1 row lock" >&2
  print_debug
  exit 1
fi

echo "PASS: concurrent same-consumer receive serialized; session2 waited ${elapsed}s and got no duplicate rows"
