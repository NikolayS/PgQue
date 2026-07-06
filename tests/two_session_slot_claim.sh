#!/usr/bin/env bash
# Validate partition slot claim/release across two real sessions
# (US-12.4 single processor per slot, US-12.5 claim/release + crash recovery).
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_DSN=postgresql://postgres:***@localhost/pgque_test \
#     tests/two_session_slot_claim.sh
#
# The target database must already have sql/pgque.sql and
# sql/pgque-api/partition_keys.sql installed. The harness registers a
# 2-slot partitioned consumer, has session 1 claim slot 0 and hold it, then
# proves from session 2 that:
#   - claim_slot(slot 0) fails while session 1 holds it (US-12.4)
#   - claim_slot(slot 1) succeeds (free slot; the claim loop lands there)
#   - partition_slot_status shows session 1's pid as slot 0 owner (US-12.6)
#   - after session 1 exits, slot 0 becomes claimable again (US-12.5)

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
queue_name="two_session_slot_claim_${$}_$(date +%s)"
session1_app="pgque_slot_claim_s1_${$}_$(date +%s)"
hold_seconds=4
workdir="$(mktemp -d)"
cleanup() {
  "${psql_base[@]}" -qAtc "
    select pgque.unsubscribe_slot('${queue_name}', 'w', 0);
    select pgque.unsubscribe_slot('${queue_name}', 'w', 1);
    select pgque.drop_queue('${queue_name}', true);
  " >/dev/null 2>&1 || true
  rm -rf "${workdir}"
}
trap cleanup EXIT

cat >"${workdir}/setup.sql" <<SQL
select pgque.create_queue('${queue_name}');
select pgque.subscribe_slot('${queue_name}', 'w', 0, 2);
select pgque.subscribe_slot('${queue_name}', 'w', 1, 2);
SQL

# Session 1: claim slot 0, hold it for a while, exit without releasing --
# session death must free the slot (US-12.5 crash recovery).
cat >"${workdir}/session1.sql" <<SQL
do \$\$
begin
  assert pgque.claim_slot('${queue_name}', 'w', 0),
    'session1: claim of free slot 0 must succeed';
end \$\$;
select 's1_claimed=1';
select pg_sleep(${hold_seconds});
SQL

# Session 2: while session 1 holds slot 0.
cat >"${workdir}/session2.sql" <<SQL
do \$\$
declare
  v_pid int;
begin
  assert not pgque.claim_slot('${queue_name}', 'w', 0),
    'session2: claim of held slot 0 must fail (US-12.4)';
  assert pgque.claim_slot('${queue_name}', 'w', 1),
    'session2: claim of free slot 1 must succeed';

  select owner_pid into v_pid
  from pgque.partition_slot_status
  where queue_name = '${queue_name}' and consumer = 'w' and slot = 0;
  assert v_pid is not null and v_pid <> pg_backend_pid(),
    'session2: slot 0 owner_pid must be session 1 (US-12.6)';

  select owner_pid into v_pid
  from pgque.partition_slot_status
  where queue_name = '${queue_name}' and consumer = 'w' and slot = 1;
  assert v_pid = pg_backend_pid(),
    'session2: slot 1 owner_pid must be this session (US-12.6)';

  assert pgque.release_slot('${queue_name}', 'w', 1),
    'session2: release of held slot 1 must return true (US-12.5)';
end \$\$;
select 's2_checks=ok';
SQL

# After session 1 exits: slot 0 claimable again.
cat >"${workdir}/session3.sql" <<SQL
do \$\$
begin
  assert pgque.claim_slot('${queue_name}', 'w', 0),
    'slot 0 must be claimable after the holding session died (US-12.5)';
  perform pgque.release_slot('${queue_name}', 'w', 0);
end \$\$;
select 's3_reclaim=ok';
SQL

"${psql_base[@]}" -f "${workdir}/setup.sql" >/dev/null

PGAPPNAME="${session1_app}" "${psql_base[@]}" -f "${workdir}/session1.sql" \
  >"${workdir}/session1.out" 2>"${workdir}/session1.err" &
session1_pid=$!

print_debug() {
  for f in session1.out session1.err session2.out session2.err session3.out session3.err; do
    echo "--- ${f} ---" >&2
    cat "${workdir}/${f}" >&2 2>/dev/null || true
  done
}

# Wait until session 1 visibly holds the claim (owner_pid set in the view).
session1_ready=0
for _ in $(seq 1 50); do
  if "${psql_base[@]}" -tAc "
    select 1
    from pgque.partition_slot_status
    where queue_name = '${queue_name}'
      and consumer = 'w'
      and slot = 0
      and owner_pid is not null
    limit 1
  " | grep -q 1; then
    session1_ready=1
    break
  fi
  sleep 0.2
done
if (( session1_ready != 1 )); then
  echo "FAIL: session1 never showed up as slot 0 owner in partition_slot_status" >&2
  print_debug
  exit 1
fi

set +e
"${psql_base[@]}" -f "${workdir}/session2.sql" >"${workdir}/session2.out" 2>"${workdir}/session2.err"
session2_status=$?
wait "${session1_pid}"
session1_status=$?
set -e

if (( session1_status != 0 || session2_status != 0 )); then
  echo "FAIL: claim harness failed (session1=${session1_status}, session2=${session2_status})" >&2
  print_debug
  exit 1
fi

# Session 1 exited; its advisory claim releases with the backend. Retry a
# few times to absorb backend-exit latency.
session3_ok=0
for _ in $(seq 1 50); do
  if "${psql_base[@]}" -f "${workdir}/session3.sql" \
      >"${workdir}/session3.out" 2>"${workdir}/session3.err"; then
    session3_ok=1
    break
  fi
  sleep 0.2
done
if (( session3_ok != 1 )); then
  echo "FAIL: slot 0 did not become claimable after session1 exit" >&2
  print_debug
  exit 1
fi

echo "PASS: slot claim exclusive across sessions; owner visible in partition_slot_status; dead session's slot reclaimable"
