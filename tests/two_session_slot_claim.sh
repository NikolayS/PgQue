#!/usr/bin/env bash
# Validate lease-based partition slot ownership across real backends
# (US-12.4 single processor per slot, US-12.5 claim/release + crash recovery).
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
set -Eeuo pipefail
IFS=$'\n\t'

# Usage:
#   PGQUE_TEST_DSN=postgresql://postgres:***@localhost/pgque_test \
#     tests/two_session_slot_claim.sh
#
# The target database must already have devel/sql/pgque.sql and
# devel/sql/pgque-api/partition_keys.sql installed.
#
# Slot ownership is a batch-granularity LEASE (worker id + TTL + epoch) held
# in pgque.partition_slot as plain transactional DML -- no session state, no
# advisory locks. This is deliberately different from a backend-scoped lock:
# a lease OUTLIVES the backend that took it. A crashed worker does not free
# its slot immediately; the slot recovers only when the TTL expires, and the
# takeover bumps the epoch (a monotonic fencing token) so the zombie can be
# fenced off. That TTL window is the trade we accept for pooler-friendly,
# stateless ownership.
#
# The harness registers a 2-slot partitioned consumer, then across separate
# psql backends proves:
#   - session 1 claims slot 0 as 'w1' under a short TTL, then EXITS without
#     releasing (a simulated crash)
#   - a contender returns NULL promptly while another transaction holds the
#     slot row, instead of blocking its first-free scan
#   - session 2 sees claim_slot(slot 0) IS NULL -- the lease outlives the
#     dead backend (US-12.4 exclusivity, and the TTL trade vs advisory locks)
#   - session 2 claims the free slot 1 as 'w2'; the status view attributes
#     slot 0 to 'w1' and slot 1 to 'w2' (US-12.6)
#   - session 3 recovers slot 0 as 'w3' once the TTL expires, and the epoch
#     it gets back is STRICTLY GREATER than session 1's (US-12.5 fencing)

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

export PAGER=cat
psql_base=(psql --no-psqlrc --set=ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
queue_name="two_session_slot_claim_${$}_$(date +%s)"
s1_ttl="3 seconds"
workdir="$(mktemp -d)"

# Remove database and filesystem fixtures while preserving the script status.
# Globals: psql_base, queue_name, workdir
# Args: none
# Returns: the status that triggered the EXIT trap
cleanup() {
  local exit_code=$?

  "${psql_base[@]}" --quiet --no-align --tuples-only --command="
    select pgque.unsubscribe_slot('${queue_name}', 'w', 0);
    select pgque.unsubscribe_slot('${queue_name}', 'w', 1);
    select pgque.drop_queue('${queue_name}', true);
  " >/dev/null 2>&1 || true
  rm -rf -- "${workdir}" || true
  exit "${exit_code}"
}
trap cleanup EXIT

# Print captured session output after a harness failure.
# Globals: workdir
# Args: none
# Outputs: captured files to STDERR
# Returns: zero
print_debug() {
  local f

  for f in setup.out lock_holder.out lock_holder.err contender.out \
           contender.err session1.out session1.err session2.out session2.err \
           session3.out session3.err; do
    echo "--- ${f} ---" >&2
    cat "${workdir}/${f}" >&2 2>/dev/null || true
  done
}

# --- setup: 2-slot partitioned consumer -----------------------------------
cat >"${workdir}/setup.sql" <<SQL
select pgque.create_queue('${queue_name}');
select pgque.subscribe_slot('${queue_name}', 'w', 0, 2);
select pgque.subscribe_slot('${queue_name}', 'w', 1, 2);
SQL

"${psql_base[@]}" --file="${workdir}/setup.sql" \
  >"${workdir}/setup.out" 2>&1 || {
  echo "FAIL: setup failed" >&2
  print_debug
  exit 1
}

# --- non-blocking claim: steer around an uncommitted slot row -------------
lock_app="pgque_slot_lock_holder_${$}_$(date +%s)"
cat >"${workdir}/lock_holder.sql" <<SQL
begin;
select pgque.claim_slot('${queue_name}', 'w', 0, 'lock-holder');
select pg_sleep(10);
rollback;
SQL

PGAPPNAME="${lock_app}" "${psql_base[@]}" --file="${workdir}/lock_holder.sql" \
  >"${workdir}/lock_holder.out" 2>"${workdir}/lock_holder.err" &
lock_holder_pid=$!

lock_ready=0
for _ in $(seq 1 50); do
  if "${psql_base[@]}" --quiet --no-align --tuples-only --command="
    select 1
    from pg_stat_activity
    where application_name = '${lock_app}'
      and state = 'active'
      and wait_event_type = 'Timeout'
      and wait_event = 'PgSleep'
    limit 1
  " | grep -q 1; then
    lock_ready=1
    break
  fi
  sleep 0.1
done
if (( lock_ready != 1 )); then
  kill "${lock_holder_pid}" >/dev/null 2>&1 || true
  wait "${lock_holder_pid}" >/dev/null 2>&1 || true
  echo "FAIL: lock holder did not acquire the slot row" >&2
  print_debug
  exit 1
fi

set +e
PGOPTIONS='-c statement_timeout=750ms' "${psql_base[@]}" \
  --quiet --no-align --tuples-only --command \
  "select coalesce(pgque.claim_slot('${queue_name}', 'w', 0, 'contender')::text, 'NULL')" \
  >"${workdir}/contender.out" 2>"${workdir}/contender.err"
contender_status=$?
set -e
"${psql_base[@]}" --quiet --no-align --tuples-only --command="
  select pg_terminate_backend(pid)
  from pg_stat_activity
  where application_name = '${lock_app}'
" >/dev/null 2>&1 || true
kill "${lock_holder_pid}" >/dev/null 2>&1 || true
wait "${lock_holder_pid}" >/dev/null 2>&1 || true

# The client process can exit just before the server backend observes the
# closed socket and rolls its transaction back. Wait for that rollback so the
# crash-recovery scenario below starts from a genuinely free slot.
holder_gone=0
for _ in $(seq 1 50); do
  if [[ "$("${psql_base[@]}" --quiet --no-align --tuples-only --command="
    select count(*)
    from pg_stat_activity
    where application_name = '${lock_app}'
  ")" = "0" ]]; then
    holder_gone=1
    break
  fi
  sleep 0.1
done
if (( holder_gone != 1 )); then
  echo "FAIL: lock-holder backend did not roll back after disconnect" >&2
  print_debug
  exit 1
fi

if (( contender_status != 0 )); then
  echo "FAIL: claim_slot blocked behind an uncommitted lease row" >&2
  print_debug
  exit 1
fi
if [[ "$(tr -d '[:space:]' <"${workdir}/contender.out")" != "NULL" ]]; then
  echo "FAIL: busy locked slot must return NULL" >&2
  print_debug
  exit 1
fi
echo "non-blocking claim: contender returned NULL behind an uncommitted slot row"

# --- session 1: claim slot 0 as 'w1' under a short TTL, then exit ----------
# No release_slot call: the backend dies with the lease still held. The lease
# is committed transactional state, so it survives the backend's exit.
s1_epoch="$(
  "${psql_base[@]}" --quiet --no-align --tuples-only --command \
    "select pgque.claim_slot('${queue_name}', 'w', 0, 'w1', interval '${s1_ttl}')"
)"
if [[ -z "${s1_epoch}" || ! "${s1_epoch}" =~ ^[0-9]+$ ]]; then
  echo "FAIL: session 1 claim of free slot 0 must return an epoch, got: '${s1_epoch}'" >&2
  print_debug
  exit 1
fi
echo "session 1: claimed slot 0 as 'w1', epoch=${s1_epoch} (now exiting without release)"

# --- session 2: fresh backend, immediately after session 1's death ---------
cat >"${workdir}/session2.sql" <<SQL
do \$\$
declare
  v_owner text;
begin
  -- The lease outlives session 1's dead backend: still exclusive (US-12.4).
  assert pgque.claim_slot('${queue_name}', 'w', 0, 'w2') is null,
    'session2: slot 0 lease must outlive the dead backend (US-12.4)';

  -- Free slot 1 is claimable.
  assert pgque.claim_slot('${queue_name}', 'w', 1, 'w2') is not null,
    'session2: claim of free slot 1 must return an epoch';

  select lease_owner into v_owner
  from pgque.partition_slot_status
  where queue_name = '${queue_name}' and consumer = 'w' and slot = 0;
  assert v_owner = 'w1',
    format('session2: slot 0 lease_owner must be w1, got %s (US-12.6)', v_owner);

  select lease_owner into v_owner
  from pgque.partition_slot_status
  where queue_name = '${queue_name}' and consumer = 'w' and slot = 1;
  assert v_owner = 'w2',
    format('session2: slot 1 lease_owner must be w2, got %s (US-12.6)', v_owner);

  -- Release slot 1 at the batch boundary so only slot 0 is left leased.
  assert pgque.release_slot('${queue_name}', 'w', 1, 'w2'),
    'session2: release of held slot 1 must return true (US-12.5)';
end \$\$;
select 's2_checks=ok';
SQL

set +e
"${psql_base[@]}" --file="${workdir}/session2.sql" \
  >"${workdir}/session2.out" 2>"${workdir}/session2.err"
session2_status=$?
set -e
if (( session2_status != 0 )); then
  echo "FAIL: session 2 checks failed (status=${session2_status})" >&2
  print_debug
  exit 1
fi

# --- session 3: recover slot 0 once w1's lease expires ---------------------
# w1 never released; slot 0 becomes claimable only after the TTL lapses. Poll
# until takeover succeeds, then prove the epoch advanced past session 1's.
cat >"${workdir}/session3.sql" <<SQL
do \$\$
declare
  v_epoch bigint;
begin
  for i in 1..30 loop
    v_epoch := pgque.claim_slot('${queue_name}', 'w', 0, 'w3');
    exit when v_epoch is not null;
    perform pg_sleep(0.3);
  end loop;

  assert v_epoch is not null,
    'session3: slot 0 must become claimable after w1''s lease expires (US-12.5)';
  assert v_epoch > ${s1_epoch},
    format('session3: takeover epoch must exceed session 1''s %s, got %s (US-12.5 fencing)',
      ${s1_epoch}, v_epoch);

  perform pgque.release_slot('${queue_name}', 'w', 0, 'w3');
end \$\$;
select 's3_reclaim=ok';
SQL

set +e
"${psql_base[@]}" --file="${workdir}/session3.sql" \
  >"${workdir}/session3.out" 2>"${workdir}/session3.err"
session3_status=$?
set -e
if (( session3_status != 0 )); then
  echo "FAIL: session 3 could not recover slot 0 after TTL expiry" >&2
  print_debug
  exit 1
fi

echo "PASS: lease is exclusive across backends (outlives a dead one for its TTL); dead worker's slot recovered after TTL expiry; epoch bumped on takeover (fencing)"
