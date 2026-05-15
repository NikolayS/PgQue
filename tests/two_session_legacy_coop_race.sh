#!/usr/bin/env bash
# Validate the legacy (5-arg) next_batch_custom locks the main subscription row
# against a concurrent register_subconsumer, so that a `normal` consumer cannot
# be silently promoted to `coop_main` between the legacy function's read and
# write.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
# Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_DSN=postgresql://postgres:***@localhost/pgque_test \
#     tests/two_session_legacy_coop_race.sh
#
# The target database must already have sql/pgque.sql installed. The harness
# temporarily installs a stand-in pgque.find_tick_helper that waits on a
# session-level advisory lock before returning the next tick. find_tick_helper
# is called from inside next_batch_custom AFTER the function's initial SELECT
# from pgque.subscription and BEFORE the UPDATE that stamps sub_batch. With
# session A wedged inside find_tick_helper, the harness fires
# register_subconsumer from session B.
#
# If legacy next_batch_custom did NOT lock the main subscription row, session B
# proceeds, promotes the row to coop_main, and commits while session A still
# holds a stale `sub_role = 'normal'` snapshot. Once the advisory lock is
# released, A's UPDATE stamps sub_batch on the now-coop_main row, violating the
# spec invariant: "coop_main must never have sub_batch IS NOT NULL".
#
# Expected outcomes:
#   - Pre-fix (no FOR UPDATE on the legacy SELECT): FAIL — session B succeeds
#     while A is paused, and A's UPDATE stamps the coop_main row.
#   - Post-fix (FOR UPDATE on the legacy SELECT):   PASS — session B blocks
#     behind A's row lock and is rejected after A commits a normal batch.
#
# The harness restores the original find_tick_helper at the end. If the harness
# is killed mid-run, re-install sql/pgque.sql to get the original definition
# back.

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(psql --no-psqlrc -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
suffix="${$}_$(date +%s)"
queue_name="legacy_coop_race_${suffix}"
session_a_app="pgque_legacy_coop_race_a_${suffix}"
session_lock_app="pgque_legacy_coop_race_lock_${suffix}"
advisory_lock_key=77889911
workdir="$(mktemp -d)"
lock_pid=""

# Capture the original find_tick_helper definition so we can restore it at exit.
original_find_tick_def="$(
    "${psql_base[@]}" -qAtX -c \
        "select pg_get_functiondef('pgque.find_tick_helper(int4,int8,timestamptz,int8,int8,interval)'::regprocedure);"
)"
if [[ -z "${original_find_tick_def}" ]]; then
    echo "FAIL: could not capture pgque.find_tick_helper definition; is pgque.sql installed?" >&2
    exit 1
fi

cleanup() {
    if [[ -n "${lock_pid}" ]]; then
        kill "${lock_pid}" 2>/dev/null || true
        wait "${lock_pid}" 2>/dev/null || true
    fi
    # Restore original find_tick_helper unconditionally.
    "${psql_base[@]}" -v ON_ERROR_STOP=0 <<RESTORE >/dev/null 2>&1 || true
${original_find_tick_def};
RESTORE
    "${psql_base[@]}" -qAtc "
        select pgque.unregister_consumer('${queue_name}', 'billing');
        select pgque.unregister_consumer('${queue_name}', 'billing.w1');
        select pgque.drop_queue('${queue_name}', true);
    " >/dev/null 2>&1 || true
    rm -rf "${workdir}"
}
trap cleanup EXIT

cat >"${workdir}/setup.sql" <<SQL
select pgque.create_queue('${queue_name}');
select pgque.register_consumer('${queue_name}', 'billing');
select pgque.insert_event('${queue_name}', 'test.legacy_coop_race', '{"n":1}');
select pgque.force_tick('${queue_name}');
select pgque.ticker('${queue_name}');

-- Test-only override of find_tick_helper. Pauses on a session-level advisory
-- lock before returning the next tick. This sits in the legacy next_batch_custom
-- flow exactly between the function's initial SELECT from pgque.subscription
-- and its UPDATE, opening a deterministic race window. Restored to the
-- original definition by the harness cleanup.
create or replace function pgque.find_tick_helper(
    in i_queue_id int4,
    in i_prev_tick_id int8,
    in i_prev_tick_time timestamptz,
    in i_prev_tick_seq int8,
    in i_min_count int8,
    in i_min_interval interval,
    out next_tick_id int8,
    out next_tick_time timestamptz,
    out next_tick_seq int8)
as \$test\$
begin
    perform pg_advisory_lock(${advisory_lock_key});
    perform pg_advisory_unlock(${advisory_lock_key});

    select tick_id, tick_time, tick_event_seq
      into next_tick_id, next_tick_time, next_tick_seq
      from pgque.tick
     where tick_queue = i_queue_id
       and tick_id > i_prev_tick_id
     order by tick_queue asc, tick_id asc
     limit 1;
    return;
end
\$test\$ language plpgsql stable;
SQL

"${psql_base[@]}" -f "${workdir}/setup.sql" >/dev/null

print_debug() {
    echo "--- lock_holder.out ---" >&2
    cat "${workdir}/lock_holder.out" 2>/dev/null || true
    echo "--- lock_holder.err ---" >&2
    cat "${workdir}/lock_holder.err" 2>/dev/null || true
    echo "--- sessionA.out ---" >&2
    cat "${workdir}/sessionA.out" 2>/dev/null || true
    echo "--- sessionA.err ---" >&2
    cat "${workdir}/sessionA.err" 2>/dev/null || true
    echo "--- sessionB.out ---" >&2
    cat "${workdir}/sessionB.out" 2>/dev/null || true
    echo "--- sessionB.err ---" >&2
    cat "${workdir}/sessionB.err" 2>/dev/null || true
}

# Lock holder: takes the session-level advisory lock that pauses A's
# find_tick_helper call. Sleeps so the session stays alive; harness kills it
# to deterministically release the lock.
cat >"${workdir}/lock_holder.sql" <<SQL
select pg_advisory_lock(${advisory_lock_key});
\\echo lock_acquired
select pg_sleep(60);
SQL
PGAPPNAME="${session_lock_app}" \
    "${psql_base[@]}" -X -q -f "${workdir}/lock_holder.sql" \
    >"${workdir}/lock_holder.out" 2>"${workdir}/lock_holder.err" &
lock_pid=$!

# Wait for the lock holder to actually hold the advisory lock.
lock_ready=0
for _ in $(seq 1 100); do
    if grep -q '^lock_acquired$' "${workdir}/lock_holder.out" 2>/dev/null; then
        lock_ready=1
        break
    fi
    sleep 0.1
done
if (( lock_ready != 1 )); then
    echo "FAIL: lock holder did not acquire advisory lock ${advisory_lock_key}" >&2
    print_debug
    exit 1
fi

# Session A: open a tx and call legacy next_batch_custom with i_min_count = 1
# so it routes through find_tick_helper and pauses inside the test override.
cat >"${workdir}/sessionA.sql" <<SQL
begin;
select 'A:next_batch_start' as marker;
select batch_id from pgque.next_batch_custom('${queue_name}', 'billing', null, 1, null);
select 'A:next_batch_returned' as marker;
commit;
SQL
PGAPPNAME="${session_a_app}" \
    "${psql_base[@]}" -f "${workdir}/sessionA.sql" \
    >"${workdir}/sessionA.out" 2>"${workdir}/sessionA.err" &
sessionA_pid=$!

# Wait until A is wedged on the advisory lock inside find_tick_helper.
a_wedged=0
for _ in $(seq 1 200); do
    if "${psql_base[@]}" -tAc "
        select 1
          from pg_stat_activity
         where application_name = '${session_a_app}'
           and wait_event_type = 'Lock'
           and wait_event = 'advisory'
         limit 1
    " | grep -q 1; then
        a_wedged=1
        break
    fi
    sleep 0.05
done
if (( a_wedged != 1 )); then
    echo "FAIL: session A never reached the advisory-lock pause inside find_tick_helper" >&2
    print_debug
    exit 1
fi

# Session B: register_subconsumer. With the fix, this blocks on the main
# subscription row's FOR UPDATE (held by A's SELECT) and is rejected once A
# commits with sub_batch set. Without the fix, it completes immediately and
# silently promotes the row to coop_main while A is still mid-function.
# lock_timeout caps how long we'll allow B to wait before declaring success
# for the "B was properly blocked" branch.
cat >"${workdir}/sessionB.sql" <<SQL
\\set ON_ERROR_STOP 0
\\timing on
select 'B:register_start' as marker;
-- convert_normal := true: B asks to promote the existing normal consumer to
-- coop_main. With the fix, B blocks on A's FOR UPDATE and is then rejected
-- because A has stamped sub_batch. Without the fix, this conversion races A
-- and silently leaves a coop_main row with sub_batch set.
select pgque.register_subconsumer('${queue_name}', 'billing', 'w1', true) as b_register;
select 'B:register_returned' as marker;
SQL
b_start_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
PGOPTIONS="-c lock_timeout=3000" \
    "${psql_base[@]}" -v ON_ERROR_STOP=0 -f "${workdir}/sessionB.sql" \
    >"${workdir}/sessionB.out" 2>"${workdir}/sessionB.err" || true
b_end_ms=$(python3 -c 'import time; print(int(time.time()*1000))')
b_elapsed_ms=$((b_end_ms - b_start_ms))

# Release A by killing the lock holder.
if [[ -n "${lock_pid}" ]]; then
    kill "${lock_pid}" 2>/dev/null || true
    wait "${lock_pid}" 2>/dev/null || true
    lock_pid=""
fi

# Reap A.
wait "${sessionA_pid}"
sessionA_status=$?

# Inspect the final subscription state on the original (main) row.
final_state=$("${psql_base[@]}" -tAc "
    select coalesce(s.sub_role, 'MISSING') || '|' || coalesce(s.sub_batch::text, 'NULL')
      from pgque.subscription s
      join pgque.queue q on q.queue_id = s.sub_queue
      join pgque.consumer c on c.co_id = s.sub_consumer
     where q.queue_name = '${queue_name}'
       and c.co_name = 'billing'
")

b_registered=$(grep -cE '^ b_register $|b_register' "${workdir}/sessionB.out" 2>/dev/null || true)
b_error=$(grep -E 'ERROR' "${workdir}/sessionB.err" 2>/dev/null || true)

echo "session A status: ${sessionA_status}"
echo "session B elapsed: ${b_elapsed_ms} ms"
echo "session B error (if any): ${b_error:-<none>}"
echo "final billing-row state (sub_role|sub_batch): ${final_state}"

# Hard invariant: a coop_main row must never carry a sub_batch.
invariant=$("${psql_base[@]}" -tAc "
    select case
        when s.sub_role = 'coop_main' and s.sub_batch is not null
        then 'VIOLATED'
        else 'OK'
    end
      from pgque.subscription s
      join pgque.queue q on q.queue_id = s.sub_queue
      join pgque.consumer c on c.co_id = s.sub_consumer
     where q.queue_name = '${queue_name}'
       and c.co_name = 'billing'
")

if [[ "${invariant}" == "VIOLATED" ]]; then
    echo "FAIL: legacy next_batch_custom raced with register_subconsumer:" >&2
    echo "      final state is sub_role='coop_main' with sub_batch set;" >&2
    echo "      spec requires coop_main.sub_batch IS NULL." >&2
    print_debug
    exit 1
fi

echo "PASS: legacy next_batch_custom serializes against register_subconsumer; invariant intact (${final_state})"
