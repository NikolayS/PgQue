#!/usr/bin/env bash
# Deterministic multi-backend acceptance for durable page ownership.
set -Eeuo pipefail

repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
container="pgque-paged-concurrency-$$"
password="$(openssl rand -hex 24)"
tmpdir="$(mktemp -d)"
postgres_image="${PGQUE_TEST_IMAGE:-postgres:18}"

cleanup() {
  docker rm -f --volumes "${container}" >/dev/null 2>&1 || true
  rm -rf "${tmpdir}"
}
trap cleanup EXIT

fail() {
  echo "FAIL: $*" >&2
  for file in "${tmpdir}"/*.out "${tmpdir}"/*.err; do
    [[ -e "${file}" ]] || continue
    echo "--- ${file##*/} ---" >&2
    sed -n '1,240p' "${file}" >&2
  done
  exit 1
}

docker run --detach --name "${container}" \
  --env POSTGRES_PASSWORD="${password}" \
  --env POSTGRES_DB=pgque_test \
  --env POSTGRES_INITDB_ARGS=--auth-host=scram-sha-256 \
  --volume "${repo}:/repo:ro" \
  "${postgres_image}" \
  -c listen_addresses=localhost \
  -c log_connections=on >/dev/null

psql_test() {
  docker exec --interactive --env PGPASSWORD="${password}" \
    --env 'PGOPTIONS=-c statement_timeout=15s' \
    --workdir /repo "${container}" \
    psql -X -qAt -h 127.0.0.1 -U postgres -d pgque_test \
    -v ON_ERROR_STOP=1 "$@"
}

ready=0
for _ in $(seq 1 30); do
  if psql_test -c 'select 1' >/dev/null 2>&1; then
    ready=1
    break
  fi
  sleep 1
done
[[ "${ready}" = 1 ]] || fail 'Postgres did not become ready'

psql_test -f devel/sql/pgque.sql >"${tmpdir}/install.out" \
  2>"${tmpdir}/install.err" || fail 'installer failed'

# One event gives every racing receiver the same immutable page.
psql_test <<'SQL' >"${tmpdir}/setup.out" 2>"${tmpdir}/setup.err" || fail 'setup failed'
select pgque.create_queue('paged_race');
select pgque.subscribe('paged_race', 'c1');
select pgque.send('paged_race', 'race', '{"n":1}'::text);
select pgque.force_next_tick('paged_race');
select pgque.ticker();
SQL

# Session 1 issues a page, records its token, then deliberately holds the
# subscription/page locks. Session 2 must block and, after commit, see busy.
psql_test -c "
  begin;
  select page_token
  from pgque.receive_page(
    'paged_race', 'c1', 'worker-one', 1, interval '1 minute'
  );
  set application_name = 'pgque_paged_receiver1';
  select pg_sleep(5);
  commit;
" >"${tmpdir}/receiver1.out" 2>"${tmpdir}/receiver1.err" &
receiver1_pid=$!

receiver1_ready=0
for _ in $(seq 1 50); do
  if psql_test -c "
    select exists (
      select 1 from pg_stat_activity
      where application_name = 'pgque_paged_receiver1'
        and wait_event = 'PgSleep'
    )
  " | grep -qx t; then
    receiver1_ready=1
    break
  fi
  sleep 0.1
done
[[ "${receiver1_ready}" = 1 ]] \
  || fail 'receiver 1 did not reach lock-holding pg_sleep'

race_started_ns="$(date +%s%N)"
psql_test -F '|' -c "
  select status, page_token is null, cardinality(messages)
  from pgque.receive_page(
    'paged_race', 'c1', 'worker-two', 1, interval '1 minute'
  )
" >"${tmpdir}/receiver2.out" 2>"${tmpdir}/receiver2.err" \
  || fail 'receiver 2 race failed'
race_finished_ns="$(date +%s%N)"
wait "${receiver1_pid}" || fail 'receiver 1 failed'

race_wait_ms=$(( (race_finished_ns - race_started_ns) / 1000000 ))
[[ "${race_wait_ms}" -ge 3000 ]] \
  || fail "receiver 2 did not block on issuance locks (${race_wait_ms}ms)"
grep -qx 'busy|t|0' "${tmpdir}/receiver2.out" \
  || fail 'competing receiver did not return metadata-only busy'

# A fresh backend using the same process identity gets byte-for-byte ownership
# metadata for the already-issued page. This models pool reconnect/redelivery.
psql_test -F '|' -c "
  select page_token, status, page_number, cardinality(messages), is_last
  from pgque.receive_page(
    'paged_race', 'c1', 'worker-one', 99, interval '10 minutes'
  )
" >"${tmpdir}/same_worker.out" 2>"${tmpdir}/same_worker.err" \
  || fail 'same-worker reconnect failed'
first_token="$(cut -d '|' -f 1 "${tmpdir}/same_worker.out")"
[[ "${first_token}" =~ ^[0-9a-f-]{36}$ ]] \
  || fail "same-worker reconnect did not return a UUID token: ${first_token}"
grep -qx "${first_token}|page|1|1|t" "${tmpdir}/same_worker.out" \
  || fail 'same-worker reconnect did not redeliver the identical page'

# Commit the ack in one backend and throw away its result, exactly the state a
# client sees after losing the response. A reconnect must replay the receipt.
psql_test -c "
  select * from pgque.ack_page('${first_token}', 'worker-one')
" >/dev/null 2>"${tmpdir}/lost_ack.err" || fail 'lost-response ack failed'
psql_test -F '|' -c "
  select * from pgque.ack_page('${first_token}', 'worker-one')
" >"${tmpdir}/ack_replay.out" 2>"${tmpdir}/ack_replay.err" \
  || fail 'ack replay after reconnect failed'
grep -qx 'already_acked|t' "${tmpdir}/ack_replay.out" \
  || fail 'lost ack response did not replay the committed terminal receipt'

# Expiry takeover preserves the page interval but rotates the token. The old
# backend can reconnect, but its token is fenced with PQP01.
psql_test <<'SQL' >"${tmpdir}/takeover_setup.out" 2>"${tmpdir}/takeover_setup.err" \
  || fail 'takeover setup failed'
select pgque.create_queue('paged_takeover_race');
select pgque.subscribe('paged_takeover_race', 'c1');
select pgque.send('paged_takeover_race', 'takeover', '{"n":1}'::text);
select pgque.send('paged_takeover_race', 'takeover', '{"n":2}'::text);
select pgque.force_next_tick('paged_takeover_race');
select pgque.ticker();
SQL

old_row="$(psql_test -F '|' -c "
  select page_token, ((messages)[1]).msg_id
  from pgque.receive_page(
    'paged_takeover_race', 'c1', 'worker-old', 1, interval '1 second'
  )
")"
old_token="${old_row%%|*}"
old_msg_id="${old_row##*|}"
sleep 1.2
new_row="$(psql_test -F '|' -c "
  select page_token, ((messages)[1]).msg_id, page_number
  from pgque.receive_page(
    'paged_takeover_race', 'c1', 'worker-new', 99, interval '1 minute'
  )
")"
IFS='|' read -r new_token new_msg_id new_page_number <<<"${new_row}"
[[ "${new_token}" != "${old_token}" && "${new_msg_id}" = "${old_msg_id}" \
  && "${new_page_number}" = 1 ]] \
  || fail 'expired takeover did not rotate token while preserving page boundary'

psql_test -c "
  do \$\$
  declare
    v_state text;
  begin
    begin
      perform * from pgque.ack_page('${old_token}', 'worker-old');
      assert false, 'superseded token unexpectedly acked';
    exception when others then
      get stacked diagnostics v_state = returned_sqlstate;
      assert v_state = 'PQP01',
        'superseded token must raise PQP01, got ' || v_state;
    end;
  end
  \$\$;
" >"${tmpdir}/stale_ack.out" 2>"${tmpdir}/stale_ack.err" \
  || fail 'superseded token fencing check failed'
psql_test -F '|' -c "
  select * from pgque.ack_page('${new_token}', 'worker-new')
" >"${tmpdir}/takeover_ack.out" 2>"${tmpdir}/takeover_ack.err" \
  || fail 'successor ack failed'
grep -qx 'acked|f' "${tmpdir}/takeover_ack.out" \
  || fail 'successor first-page ack returned the wrong receipt'

# Partition routing must take the slot lock before page/subscription locks.
# Holding the slot row blocks receive; after release, receive completes with
# the same epoch rather than deadlocking or bypassing the authority row.
psql_test <<'SQL' >"${tmpdir}/slot_setup.out" 2>"${tmpdir}/slot_setup.err" \
  || fail 'partition lock-order setup failed'
select pgque.create_queue('paged_slot_lock');
select pgque.subscribe_slot('paged_slot_lock', 'part_c', 0, 1);
select pgque.send('paged_slot_lock', 'part', '{"n":1}'::text, 'key');
select pgque.force_next_tick('paged_slot_lock');
select pgque.ticker();
select pgque.claim_slot('paged_slot_lock', 'part_c', 0, 'slot-worker', interval '1 minute');
SQL

psql_test -c "
  begin;
  select 1
  from pgque.partition_slot as ps
  join pgque.queue as q on q.queue_id = ps.queue_id
  where q.queue_name = 'paged_slot_lock'
    and ps.co_name = 'part_c'
    and ps.slot = 0
  for update of ps;
  set application_name = 'pgque_paged_slot_holder';
  select pg_sleep(5);
  commit;
" >"${tmpdir}/slot_holder.out" 2>"${tmpdir}/slot_holder.err" &
slot_holder_pid=$!

slot_holder_ready=0
for _ in $(seq 1 50); do
  if psql_test -c "
    select exists (
      select 1 from pg_stat_activity
      where application_name = 'pgque_paged_slot_holder'
        and wait_event = 'PgSleep'
    )
  " | grep -qx t; then
    slot_holder_ready=1
    break
  fi
  sleep 0.1
done
[[ "${slot_holder_ready}" = 1 ]] \
  || fail 'slot holder did not reach lock-holding pg_sleep'

slot_started_ns="$(date +%s%N)"
psql_test -F '|' -c "
  set application_name = 'pgque_paged_slot_waiter';
  select status, fence_epoch, cardinality(messages)
  from pgque.receive_page_partitioned(
    'paged_slot_lock', 'part_c', 0, 1, 'slot-worker', 1
  )
" >"${tmpdir}/slot_receiver.out" 2>"${tmpdir}/slot_receiver.err" \
  &
slot_receiver_pid=$!

slot_waiter_blocked=0
for _ in $(seq 1 50); do
  if psql_test -c "
    select exists (
      select 1 from pg_stat_activity
      where application_name = 'pgque_paged_slot_waiter'
        and wait_event_type = 'Lock'
    )
  " | grep -qx t; then
    slot_waiter_blocked=1
    break
  fi
  sleep 0.1
done
[[ "${slot_waiter_blocked}" = 1 ]] \
  || fail 'partition receiver did not block on the held slot row'

# If the waiter took subscription first, this NOWAIT probe fails. Passing
# while the waiter is blocked proves slot -> subscription/page ordering.
psql_test -c "
  begin;
  select 1
  from pgque.subscription as s
  join pgque.queue as q on q.queue_id = s.sub_queue
  where q.queue_name = 'paged_slot_lock'
  for update of s nowait;
  rollback;
" >"${tmpdir}/subscription_probe.out" 2>"${tmpdir}/subscription_probe.err" \
  || fail 'slot waiter locked subscription before slot (lock-order inversion)'

wait "${slot_receiver_pid}" \
  || fail 'partition receiver failed after slot lock release'
slot_finished_ns="$(date +%s%N)"
wait "${slot_holder_pid}" || fail 'slot lock holder failed'
slot_wait_ms=$(( (slot_finished_ns - slot_started_ns) / 1000000 ))
[[ "${slot_wait_ms}" -ge 3000 ]] \
  || fail "partition receive bypassed the slot lock (${slot_wait_ms}ms)"
grep -Eq '^page\|[1-9][0-9]*\|1$' "${tmpdir}/slot_receiver.out" \
  || fail 'partition receive returned wrong page metadata after lock release'

# Kill real backends on each side of the receive/ack transaction boundary.
wait_for_sleep() {
  local application="$1"
  local observed=0
  for _ in $(seq 1 100); do
    if [[ "$(psql_test -c "select count(*) from pg_stat_activity where application_name = '${application}' and wait_event = 'PgSleep'")" = 1 ]]; then
      observed=1
      break
    fi
    sleep 0.05
  done
  [[ "$observed" = 1 ]] || fail "backend ${application} did not reach transaction barrier"
}
psql_test <<'SQL' >/dev/null
select pgque.create_queue('paged_crash');
select pgque.subscribe('paged_crash', 'c1');
select pgque.send('paged_crash', 'crash', 'first');
select pgque.send('paged_crash', 'crash', 'second');
select pgque.force_next_tick('paged_crash');
select pgque.ticker('paged_crash');
SQL
psql_test -c "set application_name = 'page_crash_receive'; begin;
  select page_token from pgque.receive_page('paged_crash','c1','crashed',1);
  select pg_sleep(10); commit;" >"${tmpdir}/crash_receive.out" 2>"${tmpdir}/crash_receive.err" &
crash_pid=$!
wait_for_sleep page_crash_receive
psql_test -c "select pg_terminate_backend(pid) from pg_stat_activity where application_name='page_crash_receive'" >/dev/null
if wait "$crash_pid"; then fail 'terminated receive backend unexpectedly committed'; fi
crash_row="$(psql_test -F '|' -c "select page_token, ((messages)[1]).payload, page_number from pgque.receive_page('paged_crash','c1','survivor',1)")"
IFS='|' read -r crash_token crash_payload crash_number <<<"${crash_row}"
[[ "$crash_payload" = first && "$crash_number" = 1 ]] || fail 'uncommitted receive crash advanced progress'
psql_test -c "set application_name = 'page_crash_ack'; begin;
  select * from pgque.ack_page('${crash_token}','survivor');
  select pg_sleep(10); commit;" >"${tmpdir}/crash_ack.out" 2>"${tmpdir}/crash_ack.err" &
crash_pid=$!
wait_for_sleep page_crash_ack
psql_test -c "select pg_terminate_backend(pid) from pg_stat_activity where application_name='page_crash_ack'" >/dev/null
if wait "$crash_pid"; then fail 'terminated ack backend unexpectedly committed'; fi
crash_repeat="$(psql_test -F '|' -c "select page_token, ((messages)[1]).payload, page_number from pgque.receive_page('paged_crash','c1','survivor',1)")"
[[ "$crash_repeat" = "$crash_row" ]] || fail 'uncommitted ack crash changed pending page'
[[ "$(psql_test -F '|' -c "select * from pgque.ack_page('${crash_token}','survivor')")" = 'acked|f' ]] || fail 'crashed ack was incorrectly recorded as committed'
[[ "$(psql_test -F '|' -c "select ((messages)[1]).payload, page_number, is_last from pgque.receive_page('paged_crash','c1','survivor',1)")" = 'second|2|t' ]] || fail 'crash recovery skipped or repeated committed progress'

# An in-flight producer must not leak into an older immutable tick window.
psql_test <<'SQL' >/dev/null
select pgque.create_queue('paged_long_producer');
select pgque.subscribe('paged_long_producer', 'c1');
SQL
psql_test -c "set application_name='page_long_producer'; begin;
  select pgque.send('paged_long_producer','producer','late');
  select pg_sleep(4); commit;" >"${tmpdir}/long_producer.out" 2>"${tmpdir}/long_producer.err" &
producer_pid=$!
wait_for_sleep page_long_producer
psql_test <<'SQL' >/dev/null
select pgque.send('paged_long_producer','producer','early');
select pgque.force_next_tick('paged_long_producer');
select pgque.ticker('paged_long_producer');
do $$
declare p pgque.batch_page;
begin
  p := pgque.receive_page('paged_long_producer','c1','reader',1);
  assert p.status = 'page' and p.is_last and (p.messages[1]).payload = 'early';
  perform pgque.ack_page(p.page_token,'reader');
end $$;
SQL
wait "$producer_pid" || fail 'long producer did not commit'
psql_test <<'SQL' >/dev/null
select pgque.force_next_tick('paged_long_producer');
select pgque.ticker('paged_long_producer');
do $$
declare p pgque.batch_page;
begin
  p := pgque.receive_page('paged_long_producer','c1','reader',1);
  assert p.status = 'page' and p.is_last and (p.messages[1]).payload = 'late';
  perform pgque.ack_page(p.page_token,'reader');
end $$;
SQL

echo 'PASS: terminated receive/ack backends rolled back; committed progress recovered; long producer visible only in next tick window'

echo "PASS: concurrent receive serialized (${race_wait_ms}ms); same-worker reconnect redelivered; lost ack replayed; takeover fenced stale token; partition receive obeyed slot-first locking (${slot_wait_ms}ms)"
