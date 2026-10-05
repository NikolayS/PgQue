#!/usr/bin/env bash
# Prove that a partition lease cannot be released while its slot batch is open.
# Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
set -Eeuo pipefail

# Usage:
#   PGQUE_TEST_DSN=postgresql:///pgque_test tests/two_session_partition_release.sh
#
# Each receive/page operation and each release/claim check uses a fresh psql
# backend. Database-state probes are the barriers: a successor is never
# started until the open batch or page checkpoint is committed and visible.

if [[ -z "${PGQUE_TEST_DSN:-}" ]]; then
  echo "PGQUE_TEST_DSN is required" >&2
  exit 2
fi

psql_base=(psql --no-psqlrc -X -v ON_ERROR_STOP=1 "${PGQUE_TEST_DSN}")
run_id="${$}_$(date +%s)"
legacy_queue="partition_release_legacy_${run_id}"
open_page_queue="partition_release_page_open_${run_id}"
partial_page_queue="partition_release_page_partial_${run_id}"
terminal_page_queue="partition_release_page_terminal_${run_id}"
workdir="$(mktemp -d)"
failures=0

cleanup() {
  for queue_name in \
    "${legacy_queue}" "${open_page_queue}" \
    "${partial_page_queue}" "${terminal_page_queue}"; do
    "${psql_base[@]}" -qAtc \
      "select pgque.drop_queue('${queue_name}', true) where exists
         (select 1 from pgque.queue where queue_name = '${queue_name}')" \
      >/dev/null 2>&1 || true
  done
  rm -rf "${workdir}"
}
trap cleanup EXIT

sql_value() {
  "${psql_base[@]}" -qAtc "$1"
}

setup_queue() {
  local queue_name=$1
  local event_count=$2
  local i
  sql_value "select pgque.create_queue('${queue_name}');
             select pgque.subscribe_slot('${queue_name}', 'workers', 0, 1);" >/dev/null
  for ((i = 1; i <= event_count; i++)); do
    sql_value "select pgque.send('${queue_name}', 'event',
                 jsonb_build_object('n', ${i}), 'partition-key');" >/dev/null
  done
  sql_value "select pgque.force_next_tick('${queue_name}'); select pgque.ticker();" >/dev/null
  sql_value "select pgque.claim_slot('${queue_name}', 'workers', 0, 'owner', interval '1 minute');" >/dev/null
}

assert_open_batch_visible() {
  local queue_name=$1
  local state
  state="$(sql_value "select case when s.sub_batch is not null then 'open' else 'closed' end
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = '${queue_name}' and c.co_name = 'workers#0/1'")"
  if [[ "${state}" != "open" ]]; then
    echo "FAIL: committed open-batch barrier was not visible for ${queue_name}: ${state:-missing}" >&2
    exit 1
  fi
  echo "barrier: ${queue_name} open batch is committed and visible"
}

expect_release_fenced() {
  local queue_name=$1
  local label=$2
  local out_file="${workdir}/${label}.out"
  local err_file="${workdir}/${label}.err"
  local status

  set +e
  "${psql_base[@]}" -qAtc \
    "select pgque.release_slot('${queue_name}', 'workers', 0, 'owner')" \
    >"${out_file}" 2>"${err_file}"
  status=$?
  set -e

  if ((status == 0)); then
    echo "FAIL: ${label}: release_slot allowed owner release with an open batch (returned $(tr -d '[:space:]' <"${out_file}"))" >&2
    failures=$((failures + 1))
  elif ! grep -q "cannot release slot 0 of consumer workers on queue ${queue_name} while batch .* is open; ack the batch first" "${err_file}"; then
    echo "FAIL: ${label}: release was rejected with the wrong contract" >&2
    sed -n '1,20p' "${err_file}" >&2
    failures=$((failures + 1))
  else
    echo "PASS: ${label}: open batch fenced release"
  fi

  # Restore ownership after a RED implementation released it, then prove a
  # separate successor backend cannot claim while the owner is live.
  sql_value "select pgque.claim_slot('${queue_name}', 'workers', 0, 'owner', interval '1 minute');" >/dev/null
  if [[ -n "$(sql_value "select pgque.claim_slot('${queue_name}', 'workers', 0, 'successor', interval '1 minute')")" ]]; then
    echo "FAIL: ${label}: successor claimed before the batch boundary" >&2
    failures=$((failures + 1))
  else
    echo "PASS: ${label}: successor backend remained fenced"
  fi
}

# Legacy batch: the receive commits in one backend. Release and successor
# claim run in other backends after the visible sub_batch barrier.
setup_queue "${legacy_queue}" 1
sql_value "select count(*) from pgque.receive_partitioned(
  '${legacy_queue}', 'workers', 0, 1, 'owner', 10)" >/dev/null
assert_open_batch_visible "${legacy_queue}"
expect_release_fenced "${legacy_queue}" "legacy_open_batch"

# Pending first page: pending_token is committed before release is attempted.
setup_queue "${open_page_queue}" 2
open_token="$(sql_value "select page_token from pgque.receive_page_partitioned(
  '${open_page_queue}', 'workers', 0, 1, 'owner', 1)")"
assert_open_batch_visible "${open_page_queue}"
pending_count="$(sql_value "select count(*) from pgque.page_state where pending_token = '${open_token}'::uuid")"
if [[ "${pending_count}" != "1" ]]; then
  echo "FAIL: pending-page barrier was not visible" >&2
  exit 1
fi
echo "barrier: ${open_page_queue} pending page is committed and visible"
expect_release_fenced "${open_page_queue}" "partition_page_open"

# Between pages: ack page one and verify that the batch remains open with no
# pending token. This is the paging-specific gap most likely to be missed by
# a check that only looks for a pending page.
setup_queue "${partial_page_queue}" 2
partial_token="$(sql_value "select page_token from pgque.receive_page_partitioned(
  '${partial_page_queue}', 'workers', 0, 1, 'owner', 1)")"
partial_finished="$(sql_value "select batch_finished from pgque.ack_page('${partial_token}', 'owner')")"
if [[ "${partial_finished}" != "f" ]]; then
  echo "FAIL: first page unexpectedly finished the batch" >&2
  exit 1
fi
assert_open_batch_visible "${partial_page_queue}"
pending_count="$(sql_value "select count(*) from pgque.page_state as ps
  join pgque.queue as q on q.queue_id = ps.queue_id
  where q.queue_name = '${partial_page_queue}' and ps.pending_token is not null")"
if [[ "${pending_count}" != "0" ]]; then
  echo "FAIL: partial-ack barrier still has a pending page" >&2
  exit 1
fi
echo "barrier: ${partial_page_queue} partial ack is committed between pages"
expect_release_fenced "${partial_page_queue}" "partition_page_partial_ack"

# Terminal ack is the legal batch boundary. Release must succeed, and a
# successor backend must then claim with a higher epoch.
setup_queue "${terminal_page_queue}" 1
terminal_epoch="$(sql_value "select epoch from pgque.partition_slot as ps
  join pgque.queue as q on q.queue_id = ps.queue_id
  where q.queue_name = '${terminal_page_queue}' and ps.co_name = 'workers' and ps.slot = 0")"
terminal_token="$(sql_value "select page_token from pgque.receive_page_partitioned(
  '${terminal_page_queue}', 'workers', 0, 1, 'owner', 1)")"
terminal_finished="$(sql_value "select batch_finished from pgque.ack_page('${terminal_token}', 'owner')")"
if [[ "${terminal_finished}" != "t" ]]; then
  echo "FAIL: terminal page did not finish the batch" >&2
  exit 1
fi
release_result="$(sql_value "select pgque.release_slot(
  '${terminal_page_queue}', 'workers', 0, 'owner')")"
successor_epoch="$(sql_value "select pgque.claim_slot(
  '${terminal_page_queue}', 'workers', 0, 'successor', interval '1 minute')")"
if [[ "${release_result}" != "t" || -z "${successor_epoch}" || "${successor_epoch}" -le "${terminal_epoch}" ]]; then
  echo "FAIL: terminal ack must permit release and successor claim (release=${release_result}, old_epoch=${terminal_epoch}, new_epoch=${successor_epoch:-null})" >&2
  failures=$((failures + 1))
else
  echo "PASS: terminal page ack permits release and successor claim (${terminal_epoch} -> ${successor_epoch})"
fi

if ((failures > 0)); then
  echo "FAIL: partition release fencing has ${failures} violation(s)" >&2
  exit 1
fi

echo "PASS: partition release is fenced for legacy and paged open batches and allowed after terminal ack"
