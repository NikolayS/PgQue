-- test_ack_rowcount_contract.sql
-- SQL-level contract tests for pgque.ack() / pgque.finish_batch() / pgque.nack()
-- rowcount semantics. Cross-driver clients (Python, Go, TypeScript) surface
-- the integer return so callers can detect stale / double acks; this test
-- pins the SQL-side contract those drivers depend on.
--
-- Contract:
--   pgque.ack(batch_id)          returns 1 on success (batch finished)
--   pgque.ack(batch_id)          returns 0 on stale / double / unknown id
--   pgque.finish_batch(batch_id) returns 1 / 0 with the same semantics
--   pgque.nack(...)              returns 1 on success (retry or DLQ branch)
--   pgque.nack(...)              raises 'batch not found' on unknown batch id
--   pgque.nack(...)              raises 'msg_id % not found' on forged msg
--
-- A regression that turns ack() into raise-on-not-found, or that returns 1
-- for a stale batch, would silently break the driver contract: stale acks
-- would either crash apps or be invisible. This test catches both.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

-- =========================================================================
-- Setup
-- =========================================================================
do $$ begin
  perform pgque.create_queue('test_ack_rowcount');
  perform pgque.subscribe('test_ack_rowcount', 'rc1');
end $$;

do $$ begin
  perform pgque.send('test_ack_rowcount', 'rc.test', '{"n":1}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_tick('test_ack_rowcount');
  perform pgque.ticker();
end $$;

-- =========================================================================
-- Test 1: pgque.ack() returns 1 on first call, 0 on second (double-ack)
-- =========================================================================
do $$
declare
  v_msg     pgque.message;
  v_batch   bigint;
  v_first   integer;
  v_second  integer;
begin
  select * into v_msg from pgque.receive('test_ack_rowcount', 'rc1', 10) limit 1;
  assert v_msg.msg_id is not null, 'should receive 1 message';
  v_batch := v_msg.batch_id;

  v_first := pgque.ack(v_batch);
  assert v_first = 1,
    format('first ack on a held batch must return 1, got %s', v_first);

  -- Double ack: same batch_id, no longer active. Must return 0, not raise,
  -- not succeed silently. This is the contract Go/TS drivers depend on for
  -- "stale ack" detection.
  v_second := pgque.ack(v_batch);
  assert v_second = 0,
    format('second ack on already-finished batch must return 0, got %s', v_second);

  raise notice 'PASS: pgque.ack() returns 1 then 0 (double-ack detected)';
end $$;

-- =========================================================================
-- Test 2: pgque.ack() on unknown batch_id returns 0 (no exception)
-- =========================================================================
do $$
declare
  v_rc integer;
begin
  -- Pick a batch_id we know does not exist.
  v_rc := pgque.ack(9999999999::bigint);
  assert v_rc = 0,
    format('ack on unknown batch_id must return 0, got %s', v_rc);

  raise notice 'PASS: pgque.ack(unknown) returns 0 without raising';
end $$;

-- =========================================================================
-- Test 3: pgque.finish_batch() mirrors ack() rowcount semantics
-- (ack is a thin SECURITY DEFINER wrapper around finish_batch but the
-- contract is asserted independently so a future inlining/refactor doesn't
-- silently change behavior on either function.)
-- =========================================================================

-- Send + tick a fresh message for finish_batch test.
do $$ begin
  perform pgque.send('test_ack_rowcount', 'rc.test2', '{"n":2}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_tick('test_ack_rowcount');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg     pgque.message;
  v_batch   bigint;
  v_first   integer;
  v_second  integer;
  v_unknown integer;
begin
  select * into v_msg from pgque.receive('test_ack_rowcount', 'rc1', 10) limit 1;
  assert v_msg.msg_id is not null, 'should receive the second message';
  v_batch := v_msg.batch_id;

  v_first := pgque.finish_batch(v_batch);
  assert v_first = 1,
    format('finish_batch on a held batch must return 1, got %s', v_first);

  v_second := pgque.finish_batch(v_batch);
  assert v_second = 0,
    format('finish_batch on already-finished batch must return 0, got %s', v_second);

  v_unknown := pgque.finish_batch(9999999998::bigint);
  assert v_unknown = 0,
    format('finish_batch(unknown) must return 0, got %s', v_unknown);

  raise notice 'PASS: pgque.finish_batch() rowcount mirrors ack()';
end $$;

-- =========================================================================
-- Test 4: pgque.nack() returns 1 on success (retry branch)
-- =========================================================================
do $$ begin
  perform pgque.send('test_ack_rowcount', 'rc.retry', '{"n":3}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_tick('test_ack_rowcount');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
  v_rc  integer;
begin
  select * into v_msg from pgque.receive('test_ack_rowcount', 'rc1', 10) limit 1;
  assert v_msg.msg_id is not null, 'should receive the retry message';

  -- Default queue max_retries=5; retry_count=0; this should take the retry
  -- branch. nack() must return 1 in either branch (retry or DLQ).
  v_rc := pgque.nack(v_msg.batch_id, v_msg, '60 seconds'::interval, 'transient');
  assert v_rc = 1, format('nack(retry branch) must return 1, got %s', v_rc);

  -- Always close the batch so the next test starts clean.
  perform pgque.ack(v_msg.batch_id);
  raise notice 'PASS: pgque.nack() returns 1 (retry branch)';
end $$;

-- =========================================================================
-- Test 5: pgque.nack() returns 1 on success (DLQ branch)
-- =========================================================================
do $$ begin
  perform pgque.create_queue('test_ack_rowcount_dlq');
  perform pgque.set_queue_config('test_ack_rowcount_dlq', 'max_retries', '0');
  perform pgque.subscribe('test_ack_rowcount_dlq', 'rc1');
end $$;

do $$ begin
  perform pgque.send('test_ack_rowcount_dlq', 'rc.dead', '{"n":4}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_tick('test_ack_rowcount_dlq');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
  v_rc  integer;
begin
  select * into v_msg
  from pgque.receive('test_ack_rowcount_dlq', 'rc1', 10) limit 1;
  assert v_msg.msg_id is not null, 'should receive the DLQ-bound message';

  -- max_retries=0; first nack must take the DLQ branch and still return 1.
  v_rc := pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'dead');
  assert v_rc = 1, format('nack(DLQ branch) must return 1, got %s', v_rc);

  perform pgque.ack(v_msg.batch_id);
  raise notice 'PASS: pgque.nack() returns 1 (DLQ branch)';
end $$;

-- =========================================================================
-- Test 6: pgque.nack() on unknown batch_id raises 'batch not found'
-- (asymmetric with ack(): nack reaches into queue config first, which is
-- a hard lookup. We pin this so a future refactor doesn't silently relax
-- it to "return 0".)
-- =========================================================================
do $$
declare
  v_msg pgque.message;
  v_ok  boolean := false;
begin
  -- Build a syntactically-valid composite; msg_id and batch_id are both
  -- bogus so the queue-side subscription lookup must fail first.
  v_msg := row(
    1::bigint, 9999999997::bigint,
    'forge', 'forge', 0, now(),
    null, null, null, null
  )::pgque.message;

  begin
    perform pgque.nack(9999999997::bigint, v_msg, '0 seconds'::interval, 'forge');
    raise exception 'expected nack(unknown batch) to raise';
  exception when raise_exception then
    assert sqlerrm like 'batch not found%',
      format('expected "batch not found" prefix, got: %s', sqlerrm);
    v_ok := true;
  end;
  assert v_ok, 'nack(unknown batch) did not raise';
  raise notice 'PASS: pgque.nack(unknown batch) raises batch-not-found';
end $$;

-- =========================================================================
-- Cleanup
-- =========================================================================
do $$ begin
  perform pgque.unsubscribe('test_ack_rowcount', 'rc1');
  perform pgque.drop_queue('test_ack_rowcount');
  perform pgque.unsubscribe('test_ack_rowcount_dlq', 'rc1');
  perform pgque.drop_queue('test_ack_rowcount_dlq');
end $$;

\echo 'PASS: test_ack_rowcount_contract'
