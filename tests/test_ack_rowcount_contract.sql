-- test_ack_rowcount_contract.sql -- pin pgque.ack / finish_batch / nack rowcount semantics
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- ack(batch_id):          1 on success, 0 on stale/double/unknown id.
-- finish_batch(batch_id): same.
-- nack(...):              1 on success (retry or DLQ branch);
--                         raises 'batch not found' on unknown batch.

\set ON_ERROR_STOP on

do $$ begin
  perform pgque.create_queue('test_ack_rowcount');
  perform pgque.subscribe('test_ack_rowcount', 'rc1');
end $$;

do $$ begin
  perform pgque.send('test_ack_rowcount', 'rc.test', '{"n":1}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_ack_rowcount');
  perform pgque.ticker();
end $$;

-- Test 1: pgque.ack() returns 1 then 0 on double-ack.
do $$
declare
  v_msg     pgque.message;
  v_batch   bigint;
begin
  select * into v_msg from pgque.receive('test_ack_rowcount', 'rc1', 10) limit 1;
  v_batch := v_msg.batch_id;

  assert pgque.ack(v_batch) = 1, 'first ack must return 1';
  -- Stale ack contract: returns 0, no exception. Drivers detect this.
  assert pgque.ack(v_batch) = 0, 'second ack on finished batch must return 0';

  raise notice 'PASS: pgque.ack() returns 1 then 0 (double-ack detected)';
end $$;

-- Test 2: pgque.ack() on unknown batch_id returns 0 (no exception).
do $$ begin
  assert pgque.ack(9999999999::bigint) = 0, 'ack on unknown batch_id must return 0';
  raise notice 'PASS: pgque.ack(unknown) returns 0 without raising';
end $$;

-- Test 3: pgque.finish_batch() rowcount mirrors ack() (asserted independently
-- so a future inlining/refactor can't silently change either function).

do $$ begin
  perform pgque.send('test_ack_rowcount', 'rc.test2', '{"n":2}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_ack_rowcount');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_batch bigint;
begin
  select * into v_msg from pgque.receive('test_ack_rowcount', 'rc1', 10) limit 1;
  v_batch := v_msg.batch_id;

  assert pgque.finish_batch(v_batch) = 1, 'first finish_batch must return 1';
  assert pgque.finish_batch(v_batch) = 0, 'second finish_batch must return 0';
  assert pgque.finish_batch(9999999998::bigint) = 0, 'finish_batch(unknown) must return 0';

  raise notice 'PASS: pgque.finish_batch() rowcount mirrors ack()';
end $$;

-- Test 4: pgque.nack() returns 1 on the retry branch.
do $$ begin
  perform pgque.send('test_ack_rowcount', 'rc.retry', '{"n":3}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_ack_rowcount');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_ack_rowcount', 'rc1', 10) limit 1;
  -- Default max_retries=5; retry_count=0 → retry branch.
  assert pgque.nack(v_msg.batch_id, v_msg, '60 seconds'::interval, 'transient') = 1,
    'nack(retry branch) must return 1';
  perform pgque.ack(v_msg.batch_id);
  raise notice 'PASS: pgque.nack() returns 1 (retry branch)';
end $$;

-- Test 5: pgque.nack() returns 1 on the DLQ branch.
do $$ begin
  perform pgque.create_queue('test_ack_rowcount_dlq');
  perform pgque.set_queue_config('test_ack_rowcount_dlq', 'max_retries', '0');
  perform pgque.subscribe('test_ack_rowcount_dlq', 'rc1');
end $$;

do $$ begin
  perform pgque.send('test_ack_rowcount_dlq', 'rc.dead', '{"n":4}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_ack_rowcount_dlq');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg
    from pgque.receive('test_ack_rowcount_dlq', 'rc1', 10) limit 1;
  -- max_retries=0 → first nack takes the DLQ branch.
  assert pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'dead') = 1,
    'nack(DLQ branch) must return 1';
  perform pgque.ack(v_msg.batch_id);
  raise notice 'PASS: pgque.nack() returns 1 (DLQ branch)';
end $$;

-- Test 6: pgque.nack(unknown batch) raises 'batch not found' (vs. ack which
-- returns 0 — asymmetric because nack reaches into queue config first).
-- The forged composite must keep this column order in sync with pgque.message.
do $$
declare
  v_msg pgque.message := row(
    1::bigint, 9999999997::bigint,
    'forge', 'forge', 0, now(),
    null, null, null, null
  )::pgque.message;
  v_ok boolean := false;
begin
  begin
    perform pgque.nack(9999999997::bigint, v_msg, '0 seconds'::interval, 'forge');
  exception when raise_exception then
    assert sqlerrm like 'batch not found%', 'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'nack(unknown batch) did not raise';
  raise notice 'PASS: pgque.nack(unknown batch) raises batch-not-found';
end $$;

do $$ begin
  perform pgque.unsubscribe('test_ack_rowcount', 'rc1');
  perform pgque.drop_queue('test_ack_rowcount');
  perform pgque.unsubscribe('test_ack_rowcount_dlq', 'rc1');
  perform pgque.drop_queue('test_ack_rowcount_dlq');
end $$;

\echo 'PASS: test_ack_rowcount_contract'
