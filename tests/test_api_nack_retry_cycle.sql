\set ON_ERROR_STOP on

-- Test full nack -> ack -> maint -> ticker -> receive retry cycle
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- This is an integration test for the modern API layer. Unlike unit tests
-- that forge retry_count, this follows the real retry path end-to-end.

-- Step 1: setup
do $$
begin
  perform pgque.create_queue('test_nack_cycle');
  perform pgque.set_queue_config('test_nack_cycle', 'max_retries', '3');
  perform pgque.register_consumer('test_nack_cycle', 'c1');
end $$;

-- Step 2: insert event
do $$
begin
  perform pgque.insert_event('test_nack_cycle', 'retry.test', '{"x":1}');
end $$;

-- Step 3: ticker captures insert
do $$
begin
  perform pgque.ticker();
end $$;

-- Step 4: receive, nack, ack
do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_nack_cycle', 'c1', 1) limit 1;
  assert v_msg.msg_id is not null, 'should receive original message';
  assert coalesce(v_msg.retry_count, 0) = 0,
    'initial retry_count should be 0/NULL';

  perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'retry me');
  perform pgque.ack(v_msg.batch_id);
end $$;

-- Step 5: maintenance moves retry event back to queue
do $$
begin
  perform pgque.maint();
end $$;

-- Step 6: force a tick so next_batch sees the retried event
do $$
begin
  perform pgque.force_tick('test_nack_cycle');
  perform pgque.ticker();
end $$;

-- Step 7: verify retried event is delivered with incremented retry_count
do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_nack_cycle', 'c1', 1) limit 1;
  assert v_msg.msg_id is not null, 'should receive retried message';
  assert v_msg.retry_count is not null and v_msg.retry_count >= 1,
    'retried message should have retry_count >= 1, got '
    || coalesce(v_msg.retry_count::text, 'NULL');

  perform pgque.ack(v_msg.batch_id);
end $$;

-- Cleanup
do $$
begin
  perform pgque.unregister_consumer('test_nack_cycle', 'c1');
  perform pgque.drop_queue('test_nack_cycle');
  raise notice 'PASS: modern nack retry cycle';
end $$;
