\set ON_ERROR_STOP on

-- Test DLQ (dead letter queue) functionality
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- PgQ requires insert, ticker, and receive to be in separate transactions
-- (snapshot visibility). Each DO block is a separate transaction.

-- ========================================
-- Test 1: nack with retry_count < max_retries -> retry
-- ========================================

-- Setup
do $$
begin
  perform pgque.create_queue('test_dlq');
  perform pgque.set_queue_config('test_dlq', 'max_retries', '2');
  perform pgque.register_consumer('test_dlq', 'c1');
end $$;

-- Insert event
do $$
begin
  perform pgque.insert_event('test_dlq', 'dlq.test', '{"x":1}');
end $$;

-- Ticker
do $$
begin
  perform pgque.ticker();
end $$;

-- Receive, nack (retry), ack
do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_dlq', 'c1', 1) limit 1;
  assert v_msg.msg_id is not null, 'should receive a message';

  -- Nack (retry_count is NULL/0, max_retries is 2, so should retry)
  perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'test failure');
  perform pgque.ack(v_msg.batch_id);

  -- Event should be in retry queue, not DLQ
  assert (select count(*) from pgque.dead_letter) = 0, 'should not be in DLQ yet';
end $$;

-- Cleanup test 1
do $$
begin
  perform pgque.unregister_consumer('test_dlq', 'c1');
  perform pgque.drop_queue('test_dlq');
  raise notice 'PASS: nack retries when under max_retries';
end $$;

-- ========================================
-- Test 2: nack with retry_count >= max_retries -> DLQ
-- ========================================

-- Setup
do $$
begin
  perform pgque.create_queue('test_dlq2');
  perform pgque.set_queue_config('test_dlq2', 'max_retries', '2');
  perform pgque.register_consumer('test_dlq2', 'c1');
end $$;

-- Insert event
do $$
begin
  perform pgque.insert_event('test_dlq2', 'dlq.test', '{"x":1}');
end $$;

-- Ticker
do $$
begin
  perform pgque.ticker();
end $$;

-- Receive, forge retry_count, nack -> DLQ
do $$
declare
  v_msg pgque.message;
  v_dlq_count bigint;
begin
  select * into v_msg from pgque.receive('test_dlq2', 'c1', 1) limit 1;

  -- Forge retry_count to simulate prior retries (unit test approach)
  v_msg.retry_count := 2;

  -- Nack should route to DLQ (retry_count=2 >= max_retries=2)
  perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'max retries');
  perform pgque.ack(v_msg.batch_id);

  -- Verify event is in DLQ
  select count(*) into v_dlq_count from pgque.dead_letter
  where dl_queue_id = (select queue_id from pgque.queue where queue_name = 'test_dlq2');
  assert v_dlq_count = 1, 'should have 1 DLQ entry, got ' || v_dlq_count;
end $$;

-- Cleanup test 2
-- (DLQ entries cascade-delete via the dl_queue_id / dl_consumer_id FKs when
-- the queue is dropped or the consumer is unregistered — no manual purge.)
do $$
begin
  perform pgque.unregister_consumer('test_dlq2', 'c1');
  perform pgque.drop_queue('test_dlq2');
  raise notice 'PASS: nack routes to DLQ after max retries';
end $$;

-- ========================================
-- Test 3: dlq_inspect, dlq_replay, dlq_purge
-- ========================================

-- Setup
do $$
begin
  perform pgque.create_queue('test_dlq3');
  perform pgque.set_queue_config('test_dlq3', 'max_retries', '0');
  perform pgque.register_consumer('test_dlq3', 'c1');
end $$;

-- Insert event
do $$
begin
  perform pgque.insert_event('test_dlq3', 'dlq.replay', '{"replay":true}');
end $$;

-- Ticker
do $$
begin
  perform pgque.ticker();
end $$;

-- Receive and nack -> DLQ (max_retries=0 so any nack goes to DLQ)
do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_dlq3', 'c1', 1) limit 1;
  perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'force dlq');
  perform pgque.ack(v_msg.batch_id);
end $$;

-- Verify dlq_inspect, dlq_replay, dlq_purge
do $$
declare
  v_dl_count bigint;
  v_new_eid bigint;
begin
  -- dlq_inspect
  select count(*) into v_dl_count from pgque.dlq_inspect('test_dlq3', 100);
  assert v_dl_count >= 1, 'dlq_inspect should show entries';

  -- dlq_replay
  select pgque.dlq_replay(dl_id) into v_new_eid
  from pgque.dead_letter
  where dl_queue_id = (select queue_id from pgque.queue where queue_name = 'test_dlq3')
  limit 1;
  assert v_new_eid is not null, 'dlq_replay should return new event id';

  raise notice 'PASS: dlq_inspect and dlq_replay';

  -- dlq_purge
  perform pgque.dlq_purge('test_dlq3', '0 seconds'::interval);

  raise notice 'PASS: dlq_purge';
end $$;

-- Cleanup test 3
do $$
begin
  perform pgque.unregister_consumer('test_dlq3', 'c1');
  perform pgque.drop_queue('test_dlq3');
end $$;
