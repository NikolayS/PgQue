\set ON_ERROR_STOP on

-- US-3: Retry and DLQ flow (integration test)
-- As a developer, I want failed messages to retry automatically and
-- land in a dead letter queue after max retries, so that transient
-- failures are handled without manual intervention.
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- CRITICAL: Each step (nack, ack, maint_retry_events, ticker, receive)
-- must be in a SEPARATE DO block for PgQ's snapshot isolation to work.
--
-- Flow:
--   1. Create queue with max_retries=2, send event, ticker, receive
--   2. Cycle 1: nack -> ack -> maint_retry_events -> force_tick+ticker -> receive (retry_count=1)
--   3. Cycle 2: nack -> ack -> maint_retry_events -> force_tick+ticker -> receive (retry_count=2, >= max_retries)
--   4. Cycle 3: nack -> event goes to DLQ -> ack
--   5. Verify: event in dead_letter, dlq_inspect shows it
--   6. dlq_replay -> ticker -> receive (event is back)

-- Temp table to pass batch_id between DO blocks
create temporary table if not exists _us3_state (batch_id bigint);

-- ==============================
-- Setup: queue with max_retries=2
-- ==============================
do $$ begin
  perform pgque.create_queue('us3_jobs', '{"max_retries": 2}'::jsonb);
  perform pgque.subscribe('us3_jobs', 'worker');
end $$;

-- Send event
do $$ begin
  perform pgque.send('us3_jobs', 'job.process', '{"task":"test"}'::jsonb);
end $$;

-- Ticker (force_tick bypasses throttle)
do $$ begin
  perform pgque.force_tick('us3_jobs');
  perform pgque.ticker();
end $$;

-- ==============================
-- Initial receive: retry_count should be NULL (coalesced to 0)
-- ==============================
do $$
declare
  v_msg pgque.message;
  v_batch_id bigint;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('us3_jobs', 'worker', 10)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    assert coalesce(v_msg.retry_count, 0) = 0,
      'initial retry_count should be 0 (or NULL), got ' || coalesce(v_msg.retry_count::text, 'NULL');
    assert v_msg.payload::jsonb = '{"task":"test"}'::jsonb,
      'payload should match, got ' || coalesce(v_msg.payload, 'NULL');

    -- Nack: schedule for retry (0 seconds delay for testing)
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'transient failure');
  end loop;

  assert v_count = 1, 'should receive exactly 1 message, got ' || v_count;

  -- Save batch_id for ack in next DO block
  delete from _us3_state;
  insert into _us3_state values (v_batch_id);

  raise notice 'PASS: US-3 initial receive (retry_count=0)';
end $$;

-- Ack the batch (separate transaction)
do $$
declare
  v_batch_id bigint;
begin
  select batch_id into v_batch_id from _us3_state;
  assert v_batch_id is not null, 'batch_id should not be null';
  perform pgque.ack(v_batch_id);
end $$;

-- ==============================
-- Cycle 1: move retry event back to queue
-- ==============================

-- maint_retry_events: move from retry_queue back to event table
do $$ begin
  perform pgque.maint_retry_events();
end $$;

-- force_tick + ticker: create a new tick covering the re-inserted event
do $$ begin
  perform pgque.force_tick('us3_jobs');
  perform pgque.ticker();
end $$;

-- Receive: retry_count should be 1
do $$
declare
  v_msg pgque.message;
  v_batch_id bigint;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('us3_jobs', 'worker', 10)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    assert v_msg.retry_count = 1,
      'cycle 1: retry_count should be 1, got ' || coalesce(v_msg.retry_count::text, 'NULL');

    -- Nack again
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'still failing');
  end loop;

  assert v_count = 1, 'cycle 1: should receive 1 message, got ' || v_count;

  -- Save batch_id for ack in next DO block
  delete from _us3_state;
  insert into _us3_state values (v_batch_id);

  raise notice 'PASS: US-3 cycle 1 (retry_count=1)';
end $$;

-- Ack batch after cycle 1
do $$
declare
  v_batch_id bigint;
begin
  select batch_id into v_batch_id from _us3_state;
  assert v_batch_id is not null, 'batch_id should not be null';
  perform pgque.ack(v_batch_id);
end $$;

-- ==============================
-- Cycle 2: retry again
-- ==============================

-- maint_retry_events
do $$ begin
  perform pgque.maint_retry_events();
end $$;

-- force_tick + ticker
do $$ begin
  perform pgque.force_tick('us3_jobs');
  perform pgque.ticker();
end $$;

-- Receive: retry_count should be 2 (now >= max_retries)
do $$
declare
  v_msg pgque.message;
  v_batch_id bigint;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('us3_jobs', 'worker', 10)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    assert v_msg.retry_count = 2,
      'cycle 2: retry_count should be 2, got ' || coalesce(v_msg.retry_count::text, 'NULL');

    -- Nack: retry_count=2 >= max_retries=2, so this should go to DLQ
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'permanent failure');
  end loop;

  assert v_count = 1, 'cycle 2: should receive 1 message, got ' || v_count;

  -- Save batch_id for ack in next DO block
  delete from _us3_state;
  insert into _us3_state values (v_batch_id);

  raise notice 'PASS: US-3 cycle 2 (retry_count=2, nack routes to DLQ)';
end $$;

-- Ack batch after cycle 2 (event is now in DLQ, not retry_queue)
do $$
declare
  v_batch_id bigint;
begin
  select batch_id into v_batch_id from _us3_state;
  assert v_batch_id is not null, 'batch_id should not be null';
  perform pgque.ack(v_batch_id);
end $$;

-- ==============================
-- Verify: event is in dead_letter
-- ==============================
do $$
declare
  v_dlq_count bigint;
  v_dl record;
begin
  select count(*) into v_dlq_count
  from pgque.dead_letter dl
  join pgque.queue q on q.queue_id = dl.dl_queue_id
  where q.queue_name = 'us3_jobs';

  assert v_dlq_count = 1, 'should have 1 DLQ entry, got ' || v_dlq_count;

  -- Verify dlq_inspect shows the event
  select * into v_dl from pgque.dlq_inspect('us3_jobs', 100) limit 1;
  assert v_dl.dl_id is not null, 'dlq_inspect should return an entry';
  assert v_dl.dl_reason = 'permanent failure',
    'DLQ reason should be permanent failure, got ' || coalesce(v_dl.dl_reason, 'NULL');
  assert v_dl.ev_type = 'job.process',
    'DLQ event type should be job.process, got ' || coalesce(v_dl.ev_type, 'NULL');
  assert v_dl.ev_data::jsonb = '{"task":"test"}'::jsonb,
    'DLQ event data should match, got ' || coalesce(v_dl.ev_data, 'NULL');

  raise notice 'PASS: US-3 event in DLQ with correct reason';
end $$;

-- ==============================
-- dlq_replay: re-insert event from DLQ back into queue
-- ==============================
do $$
declare
  v_dl_id bigint;
  v_new_eid bigint;
begin
  select dl_id into v_dl_id
  from pgque.dead_letter dl
  join pgque.queue q on q.queue_id = dl.dl_queue_id
  where q.queue_name = 'us3_jobs'
  limit 1;

  v_new_eid := pgque.dlq_replay(v_dl_id);
  assert v_new_eid is not null, 'dlq_replay should return new event id';

  -- Verify DLQ is now empty for this queue
  assert not exists (
    select 1 from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'us3_jobs'
  ), 'DLQ should be empty after replay';

  raise notice 'PASS: US-3 dlq_replay re-inserted event';
end $$;

-- Ticker after replay (force_tick bypasses throttle)
do $$ begin
  perform pgque.force_tick('us3_jobs');
  perform pgque.ticker();
end $$;

-- Receive the replayed event (retry_count should be reset/NULL)
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('us3_jobs', 'worker', 10)
  loop
    v_count := v_count + 1;
    assert v_msg.type = 'job.process',
      'replayed event type should be job.process, got ' || coalesce(v_msg.type, 'NULL');
    assert v_msg.payload::jsonb = '{"task":"test"}'::jsonb,
      'replayed event payload should match, got ' || coalesce(v_msg.payload, 'NULL');

    -- Ack the replayed event
    perform pgque.ack(v_msg.batch_id);
  end loop;

  assert v_count = 1, 'should receive 1 replayed event, got ' || v_count;
  raise notice 'PASS: US-3 replayed event received successfully';
end $$;

-- ==============================
-- Teardown
-- ==============================
drop table if exists _us3_state;

do $$ begin
  -- Clean up any remaining DLQ entries
  delete from pgque.dead_letter
  where dl_queue_id = (select queue_id from pgque.queue where queue_name = 'us3_jobs');
  perform pgque.unsubscribe('us3_jobs', 'worker');
  perform pgque.drop_queue('us3_jobs');
end $$;

\echo 'US-3: PASSED'
