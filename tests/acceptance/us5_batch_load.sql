\set ON_ERROR_STOP on

-- US-5: Batch processing under load
-- As a platform team, I want to process 10,000 events in one batch,
-- confirming pgque handles real workloads without dead-tuple bloat.
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Setup: create queue and subscribe consumer
do $$ begin
  perform pgque.create_queue('us5_ingest');
  perform pgque.subscribe('us5_ingest', 'etl');
end $$;

-- Action: insert 10,000 events in a single transaction
do $$
declare
  v_id bigint;
begin
  for i in 1..10000 loop
    v_id := pgque.insert_event('us5_ingest', 'data.load',
      '{"seq":' || i || '}');
  end loop;
  raise notice 'US-5: inserted 10,000 events';
end $$;

-- Tick (force_next_tick bypasses throttle)
do $$ begin
  perform pgque.force_next_tick('us5_ingest');
  perform pgque.ticker();
end $$;

-- Verify: receive returns all 10,000 events
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from pgque.receive('us5_ingest', 'etl', 20000)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 10000,
    'should receive 10,000 events, got ' || v_count;
  assert v_batch_id is not null, 'batch_id should be set';

  -- Ack the batch
  perform pgque.ack(v_batch_id);

  raise notice 'PASS: US-5 received and acked 10,000 events';
end $$;

-- Verify: consumer advanced after ack (without relying on experimental observability)
do $$
declare
  v_batch bigint;
begin
  select sub_batch into v_batch
  from pgque.subscription s
  join pgque.queue q on q.queue_id = s.sub_queue
  join pgque.consumer c on c.co_id = s.sub_consumer
  where q.queue_name = 'us5_ingest'
    and c.co_name = 'etl';

  assert v_batch is null, 'subscription batch should be cleared after ack';
  raise notice 'PASS: US-5 consumer batch cleared after ack';
end $$;

-- Verify: no dead tuples in pgque event tables
-- After ack, PgQ rotation should leave no dead tuples.
-- We trigger rotation and vacuum, then check pg_stat_user_tables.
do $$
declare
  v_dead bigint;
begin
  -- Force a rotation so old event table data can be reclaimed
  perform pgque.maint();

  -- Check dead tuples across all pgque event tables
  select coalesce(sum(n_dead_tup), 0) into v_dead
  from pg_stat_user_tables
  where schemaname = 'pgque'
    and relname like 'event_%';

  -- Dead tuples should be 0 (or very low -- stats may lag slightly)
  -- We allow a small margin because pg_stat counters are not instant
  assert v_dead <= 100,
    'dead tuples in event tables should be near 0, got ' || v_dead;

  raise notice 'PASS: US-5 dead tuples in event tables = %', v_dead;
end $$;

-- Teardown
do $$ begin
  perform pgque.unsubscribe('us5_ingest', 'etl');
  perform pgque.drop_queue('us5_ingest');
end $$;

\echo 'US-5: PASSED'
