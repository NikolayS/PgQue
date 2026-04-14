\set ON_ERROR_STOP on

-- US-10: Idempotent install
-- As an operator, I want to re-run pgque.sql without losing
-- existing queues, consumers, or event data.
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Temp table for cross-block state
create temporary table if not exists _us10_state (
  key text primary key,
  val text
);

-- ==============================
-- Phase 1: Create pre-existing state
-- ==============================

-- Create queues and consumers
do $$ begin
  perform pgque.create_queue('us10_orders');
  perform pgque.create_queue('us10_logs');
  perform pgque.subscribe('us10_orders', 'fulfillment');
  perform pgque.subscribe('us10_orders', 'analytics');
  perform pgque.subscribe('us10_logs', 'archiver');
end $$;

-- Insert events
do $$ begin
  perform pgque.insert_event('us10_orders', 'order.created', '{"id":1}');
  perform pgque.insert_event('us10_orders', 'order.created', '{"id":2}');
  perform pgque.insert_event('us10_orders', 'order.created', '{"id":3}');
  perform pgque.insert_event('us10_logs', 'log.entry', '{"msg":"hello"}');
end $$;

-- Tick to create proper state
do $$ begin
  perform pgque.force_tick('us10_orders');
  perform pgque.force_tick('us10_logs');
  perform pgque.ticker();
end $$;

-- Partially consume: fulfillment processes orders
do $$
declare
  v_msg pgque.message;
  v_batch_id bigint;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('us10_orders', 'fulfillment', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;
  if v_batch_id is not null then
    perform pgque.ack(v_batch_id);
  end if;
  raise notice 'US-10 phase 1: fulfillment consumed % events', v_count;
end $$;

-- Record state before reinstall
do $$
declare
  v_queue_count int;
  v_sub_count int;
  v_last_tick bigint;
begin
  select count(*) into v_queue_count from pgque.queue;
  select count(*) into v_sub_count from pgque.subscription;

  select s.sub_last_tick into v_last_tick
  from pgque.subscription s
  join pgque.queue q on q.queue_id = s.sub_queue
  join pgque.consumer c on c.co_id = s.sub_consumer
  where q.queue_name = 'us10_orders'
    and c.co_name = 'analytics';

  delete from _us10_state;
  insert into _us10_state values ('queue_count', v_queue_count::text);
  insert into _us10_state values ('sub_count', v_sub_count::text);
  insert into _us10_state values ('analytics_last_tick', coalesce(v_last_tick, 0)::text);

  raise notice 'US-10 phase 1: queues=%, subs=%, analytics_last_tick=%',
    v_queue_count, v_sub_count, v_last_tick;
end $$;

-- ==============================
-- Phase 2: Re-run install (idempotent)
-- ==============================
\i sql/pgque.sql

-- ==============================
-- Phase 3: Verify state preserved
-- ==============================

-- Verify: no errors during install (if we got here, install succeeded)
do $$ begin
  raise notice 'PASS: US-10 pgque.sql re-run completed without errors';
end $$;

-- Verify: queues still exist with same count
do $$
declare
  v_queue_count int;
  v_expected int;
begin
  select count(*) into v_queue_count from pgque.queue;
  select val::int into v_expected from _us10_state where key = 'queue_count';

  assert v_queue_count = v_expected,
    'queue count should be preserved: expected ' || v_expected
    || ', got ' || v_queue_count;
  raise notice 'PASS: US-10 queue count preserved (% queues)', v_queue_count;
end $$;

-- Verify: subscriptions preserved
do $$
declare
  v_sub_count int;
  v_expected int;
begin
  select count(*) into v_sub_count from pgque.subscription;
  select val::int into v_expected from _us10_state where key = 'sub_count';

  assert v_sub_count = v_expected,
    'subscription count should be preserved: expected ' || v_expected
    || ', got ' || v_sub_count;
  raise notice 'PASS: US-10 subscription count preserved (% subs)', v_sub_count;
end $$;

-- Verify: consumer position preserved
 do $$
declare
  v_last_tick bigint;
  v_expected bigint;
begin
  select s.sub_last_tick into v_last_tick
  from pgque.subscription s
  join pgque.queue q on q.queue_id = s.sub_queue
  join pgque.consumer c on c.co_id = s.sub_consumer
  where q.queue_name = 'us10_orders'
    and c.co_name = 'analytics';

  select val::bigint into v_expected
  from _us10_state where key = 'analytics_last_tick';

  assert coalesce(v_last_tick, 0) = v_expected,
    'analytics last tick should stay ' || v_expected || ', got ' || coalesce(v_last_tick, 0);
  raise notice 'PASS: US-10 consumer position preserved (analytics last_tick=%)', v_last_tick;
end $$;

-- Verify: all operations still work after reinstall

-- send still works
do $$
declare
  v_id bigint;
begin
  v_id := pgque.send('us10_orders', 'order.created',
    '{"id":99,"after_reinstall":true}'::jsonb);
  assert v_id is not null, 'send should return event id after reinstall';
  raise notice 'PASS: US-10 send() works after reinstall';
end $$;

-- ticker still works
do $$ begin
  perform pgque.force_tick('us10_orders');
  perform pgque.ticker();
  raise notice 'PASS: US-10 ticker() works after reinstall';
end $$;

-- receive still works
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from pgque.receive('us10_orders', 'analytics', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count >= 1,
    'analytics should receive events after reinstall, got ' || v_count;
  perform pgque.ack(v_batch_id);
  raise notice 'PASS: US-10 receive() works after reinstall (got % events)', v_count;
end $$;

-- direct queue metadata still works
 do $$
declare
  v_found bool := false;
begin
  select true into v_found
  from pgque.queue
  where queue_name = 'us10_orders';

  assert coalesce(v_found, false), 'queue metadata should include us10_orders after reinstall';
  raise notice 'PASS: US-10 queue metadata accessible after reinstall';
end $$;

-- ==============================
-- Teardown
-- ==============================
drop table if exists _us10_state;

do $$ begin
  perform pgque.unsubscribe('us10_orders', 'fulfillment');
  perform pgque.unsubscribe('us10_orders', 'analytics');
  perform pgque.unsubscribe('us10_logs', 'archiver');
  perform pgque.drop_queue('us10_orders');
  perform pgque.drop_queue('us10_logs');
end $$;

\echo 'US-10: PASSED'
