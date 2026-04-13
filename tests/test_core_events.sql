-- test_core_events.sql -- Event insertion and retrieval
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- PgQ requires insert and ticker to be in separate transactions
-- (snapshot visibility). Each DO block here is a separate transaction.

-- Step 1: setup
do $$
begin
  perform pgque.create_queue('test_events');
  perform pgque.register_consumer('test_events', 'c1');
end $$;

-- Step 2: insert event (separate transaction)
do $$
declare
  v_eid bigint;
begin
  v_eid := pgque.insert_event('test_events', 'test.type', '{"key":"value"}');
  assert v_eid is not null, 'insert_event should return event id';
end $$;

-- Step 3: ticker (separate transaction to capture the insert)
do $$
begin
  perform pgque.ticker();
end $$;

-- Step 4: verify batch events
do $$
declare
  v_batch_id bigint;
  v_ev record;
begin
  v_batch_id := pgque.next_batch('test_events', 'c1');
  assert v_batch_id is not null, 'should have a batch';

  select * into v_ev from pgque.get_batch_events(v_batch_id) limit 1;
  assert v_ev.ev_type = 'test.type', 'event type should match';
  assert v_ev.ev_data = '{"key":"value"}', 'event data should match';

  perform pgque.finish_batch(v_batch_id);
end $$;

-- Step 5: verify no more batches
do $$
declare
  v_batch_id bigint;
begin
  v_batch_id := pgque.next_batch('test_events', 'c1');
  assert v_batch_id is null, 'no more batches';
end $$;

-- Cleanup
do $$
begin
  perform pgque.unregister_consumer('test_events', 'c1');
  perform pgque.drop_queue('test_events');

  raise notice 'PASS: core_events';
end $$;
