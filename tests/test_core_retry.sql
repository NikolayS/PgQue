-- test_core_retry.sql -- Event retry mechanism
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- PgQ requires insert and ticker to be in separate transactions
-- (snapshot visibility). Each DO block here is a separate transaction.

-- Step 1: setup
do $$
begin
  perform pgque.create_queue('test_retry');
  perform pgque.register_consumer('test_retry', 'c1');
end $$;

-- Step 2: insert event
do $$
begin
  perform pgque.insert_event('test_retry', 'retry.test', 'data1');
end $$;

-- Step 3: ticker
do $$
begin
  perform pgque.ticker();
end $$;

-- Step 4: get batch and retry the event
do $$
declare
  v_batch_id bigint;
  v_ev record;
begin
  v_batch_id := pgque.next_batch('test_retry', 'c1');
  select * into v_ev from pgque.get_batch_events(v_batch_id) limit 1;

  -- Retry the event (0 seconds delay for test)
  perform pgque.event_retry(v_batch_id, v_ev.ev_id, 0);
  perform pgque.finish_batch(v_batch_id);
end $$;

-- Step 5: maintenance moves retried events back
do $$
begin
  perform pgque.maint_retry_events();
end $$;

-- Step 6: force a tick after the re-insert.
-- force_tick bumps the event sequence so the next ticker() call will
-- definitely create a tick (bypassing idle period optimization).
do $$
begin
  perform pgque.force_tick('test_retry');
  perform pgque.ticker();
end $$;

-- Step 7: verify the retried event appears
do $$
declare
  v_batch_id bigint;
  v_ev record;
begin
  v_batch_id := pgque.next_batch('test_retry', 'c1');
  assert v_batch_id is not null, 'should have batch with retried event';

  select * into v_ev from pgque.get_batch_events(v_batch_id) limit 1;
  assert v_ev.ev_retry is not null and v_ev.ev_retry >= 1,
    'retry count should be >= 1, got '
    || coalesce(v_ev.ev_retry::text, 'NULL');

  perform pgque.finish_batch(v_batch_id);
end $$;

-- Cleanup
do $$
begin
  perform pgque.unregister_consumer('test_retry', 'c1');
  perform pgque.drop_queue('test_retry');

  raise notice 'PASS: core_retry';
end $$;
