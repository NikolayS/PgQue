\set ON_ERROR_STOP on

-- Regression test for #134: pgque.receive(..., max_return) followed by
-- pgque.ack(batch_id) must NOT silently drop the events that the underlying
-- PgQ batch contained but receive() did not yield.
--
-- Root cause: pgque.ack() called pgque.finish_batch() unconditionally, which
-- advances sub_last_tick past the entire tick window. Any events in that
-- window that the caller never saw became unreachable.
--
-- Fix contract: pgque.receive() records which msg_ids it actually returned,
-- and pgque.ack() re-queues the unreturned events to pgque.retry_queue with
-- ev_retry preserved (these events were never delivered to a handler) so
-- they are eligible for the next receive() call after maint_retry_events().

-- Step 1: setup
do $$
begin
  perform pgque.create_queue('t134_partial');
  perform pgque.register_consumer('t134_partial', 'c');
end $$;

-- Step 2: insert 105 events and tick (separate transactions)
do $$
begin
  perform pgque.send('t134_partial', 'tt', jsonb_build_object('i', g))
  from generate_series(1, 105) g;
end $$;

do $$
begin
  perform pgque.force_tick('t134_partial');
  perform pgque.ticker('t134_partial');
end $$;

-- Step 3: receive 100 of the 105 and ack the batch.
-- Before the fix, this silently dropped the remaining 5.
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
  v_batch_id bigint;
  v_max_id bigint := 0;
begin
  for v_msg in select * from pgque.receive('t134_partial', 'c', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    if v_msg.msg_id > v_max_id then
      v_max_id := v_msg.msg_id;
    end if;
  end loop;

  assert v_count = 100,
    format('first receive should return 100 rows, got %s', v_count);
  assert v_batch_id is not null, 'batch_id should be set';

  perform pgque.ack(v_batch_id);
end $$;

-- Step 4: pump the retry path so re-queued rows get re-inserted into the
-- main event table, then create a tick that covers them. Each DO block is
-- a separate transaction — required by PgQ snapshot-visibility semantics
-- (events inserted by maint_retry_events must commit before the ticker
-- snapshot is taken, otherwise the next batch can't see them).
do $$ begin
  perform pgque.maint_retry_events();
end $$;

do $$ begin
  perform pgque.force_tick('t134_partial');
  perform pgque.ticker('t134_partial');
end $$;

-- Step 5: the 5 unreturned events MUST be visible to the next receive() call.
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
  v_batch_id bigint;
  v_seen_ids bigint[] := '{}'::bigint[];
begin
  for v_msg in select * from pgque.receive('t134_partial', 'c', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
    v_seen_ids := v_seen_ids || v_msg.msg_id;
  end loop;

  assert v_count = 5,
    format('after ack, the 5 unreturned events MUST be re-delivered, got %s (issue #134)', v_count);

  -- The unreturned events were msg_ids 101..105 (last 5 of the 105 sent).
  assert v_seen_ids @> array[101::bigint, 102, 103, 104, 105],
    format('expected msg_ids 101..105 to be re-delivered, got %s', v_seen_ids::text);

  perform pgque.ack(v_batch_id);
end $$;

-- Step 6: confirm the second ack closes the queue cleanly (no leftovers).
do $$ begin
  perform pgque.maint_retry_events();
end $$;

do $$ begin
  perform pgque.force_tick('t134_partial');
  perform pgque.ticker('t134_partial');
end $$;

do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('t134_partial', 'c', 100)
  loop
    v_count := v_count + 1;
    perform pgque.ack(v_msg.batch_id);
    exit;
  end loop;

  assert v_count = 0,
    format('after both batches acked, queue should be drained, got %s leftover', v_count);
end $$;

-- Cleanup
do $$
begin
  perform pgque.unregister_consumer('t134_partial', 'c');
  perform pgque.drop_queue('t134_partial');
  raise notice 'PASS: receive/ack does not skip unreturned batch rows (#134)';
end $$;
