\set ON_ERROR_STOP on

-- Test: receive() must not strand consumer on empty active batch (#103)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Reproduces the bug: force an empty tick (no events in the window), then
-- call receive().  On the buggy version receive() opens a batch, finds 0
-- events, returns [], but leaves sub_batch set (the active batch is never
-- finished).  A second receive() call returns the *same* empty batch again
-- even after a new event arrives and a second tick fires.
--
-- After the fix: the first receive() must finish the empty batch internally
-- so the second receive() can see the new event.
--
-- See also: Issue #103 minimal repro in devel/sql/pgque-api/receive.sql header.

-- Setup
do $$
begin
  perform pgque.create_queue('test_empty_batch');
  perform pgque.register_consumer('test_empty_batch', 'c1');
end $$;

-- Fire a tick that captures zero events (empty tick window).
do $$
begin
  perform pgque.force_next_tick('test_empty_batch');
  perform pgque.ticker();
end $$;

-- receive() on an empty tick window: must return 0 rows AND not strand the
-- consumer (i.e. must finish the empty batch internally).
do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
  v_batch bigint;
  v_sub   record;
begin
  for v_msg in select * from pgque.receive('test_empty_batch', 'c1', 100)
  loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
  end loop;

  -- Correct: 0 rows returned for the empty window.
  assert v_count = 0, 'empty tick window should yield 0 messages, got ' || v_count;

  -- Key assertion: sub_batch must be NULL after receive() on an empty window.
  -- If the bug is present, sub_batch will still be set here, proving the
  -- consumer is stranded on the empty batch.
  select s.sub_batch into v_sub
  from pgque.subscription s
  join pgque.queue q on q.queue_id = s.sub_queue
  join pgque.consumer c on c.co_id = s.sub_consumer
  where q.queue_name = 'test_empty_batch'
    and c.co_name = 'c1';

  assert v_sub.sub_batch is null,
    'BUG #103: receive() left sub_batch set after returning 0 rows — '
    || 'consumer is stranded on empty batch id ' || coalesce(v_sub.sub_batch::text, 'NULL');

  raise notice 'PASS: receive() on empty tick window returns 0 rows and does not strand consumer';
end $$;

-- Now send a real event and tick it in.
do $$
begin
  perform pgque.send('test_empty_batch', 'hello', 'world');
end $$;

do $$
begin
  perform pgque.force_next_tick('test_empty_batch');
  perform pgque.ticker();
end $$;

-- Second receive() call must see the new event — not get stuck on the
-- previously empty batch.
do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive('test_empty_batch', 'c1', 100)
  loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
  end loop;

  assert v_count = 1,
    'second receive() should return the later event; got ' || v_count
    || ' rows (0 = still stuck on empty batch — Bug #103)';

  assert v_msg.type = 'hello', 'unexpected type: ' || coalesce(v_msg.type, 'NULL');

  perform pgque.ack(v_batch);
  raise notice 'PASS: second receive() sees new event after empty-batch window';
end $$;

-- Cleanup
do $$
begin
  perform pgque.unregister_consumer('test_empty_batch', 'c1');
  perform pgque.drop_queue('test_empty_batch');
  raise notice 'PASS: receive() empty-batch trap regression test (#103)';
end $$;
