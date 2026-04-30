-- test_core_batch_retry.sql -- Regression test for batch_retry() xid8 cast bug
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Regression: batch_retry() was inserting NULL::int8 into retry_queue.ev_txid
-- which is xid8. Fix: use NULL::xid8.
-- See: https://github.com/NikolayS/pgque/issues/107

-- Step 1: setup
do $$
begin
  perform pgque.create_queue('test_batch_retry');
  perform pgque.register_consumer('test_batch_retry', 'c1');
end $$;

-- Step 2: insert two events (separate transaction for snapshot visibility)
do $$
begin
  perform pgque.insert_event('test_batch_retry', 'batch.retry.test', 'evt1');
  perform pgque.insert_event('test_batch_retry', 'batch.retry.test', 'evt2');
end $$;

-- Step 3: tick so events are visible to consumers
do $$
begin
  perform pgque.force_tick('test_batch_retry');
  perform pgque.ticker();
end $$;

-- Step 4: batch_retry() must succeed without type mismatch error
-- (was: ERROR: column "ev_txid" is of type xid8 but expression is of type bigint)
do $$
declare
  v_batch_id bigint;
  v_retry_cnt integer;
begin
  v_batch_id := pgque.next_batch('test_batch_retry', 'c1');
  assert v_batch_id is not null, 'should get a batch';

  -- This call previously failed with:
  -- ERROR:  column "ev_txid" is of type xid8 but expression is of type bigint
  v_retry_cnt := pgque.batch_retry(v_batch_id, 0);
  assert v_retry_cnt = 2,
    'batch_retry should return 2, got ' || coalesce(v_retry_cnt::text, 'NULL');
  raise notice 'PASS: batch_retry() returned % rows (no xid8 type mismatch)', v_retry_cnt;
end $$;

-- Step 5: second call to batch_retry() on same batch must be idempotent (return 0)
do $$
declare
  v_batch_id bigint;
  v_retry_cnt integer;
begin
  -- Re-open same batch (still active): look up by queue name + consumer name
  select s.sub_batch into v_batch_id
    from pgque.subscription s
    join pgque.queue       q  on q.queue_id  = s.sub_queue
    join pgque.consumer    c  on c.co_id     = s.sub_consumer
   where q.queue_name = 'test_batch_retry'
     and c.co_name    = 'c1';
  assert v_batch_id is not null, 'batch should still be active';

  v_retry_cnt := pgque.batch_retry(v_batch_id, 0);
  assert v_retry_cnt = 0,
    'second batch_retry call should be idempotent (0 rows), got '
    || coalesce(v_retry_cnt::text, 'NULL');
  raise notice 'PASS: batch_retry() idempotent on second call';
end $$;

-- Step 5b: finish the original batch (batch_retry does not finish it)
do $$
declare
  v_batch_id bigint;
begin
  select s.sub_batch into v_batch_id
    from pgque.subscription s
    join pgque.queue       q  on q.queue_id  = s.sub_queue
    join pgque.consumer    c  on c.co_id     = s.sub_consumer
   where q.queue_name = 'test_batch_retry'
     and c.co_name    = 'c1';
  perform pgque.finish_batch(v_batch_id);
end $$;

-- Step 6: maintenance moves retried events back into the event table
do $$
begin
  perform pgque.maint_retry_events();
end $$;

-- Step 7: re-tick so reinserted events are visible
do $$
begin
  perform pgque.force_tick('test_batch_retry');
  perform pgque.ticker();
end $$;

-- Step 8: verify retry events reappear with incremented ev_retry
do $$
declare
  v_batch_id bigint;
  v_ev_count integer;
  v_min_retry integer;
begin
  v_batch_id := pgque.next_batch('test_batch_retry', 'c1');
  assert v_batch_id is not null, 'should get a batch with retried events';

  select count(*), min(ev_retry)
    into v_ev_count, v_min_retry
    from pgque.get_batch_events(v_batch_id);

  assert v_ev_count >= 1,
    'retried events should reappear, got ' || v_ev_count;
  assert v_min_retry >= 1,
    'ev_retry should be >= 1, got ' || coalesce(v_min_retry::text, 'NULL');
  raise notice 'PASS: retried events redelivered (count=%, min ev_retry=%)',
    v_ev_count, v_min_retry;

  perform pgque.finish_batch(v_batch_id);
end $$;

-- Cleanup
do $$
begin
  perform pgque.unregister_consumer('test_batch_retry', 'c1');
  perform pgque.drop_queue('test_batch_retry');
  raise notice 'PASS: core_batch_retry';
end $$;
