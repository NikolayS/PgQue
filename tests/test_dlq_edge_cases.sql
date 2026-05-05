-- test_dlq_edge_cases.sql -- DLQ idempotency, ordering, purge, replay_all
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

-- max_retries=0 → any nack lands directly in DLQ.
do $$ begin
  perform pgque.create_queue('test_dlq_edges');
  perform pgque.set_queue_config('test_dlq_edges', 'max_retries', '0');
  perform pgque.subscribe('test_dlq_edges', 'dle1');
end $$;

do $$ begin
  perform pgque.send('test_dlq_edges', 'dlq.a', '{"k":"a"}'::jsonb);
  perform pgque.send('test_dlq_edges', 'dlq.b', '{"k":"b"}'::jsonb);
  perform pgque.send('test_dlq_edges', 'dlq.c', '{"k":"c"}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_dlq_edges');
  perform pgque.ticker();
end $$;

-- All three messages share one batch_id; nack each, ack the batch once.
do $$
declare
  v_msg   pgque.message;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive('test_dlq_edges', 'dle1', 100) loop
    v_batch := v_msg.batch_id;
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'dead-' || v_msg.type);
  end loop;
  if v_batch is not null then
    perform pgque.ack(v_batch);
  end if;
end $$;

do $$
declare
  v_count bigint;
begin
  select count(*) into v_count
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';
  assert v_count = 3, format('expected 3 DLQ rows, got %s', v_count);
end $$;

-- Test 1: dlq_replay(dl_id) of the same id twice raises on the second call.
do $$
declare
  v_dl_id bigint;
  v_ok    boolean := false;
begin
  select dl.dl_id into v_dl_id
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges'
    order by dl.dl_id
    limit 1;

  assert pgque.dlq_replay(v_dl_id) is not null, 'first dlq_replay must return a new ev_id';

  begin
    perform pgque.dlq_replay(v_dl_id);
  exception when raise_exception then
    assert sqlerrm like 'dead letter entry not found%', 'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'second dlq_replay did not raise';
  raise notice 'PASS: dlq_replay() of already-replayed dl_id raises';
end $$;

-- Test 2: dlq_replay(unknown dl_id) raises 'dead letter entry not found'.
do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.dlq_replay(9999999996::bigint);
  exception when raise_exception then
    assert sqlerrm like 'dead letter entry not found%', 'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'dlq_replay(unknown) did not raise';
  raise notice 'PASS: dlq_replay(unknown dl_id) raises';
end $$;

-- Test 3: dlq_inspect honors limit and matches the underlying table set.
-- (Can't pin dl_time desc ordering deterministically: rows nacked in one
-- xact share now(), so the dl_time tie-break order is unspecified.)
do $$
declare
  v_rows int;
  v_table_ids bigint[];
  v_inspect_ids bigint[];
begin
  -- One entry replayed above, two remain.
  select count(*) into v_rows from pgque.dlq_inspect('test_dlq_edges', 100);
  assert v_rows = 2, format('expected 2 DLQ rows, got %s', v_rows);

  select count(*) into v_rows from pgque.dlq_inspect('test_dlq_edges', 1);
  assert v_rows = 1, 'limit must clamp dlq_inspect';

  select array_agg(dl.dl_id order by dl.dl_id) into v_table_ids
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';

  select array_agg(dl.dl_id order by dl.dl_id) into v_inspect_ids
    from pgque.dlq_inspect('test_dlq_edges', 100) dl;

  assert v_table_ids = v_inspect_ids, 'dlq_inspect rows must match the underlying table';
  raise notice 'PASS: dlq_inspect rows match underlying table and respect limit';
end $$;

-- Test 4: dlq_purge with cutoff older than every row deletes nothing.
do $$
declare
  v_remaining bigint;
begin
  assert pgque.dlq_purge('test_dlq_edges', '1 day'::interval) = 0,
    'dlq_purge(1 day) on fresh entries must delete 0';

  select count(*) into v_remaining
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';
  assert v_remaining = 2, 'dlq_purge(1 day) must not touch fresh rows';
  raise notice 'PASS: dlq_purge with future cutoff is a no-op';
end $$;

-- Test 5: dlq_purge with age 0 deletes everything; second run is a no-op.
do $$
declare
  v_remaining bigint;
begin
  assert pgque.dlq_purge('test_dlq_edges', '0 seconds'::interval) = 2,
    'dlq_purge(0s) must delete the 2 remaining rows';

  select count(*) into v_remaining
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';
  assert v_remaining = 0, 'after dlq_purge(0s), DLQ must be empty for this queue';

  assert pgque.dlq_purge('test_dlq_edges', '0 seconds'::interval) = 0,
    'dlq_purge on empty DLQ must return 0';
  raise notice 'PASS: dlq_purge deletes by age and returns row count';
end $$;

-- Test 6: dlq_replay_all returns (replayed, failed, first_error).
-- Drain first: Test 1 re-inserted dlq.a as a live event.
do $$ begin
  perform pgque.force_next_tick('test_dlq_edges');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive('test_dlq_edges', 'dle1', 100) loop
    v_batch := v_msg.batch_id;
  end loop;
  if v_batch is not null then
    perform pgque.ack(v_batch);
  end if;
end $$;

do $$ begin
  perform pgque.send('test_dlq_edges', 'dlq.x', '{"k":"x"}'::jsonb);
  perform pgque.send('test_dlq_edges', 'dlq.y', '{"k":"y"}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_dlq_edges');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive('test_dlq_edges', 'dle1', 100) loop
    v_batch := v_msg.batch_id;
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'dead-again');
  end loop;
  if v_batch is not null then
    perform pgque.ack(v_batch);
  end if;
end $$;

do $$
declare
  v_replayed    bigint;
  v_failed      bigint;
  v_first_error text;
  v_remaining   bigint;
begin
  select replayed, failed, first_error into v_replayed, v_failed, v_first_error
    from pgque.dlq_replay_all('test_dlq_edges');
  assert v_replayed = 2,        format('expected replayed=2, got %s', v_replayed);
  assert v_failed = 0,          format('expected failed=0, got %s', v_failed);
  assert v_first_error is null, 'expected first_error=NULL on full success';

  select count(*) into v_remaining
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';
  assert v_remaining = 0, 'dlq_replay_all should drain the DLQ';
  raise notice 'PASS: dlq_replay_all returns (replayed=2, failed=0, first_error=NULL)';
end $$;

-- Test 7: dlq_replay_all on an empty DLQ is a no-op.
do $$
declare
  v_replayed    bigint;
  v_failed      bigint;
  v_first_error text;
begin
  select replayed, failed, first_error into v_replayed, v_failed, v_first_error
    from pgque.dlq_replay_all('test_dlq_edges');
  assert v_replayed = 0 and v_failed = 0 and v_first_error is null,
    'dlq_replay_all on empty DLQ must return (0, 0, NULL)';
  raise notice 'PASS: dlq_replay_all on empty DLQ is a no-op';
end $$;

do $$ begin
  perform pgque.unsubscribe('test_dlq_edges', 'dle1');
  perform pgque.drop_queue('test_dlq_edges');
end $$;

\echo 'PASS: test_dlq_edge_cases'
