-- test_dlq_edge_cases.sql
-- Edge-case coverage for the dead-letter API. Existing tests cover the
-- nack-routes-to-DLQ path and basic dlq_inspect / dlq_replay flow; this
-- file focuses on:
--
--   1. dlq_replay(dl_id) of an already-replayed entry RAISES
--      'dead letter entry not found' (idempotent semantics: callers can
--      detect double-replays).
--   2. dlq_replay_all returns a (replayed, failed, first_error) record
--      with correct counts on success, on partial failure, and on an
--      empty DLQ.
--   3. dlq_purge filters by age (older_than interval) and returns the
--      number of rows deleted.
--   4. dlq_inspect respects ordering (dl_time desc) and the limit.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

-- =========================================================================
-- Setup: a queue with max_retries=0 so any nack lands directly in DLQ.
-- =========================================================================
do $$ begin
  perform pgque.create_queue('test_dlq_edges');
  perform pgque.set_queue_config('test_dlq_edges', 'max_retries', '0');
  perform pgque.subscribe('test_dlq_edges', 'dle1');
end $$;

-- Send 3 events, tick, receive, nack each so we get 3 DLQ rows.
do $$ begin
  perform pgque.send('test_dlq_edges', 'dlq.a', '{"k":"a"}'::jsonb);
  perform pgque.send('test_dlq_edges', 'dlq.b', '{"k":"b"}'::jsonb);
  perform pgque.send('test_dlq_edges', 'dlq.c', '{"k":"c"}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_tick('test_dlq_edges');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg     pgque.message;
  v_batch   bigint;
begin
  -- All messages share one batch_id; nack each, ack the batch once.
  for v_msg in select * from pgque.receive('test_dlq_edges', 'dle1', 100)
  loop
    v_batch := v_msg.batch_id;
    perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'dead-' || v_msg.type);
  end loop;
  if v_batch is not null then
    perform pgque.ack(v_batch);
  end if;
end $$;

-- Sanity: 3 DLQ rows landed.
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

-- =========================================================================
-- Test 1: dlq_replay(dl_id) of the same id twice raises on the second call.
-- =========================================================================
do $$
declare
  v_dl_id   bigint;
  v_new_eid bigint;
  v_ok      boolean := false;
begin
  -- Pick the oldest DLQ entry (deterministic ordering by dl_id).
  select dl.dl_id into v_dl_id
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges'
    order by dl.dl_id
    limit 1;

  v_new_eid := pgque.dlq_replay(v_dl_id);
  assert v_new_eid is not null, 'first dlq_replay must return a new ev_id';

  -- The replayed event sits in the live queue now. Other tests in this
  -- file expect a clean queue, so drop_queue at the end takes care of it;
  -- we just need to avoid double-replaying same dl_id below.

  -- Second replay of same dl_id: row was deleted, must raise.
  begin
    perform pgque.dlq_replay(v_dl_id);
    raise exception 'expected second dlq_replay(%) to raise', v_dl_id;
  exception when raise_exception then
    assert sqlerrm like 'dead letter entry not found%',
      format('expected "dead letter entry not found", got: %s', sqlerrm);
    v_ok := true;
  end;
  assert v_ok, 'second dlq_replay did not raise';
  raise notice 'PASS: dlq_replay() of already-replayed dl_id raises';
end $$;

-- =========================================================================
-- Test 2: dlq_replay(unknown dl_id) raises 'dead letter entry not found'
-- =========================================================================
do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.dlq_replay(9999999996::bigint);
    raise exception 'expected dlq_replay(unknown) to raise';
  exception when raise_exception then
    assert sqlerrm like 'dead letter entry not found%',
      format('expected "dead letter entry not found", got: %s', sqlerrm);
    v_ok := true;
  end;
  assert v_ok, 'dlq_replay(unknown) did not raise';
  raise notice 'PASS: dlq_replay(unknown dl_id) raises';
end $$;

-- =========================================================================
-- Test 3: dlq_inspect respects limit and returns rows from the right queue.
-- (We can't reliably pin dl_time desc ordering here because all 3 rows
-- were nacked inside one transaction, so they share now() — the dl_time
-- desc order is then a tie. Pin the contract that holds in any case:
-- limit clamps; rows match the underlying table; queue scoping is
-- correct.)
-- =========================================================================
do $$
declare
  v_rows int;
  v_table_ids bigint[];
  v_inspect_ids bigint[];
begin
  -- We replayed one entry above, so 2 remain.
  select count(*) into v_rows
    from pgque.dlq_inspect('test_dlq_edges', 100);
  assert v_rows = 2, format('expected 2 DLQ rows visible to dlq_inspect, got %s', v_rows);

  -- Limit must clamp.
  select count(*) into v_rows
    from pgque.dlq_inspect('test_dlq_edges', 1);
  assert v_rows = 1, format('expected dlq_inspect(..., 1) to return 1 row, got %s', v_rows);

  -- The set of dl_ids returned by dlq_inspect must equal the set in the
  -- underlying table for this queue.
  select array_agg(dl.dl_id order by dl.dl_id) into v_table_ids
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';

  select array_agg(dl.dl_id order by dl.dl_id) into v_inspect_ids
    from pgque.dlq_inspect('test_dlq_edges', 100) dl;

  assert v_table_ids = v_inspect_ids,
    format('dlq_inspect rows mismatch table: table=%s inspect=%s',
      v_table_ids, v_inspect_ids);

  raise notice 'PASS: dlq_inspect rows match underlying table and respect limit';
end $$;

-- =========================================================================
-- Test 4: dlq_purge with cutoff in the future (large interval) returns 0.
-- (Entries are recent; older_than = 1 day means delete rows older than 1 day,
-- which is none.)
-- =========================================================================
do $$
declare
  v_cnt integer;
  v_remaining bigint;
begin
  v_cnt := pgque.dlq_purge('test_dlq_edges', '1 day'::interval);
  assert v_cnt = 0,
    format('dlq_purge(1 day) on fresh entries must delete 0, got %s', v_cnt);

  select count(*) into v_remaining
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';
  assert v_remaining = 2,
    format('dlq_purge(1 day) must not touch fresh rows; remaining=%s', v_remaining);

  raise notice 'PASS: dlq_purge with future cutoff is a no-op';
end $$;

-- =========================================================================
-- Test 5: dlq_purge with age 0 deletes everything and returns the count.
-- =========================================================================
do $$
declare
  v_cnt integer;
  v_remaining bigint;
begin
  v_cnt := pgque.dlq_purge('test_dlq_edges', '0 seconds'::interval);
  assert v_cnt = 2,
    format('dlq_purge(0s) must delete the 2 remaining rows, got %s', v_cnt);

  select count(*) into v_remaining
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';
  assert v_remaining = 0,
    format('after dlq_purge(0s), no DLQ rows for this queue, got %s', v_remaining);

  -- Re-running dlq_purge on an empty DLQ is a no-op (returns 0).
  v_cnt := pgque.dlq_purge('test_dlq_edges', '0 seconds'::interval);
  assert v_cnt = 0,
    format('dlq_purge on empty DLQ must return 0, got %s', v_cnt);

  raise notice 'PASS: dlq_purge deletes by age and returns row count';
end $$;

-- =========================================================================
-- Test 6: dlq_replay_all returns (replayed, failed, first_error) record.
-- =========================================================================

-- Drain the queue first: Test 1's dlq_replay re-inserted dlq.a as a live
-- event; consume + ack it cleanly so it doesn't pollute the next batch.
do $$ begin
  perform pgque.force_tick('test_dlq_edges');
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

-- Repopulate the DLQ with 2 rows.
do $$ begin
  perform pgque.send('test_dlq_edges', 'dlq.x', '{"k":"x"}'::jsonb);
  perform pgque.send('test_dlq_edges', 'dlq.y', '{"k":"y"}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_tick('test_dlq_edges');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive('test_dlq_edges', 'dle1', 100)
  loop
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
  assert v_replayed = 2,
    format('dlq_replay_all should replay 2 rows, got %s', v_replayed);
  assert v_failed = 0,
    format('dlq_replay_all should not fail any, got %s', v_failed);
  assert v_first_error is null,
    format('dlq_replay_all on success should leave first_error NULL, got %s', v_first_error);

  select count(*) into v_remaining
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'test_dlq_edges';
  assert v_remaining = 0,
    format('dlq_replay_all should drain the DLQ, %s rows left', v_remaining);

  raise notice 'PASS: dlq_replay_all returns (replayed=2, failed=0, first_error=NULL)';
end $$;

-- =========================================================================
-- Test 7: dlq_replay_all on an already-empty DLQ returns (0, 0, NULL).
-- =========================================================================
do $$
declare
  v_replayed    bigint;
  v_failed      bigint;
  v_first_error text;
begin
  select replayed, failed, first_error into v_replayed, v_failed, v_first_error
    from pgque.dlq_replay_all('test_dlq_edges');
  assert v_replayed = 0 and v_failed = 0 and v_first_error is null,
    format('dlq_replay_all on empty DLQ must be (0,0,NULL), got (%s,%s,%s)',
      v_replayed, v_failed, coalesce(v_first_error, '<null>'));
  raise notice 'PASS: dlq_replay_all on empty DLQ is a no-op';
end $$;

-- =========================================================================
-- Cleanup
-- =========================================================================
do $$ begin
  delete from pgque.dead_letter
   where dl_queue_id = (select queue_id from pgque.queue
                          where queue_name = 'test_dlq_edges');
  perform pgque.unsubscribe('test_dlq_edges', 'dle1');
  perform pgque.drop_queue('test_dlq_edges');
end $$;

\echo 'PASS: test_dlq_edge_cases'
