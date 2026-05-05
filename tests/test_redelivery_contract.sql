-- test_redelivery_contract.sql -- pin at-least-once redelivery semantics
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- The cross-driver "skip ack on nack-fail" mitigation only works if the SQL
-- side actually re-opens an unfinished batch on the next next_batch. Pin:
--   1. receive without ack → next receive yields the SAME batch_id / msg_id.
--   2. nack + ack → events sit in retry_queue, redelivered with retry_count=1
--      after maint_retry_events + tick.
--   3. clean ack → next receive yields nothing.
--   4. mixed ok / nack / ok batch → only the nacked row redelivers.

\set ON_ERROR_STOP on

create temporary table if not exists _rd_state (
    label text primary key, batch_id bigint, msg_id bigint
);

do $$ begin
  perform pgque.create_queue('test_rd_no_ack');
  perform pgque.subscribe('test_rd_no_ack', 'rd1');
end $$;

do $$ begin
  perform pgque.send('test_rd_no_ack', 'rd.msg', '{"n":1}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

-- Scenario 1: receive without ack must redeliver the same batch.
do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_rd_no_ack', 'rd1', 10) limit 1;
  delete from _rd_state where label = 'first';
  insert into _rd_state values ('first', v_msg.batch_id, v_msg.msg_id);
end $$;

do $$
declare
  v_msg          pgque.message;
  v_first_batch  bigint;
  v_first_msg    bigint;
  v_count        int := 0;
begin
  select batch_id, msg_id into v_first_batch, v_first_msg
    from _rd_state where label = 'first';

  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10)
  loop
    v_count := v_count + 1;
    assert v_msg.batch_id = v_first_batch, 'second receive must reopen the same batch_id';
    assert v_msg.msg_id = v_first_msg,    'second receive must yield the same msg_id';
  end loop;

  assert v_count = 1, 'second receive must redeliver exactly 1 message';
  perform pgque.ack(v_first_batch);
  raise notice 'PASS: receive() without ack redelivers the same batch on next receive';
end $$;

-- Scenario 2: nack + ack routes through retry_queue, reappears with retry_count=1.
do $$ begin
  perform pgque.send('test_rd_no_ack', 'rd.msg', '{"n":2}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_rd_no_ack', 'rd1', 10) limit 1;
  -- 0-second delay so maint_retry_events picks it up immediately.
  perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'transient');
  perform pgque.ack(v_msg.batch_id);
end $$;

-- Right after ack, the nacked message sits in retry_queue, not the live
-- event table — receive must return nothing.
do $$
declare
  v_count int := 0;
  v_msg   pgque.message;
begin
  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10) loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'retry events must not be delivered before maint_retry_events';
end $$;

-- maint_retry_events, force_next_tick, and ticker must each commit in their own
-- xact: the re-inserted event row has to commit before the next tick's
-- snapshot is taken, or it will be filtered out as in-flight.
do $$ begin perform pgque.maint_retry_events(); end $$;

do $$ begin
  perform pgque.force_next_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10) loop
    v_count := v_count + 1;
    assert v_msg.retry_count = 1, 'retried message must have retry_count=1';
    perform pgque.ack(v_msg.batch_id);
  end loop;
  assert v_count = 1, 'after maint_retry_events + tick, retried message must be delivered';
  raise notice 'PASS: nack() routes through retry_queue and reappears with retry_count=1';
end $$;

-- Scenario 3: clean ack does not redeliver.
do $$ begin
  perform pgque.send('test_rd_no_ack', 'rd.msg', '{"n":3}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_rd_no_ack', 'rd1', 10) limit 1;
  perform pgque.ack(v_msg.batch_id);
end $$;

do $$
declare
  v_count int := 0;
  v_msg   pgque.message;
begin
  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10) loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'clean-acked message must not redeliver';
  raise notice 'PASS: clean ack does not redeliver';
end $$;

-- Scenario 4: mixed batch behavior used by all three high-level clients.
-- If a consumer successfully processes ok / boom / ok, nacks only boom,
-- and then acks the underlying batch, only boom must reappear after retry
-- maintenance. The ok rows are finished by the batch ack.
do $$ begin
  perform pgque.send('test_rd_no_ack', 'rd.ok',   '{"n":4}'::jsonb);
  perform pgque.send('test_rd_no_ack', 'rd.boom', '{"n":5}'::jsonb);
  perform pgque.send('test_rd_no_ack', 'rd.ok',   '{"n":6}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg       pgque.message;
  v_batch     bigint;
  v_seen_ok   int := 0;
  v_seen_boom int := 0;
begin
  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10)
  loop
    v_batch := v_msg.batch_id;
    if v_msg.type = 'rd.boom' then
      v_seen_boom := v_seen_boom + 1;
      perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'handler error: boom');
    elsif v_msg.type = 'rd.ok' then
      v_seen_ok := v_seen_ok + 1;
    end if;
  end loop;

  assert v_seen_ok = 2,   format('expected 2 ok rows in mixed batch, got %s', v_seen_ok);
  assert v_seen_boom = 1, format('expected 1 boom row in mixed batch, got %s', v_seen_boom);
  perform pgque.ack(v_batch);
end $$;

-- The batch cursor advanced; before retry maintenance nothing from the
-- original batch should be visible.
do $$
declare
  v_count int := 0;
  v_msg   pgque.message;
begin
  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10) loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'after partial nack + batch ack, original ok rows must be finished';
end $$;

do $$ begin perform pgque.maint_retry_events(); end $$;

do $$ begin
  perform pgque.force_next_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10) loop
    v_count := v_count + 1;
    assert v_msg.type = 'rd.boom', format('only rd.boom may redeliver, got %s', v_msg.type);
    assert v_msg.retry_count = 1, 'mixed-batch redelivery must have retry_count=1';
    perform pgque.ack(v_msg.batch_id);
  end loop;
  assert v_count = 1, format('expected exactly 1 redelivered boom row, got %s', v_count);
  raise notice 'PASS: mixed ok/nack/ok batch redelivers only the nacked row';
end $$;

drop table if exists _rd_state;

do $$ begin
  perform pgque.unsubscribe('test_rd_no_ack', 'rd1');
  perform pgque.drop_queue('test_rd_no_ack');
end $$;

\echo 'PASS: test_redelivery_contract'
