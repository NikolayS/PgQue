-- test_redelivery_contract.sql
-- SQL-level guarantee that backs the cross-driver "skip ack on nack-fail"
-- behavior. The Python / Go / TypeScript Consumers were updated so that
-- if any required nack() fails, the batch ack() is SKIPPED and PgQ is
-- expected to redeliver the affected messages on the next receive.
--
-- That contract only holds if the SQL side actually re-opens an unfinished
-- batch on the next next_batch() call. This test pins that:
--
--   1. receive() then NO ack: next receive() returns the SAME batch_id
--      (same messages, same ev_ids).
--   2. receive() then nack() of every message then ack(): next receive
--      returns nothing immediately (events sit in retry_queue), and
--      reappear after maint_retry_events + tick.
--   3. receive() then ack() (clean path): next receive returns nothing.
--
-- This isolates the at-least-once redelivery semantics PgQ already
-- provides from the higher-level driver behavior built on top.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

-- =========================================================================
-- Scenario 1: receive() with NO ack must redeliver the same batch.
-- This is the foundation of the cross-driver nack-fail mitigation.
-- =========================================================================

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
  perform pgque.force_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

-- First receive: capture batch_id and msg_id, then DO NOT ack.
do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_rd_no_ack', 'rd1', 10) limit 1;
  assert v_msg.msg_id is not null, 'first receive must yield 1 message';

  delete from _rd_state where label = 'first';
  insert into _rd_state(label, batch_id, msg_id) values ('first', v_msg.batch_id, v_msg.msg_id);
end $$;

-- Second receive (no ack happened in between): MUST yield the same
-- batch_id and the same msg_id. PgQ's next_batch returns the carry-over
-- batch when sub_batch is still set.
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
    assert v_msg.batch_id = v_first_batch,
      format('second receive must reopen the same batch_id; first=%s second=%s',
        v_first_batch, v_msg.batch_id);
    assert v_msg.msg_id = v_first_msg,
      format('second receive must yield the same msg_id; first=%s second=%s',
        v_first_msg, v_msg.msg_id);
  end loop;

  assert v_count = 1,
    format('second receive must redeliver exactly 1 message, got %s', v_count);

  -- Now ack so we don't strand the consumer for the next scenario.
  perform pgque.ack(v_first_batch);
  raise notice 'PASS: receive() without ack redelivers the same batch on next receive';
end $$;

-- =========================================================================
-- Scenario 2: nack() then ack() routes the message into the retry path.
-- A subsequent receive returns nothing immediately; after
-- maint_retry_events + force_tick + ticker the message is delivered with
-- retry_count incremented. This is the at-least-once contract that the
-- high-level Consumer "ack on success / nack on failure" code relies on.
-- =========================================================================

do $$ begin
  perform pgque.send('test_rd_no_ack', 'rd.msg', '{"n":2}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_rd_no_ack', 'rd1', 10) limit 1;
  assert v_msg.msg_id is not null, 'should receive the new message';

  -- nack with 0 delay so maint_retry_events picks it up immediately.
  perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'transient');
  perform pgque.ack(v_msg.batch_id);

  delete from _rd_state where label = 'retry';
  insert into _rd_state(label, batch_id, msg_id) values ('retry', v_msg.batch_id, v_msg.msg_id);
end $$;

-- Right after ack: receive must not return the nacked message yet (it is
-- in retry_queue, not the live event table).
do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10) loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0,
    format('immediately after nack+ack, retry events must NOT be delivered, got %s', v_count);
end $$;

-- Run maintenance to move retry_queue rows back into the event table.
-- IMPORTANT: maint_retry_events, force_tick, and ticker each need their
-- own transaction. The new event row must commit before the next tick's
-- snapshot is taken; otherwise the snapshot includes the inserting xact
-- in xip and filters the row out of the next batch.
do $$ begin
  perform pgque.maint_retry_events();
end $$;

do $$ begin
  perform pgque.force_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

-- Now receive must yield the retried message with retry_count incremented.
do $$
declare
  v_msg          pgque.message;
  v_count        int := 0;
  v_seen_retry   boolean := false;
begin
  for v_msg in select * from pgque.receive('test_rd_no_ack', 'rd1', 10) loop
    v_count := v_count + 1;
    assert v_msg.retry_count = 1,
      format('retried message must have retry_count=1, got %s',
        coalesce(v_msg.retry_count::text, 'NULL'));
    v_seen_retry := true;
    perform pgque.ack(v_msg.batch_id);
  end loop;
  assert v_count = 1,
    format('after maint_retry_events + tick, retried message must be delivered, got %s', v_count);
  assert v_seen_retry, 'expected retried message';
  raise notice 'PASS: nack() routes through retry_queue and reappears with retry_count=1';
end $$;

-- =========================================================================
-- Scenario 3: clean ack() leaves no message redelivered.
-- (Sanity guard so a regression that always returns the previous batch
-- on next next_batch is caught from both sides.)
-- =========================================================================

do $$ begin
  perform pgque.send('test_rd_no_ack', 'rd.msg', '{"n":3}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_tick('test_rd_no_ack');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_rd_no_ack', 'rd1', 10) limit 1;
  assert v_msg.msg_id is not null, 'should receive cleanup message';
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
  assert v_count = 0,
    format('clean-acked message must not redeliver, got %s', v_count);
  raise notice 'PASS: clean ack does not redeliver';
end $$;

-- =========================================================================
-- Cleanup
-- =========================================================================
drop table if exists _rd_state;

do $$ begin
  perform pgque.unsubscribe('test_rd_no_ack', 'rd1');
  perform pgque.drop_queue('test_rd_no_ack');
end $$;

\echo 'PASS: test_redelivery_contract'
