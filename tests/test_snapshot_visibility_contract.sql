-- test_snapshot_visibility_contract.sql -- pin PgQ snapshot isolation contract
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- A regression that let in-progress xacts into a tick batch would silently
-- break at-least-once delivery: consumers could ack a message whose producer
-- later rolled back. Pin four invariants:
--   1. send + tick + receive in ONE xact yields 0 (producing xid is in xip).
--   2. After commit + new tick, the deferred message delivers exactly once.
--   3. A rolled-back send is invisible to consumers.
--   4. maint_retry_events + tick + receive has the same separate-xact rule.

\set ON_ERROR_STOP on

do $$ begin
  perform pgque.create_queue('test_snapshot_vis');
  perform pgque.subscribe('test_snapshot_vis', 'sv1');
end $$;

-- Test 1: same-xact send + tick + receive returns 0.
do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
begin
  perform pgque.send('test_snapshot_vis', 'sv.same_xact', '{"k":"sv"}'::jsonb);
  perform pgque.force_next_tick('test_snapshot_vis');
  perform pgque.ticker('test_snapshot_vis');

  for v_msg in select * from pgque.receive('test_snapshot_vis', 'sv1', 100) loop
    v_count := v_count + 1;
  end loop;

  assert v_count = 0, 'same-xact send + tick + receive must yield 0 messages';
  raise notice 'PASS: same-xact send + tick + receive returns 0 (snapshot contract)';
end $$;

-- Test 2: after commit + new tick, the deferred message delivers exactly once.
do $$ begin
  perform pgque.force_next_tick('test_snapshot_vis');
  perform pgque.ticker('test_snapshot_vis');
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_snapshot_vis', 'sv1', 100) loop
    v_count := v_count + 1;
    perform pgque.ack(v_msg.batch_id);
  end loop;

  assert v_count = 1, 'after commit + new tick, the deferred message must deliver exactly once';
  raise notice 'PASS: deferred message delivers in next xact (commit + tick)';
end $$;

-- Test 3: a rolled-back send is invisible to consumers. A PL/pgSQL
-- exception block is a subtransaction; raising inside and catching
-- outside rolls back only that subtransaction's work.
do $$ begin
  begin
    perform pgque.send('test_snapshot_vis', 'sv.rollback', '{"k":"rb"}'::jsonb);
    raise exception 'rollback this subxact';
  exception when others then null;
  end;
end $$;

do $$ begin
  perform pgque.force_next_tick('test_snapshot_vis');
  perform pgque.ticker('test_snapshot_vis');
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_snapshot_vis', 'sv1', 100) loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'rolled-back send must be invisible to consumers';
  raise notice 'PASS: rolled-back send is invisible to consumers';
end $$;

-- Test 4: maint_retry_events + tick + receive also needs committed xact
-- boundaries. The docs call this out as a contract: the retry row is
-- reinserted with the current xid, so a tick snapshot taken in the same xact
-- must not expose it.
do $$ begin
  perform pgque.send('test_snapshot_vis', 'sv.retry', '{"k":"retry"}'::jsonb);
end $$;

do $$ begin
  perform pgque.force_next_tick('test_snapshot_vis');
  perform pgque.ticker('test_snapshot_vis');
end $$;

do $$
declare
  v_msg pgque.message;
begin
  select * into v_msg from pgque.receive('test_snapshot_vis', 'sv1', 100) limit 1;
  assert v_msg.msg_id is not null, 'retry setup expected one visible message';
  perform pgque.nack(v_msg.batch_id, v_msg, '0 seconds'::interval, 'snapshot retry');
  perform pgque.ack(v_msg.batch_id);
end $$;

-- Anti-pattern from the docs: same transaction for retry maintenance, tick,
-- and receive. It must return zero rows.
do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
begin
  perform pgque.maint_retry_events();
  perform pgque.force_next_tick('test_snapshot_vis');
  perform pgque.ticker('test_snapshot_vis');

  for v_msg in select * from pgque.receive('test_snapshot_vis', 'sv1', 100) loop
    v_count := v_count + 1;
  end loop;

  assert v_count = 0, 'same-xact maint_retry_events + tick + receive must yield 0 messages';
  raise notice 'PASS: same-xact maint_retry_events + tick + receive returns 0';
end $$;

-- Correct pattern: after commit, tick again in a new xact, then receive.
do $$ begin
  perform pgque.force_next_tick('test_snapshot_vis');
  perform pgque.ticker('test_snapshot_vis');
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('test_snapshot_vis', 'sv1', 100) loop
    v_count := v_count + 1;
    assert v_msg.type = 'sv.retry', format('expected sv.retry redelivery, got %s', v_msg.type);
    perform pgque.ack(v_msg.batch_id);
  end loop;
  assert v_count = 1, 'after committed retry maintenance + new tick, retried row must deliver';
  raise notice 'PASS: committed maint_retry_events + tick + receive delivers retry row';
end $$;

do $$ begin
  perform pgque.unsubscribe('test_snapshot_vis', 'sv1');
  perform pgque.drop_queue('test_snapshot_vis');
end $$;

\echo 'PASS: test_snapshot_visibility_contract'
