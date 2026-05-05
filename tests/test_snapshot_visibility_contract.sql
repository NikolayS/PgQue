-- test_snapshot_visibility_contract.sql
-- Codify PgQ's snapshot-isolation contract at the SQL level so client
-- drivers and example code can rely on it explicitly:
--
--   In a single transaction, send → ticker → receive returns ZERO
--   messages. The producing transaction's xid is in the tick snapshot's
--   xip list, so its events are filtered out of the next batch until
--   it commits.
--
-- The cross-driver Python tests exercise this from the application side
-- (see clients/python/tests/test_transaction_visibility.py); pin it on
-- the SQL side too. A regression that allowed in-progress xacts into the
-- batch would silently break at-least-once delivery (consumers could ack
-- a message whose producer later rolled back).
--
-- The complementary test — separate transactions for produce / tick /
-- receive deliver normally — is covered throughout the regression and
-- acceptance suites.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

-- =========================================================================
-- Setup (separate transaction so create_queue + subscribe commit cleanly).
-- =========================================================================
do $$ begin
  perform pgque.create_queue('test_snapshot_vis');
  perform pgque.subscribe('test_snapshot_vis', 'sv1');
end $$;

-- =========================================================================
-- Test 1: send + force_tick + ticker + receive in ONE transaction yields
-- 0 messages. The whole DO block is a single PL/pgSQL transaction.
-- =========================================================================
do $$
declare
  v_msg     pgque.message;
  v_count   int := 0;
  v_eid     bigint;
  v_tid     bigint;
begin
  v_eid := pgque.send('test_snapshot_vis', 'sv.same_xact', '{"k":"sv"}'::jsonb);
  assert v_eid is not null, 'send must return a non-NULL ev_id';

  v_tid := pgque.force_tick('test_snapshot_vis');
  assert v_tid is not null, 'force_tick must return a non-NULL tick id';

  perform pgque.ticker('test_snapshot_vis');

  for v_msg in select * from pgque.receive('test_snapshot_vis', 'sv1', 100) loop
    v_count := v_count + 1;
  end loop;

  assert v_count = 0,
    format('PgQ snapshot contract: send + tick + receive in ONE xact must '
           'yield 0 messages (the producing xid is in the tick snapshot xip '
           'list); got %s', v_count);

  raise notice 'PASS: same-xact send + tick + receive returns 0 (snapshot contract)';
end $$;

-- =========================================================================
-- Test 2: after the previous DO block committed, a fresh receive() in a
-- NEW transaction must see the message that test 1 sent. The same-xact
-- invisibility was not loss — it was just deferred until commit.
-- =========================================================================
-- Re-tick from a separate xact so a new tick snapshot captures the
-- now-committed event.
do $$ begin
  perform pgque.force_tick('test_snapshot_vis');
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

  assert v_count = 1,
    format('after commit + new tick, the deferred message must deliver '
           'exactly once, got %s', v_count);
  raise notice 'PASS: deferred message delivers in next xact (commit + tick)';
end $$;

-- =========================================================================
-- Test 3: rollback semantics. Send inside a sub-block that intentionally
-- rolls back via raise+catch must NOT leave any visible message — the
-- canonical "consumer never sees uncommitted work" guarantee that
-- transactional outboxes rely on.
--
-- A PL/pgSQL EXCEPTION block creates a subtransaction; raising inside it
-- and catching outside rolls back only that subtransaction's work.
-- Verify that a send() rolled back this way produces 0 visible messages
-- after a fresh tick + receive.
-- =========================================================================
do $$
declare
  v_eid bigint;
begin
  begin
    v_eid := pgque.send('test_snapshot_vis', 'sv.rollback', '{"k":"rb"}'::jsonb);
    assert v_eid is not null;
    raise exception 'rollback this subxact';
  exception when others then
    -- Swallow: the inner block's send() is rolled back.
    null;
  end;
end $$;

do $$ begin
  perform pgque.force_tick('test_snapshot_vis');
  perform pgque.ticker('test_snapshot_vis');
end $$;

do $$
declare
  v_msg   pgque.message;
  v_count int := 0;
  v_batch bigint;
begin
  for v_msg in select * from pgque.receive('test_snapshot_vis', 'sv1', 100) loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
  end loop;

  -- The rolled-back send must not produce any deliverable message.
  assert v_count = 0,
    format('rolled-back send must produce 0 deliverable messages, got %s', v_count);

  -- empty receive() must finish the empty batch internally (#103); no
  -- v_batch to ack here.
  raise notice 'PASS: rolled-back send is invisible to consumers';
end $$;

-- =========================================================================
-- Cleanup
-- =========================================================================
do $$ begin
  perform pgque.unsubscribe('test_snapshot_vis', 'sv1');
  perform pgque.drop_queue('test_snapshot_vis');
end $$;

\echo 'PASS: test_snapshot_visibility_contract'
