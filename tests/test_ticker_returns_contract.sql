-- test_ticker_returns_contract.sql
-- SQL-level contract tests for pgque.ticker() and pgque.force_tick() return
-- values. The TypeScript driver (and operator scripts) consume these
-- integers; a regression that returns void / NULL / wrong shape would be
-- silent on the SQL side but break tooling.
--
-- Contract pinned here:
--   pgque.ticker(queue text)       returns bigint -- new tick id, or NULL
--                                                    when no tick was needed
--                                                    (throttle in effect)
--   pgque.ticker()                 returns bigint -- count of queues that
--                                                    had a tick inserted
--                                                    on this call
--   pgque.force_tick(queue text)   returns bigint -- last tick id (every
--                                                    queue carries an
--                                                    initial tick from
--                                                    create_queue, so this
--                                                    is non-NULL whenever
--                                                    the queue exists);
--                                                    NULL if the queue
--                                                    name is unknown
--   pgque.ticker(queue) on an unknown queue  RAISES 'no such queue'
--   pgque.ticker(queue) on paused queue      RAISES 'Ticker has been paused'
--   pgque.ticker(queue) on external_ticker   RAISES 'external tick source'
--
-- Subtlety: force_tick on a paused / external queue does NOT bump
-- event_seq (the WHERE clause filters those out), but the second SELECT
-- still returns the existing last tick id. Pin both halves explicitly.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

-- =========================================================================
-- Test 1: ticker(queue) returns a bigint > 0 when a tick is created
-- =========================================================================
do $$ begin
  perform pgque.create_queue('test_ticker_ret');
  perform pgque.subscribe('test_ticker_ret', 'rt1');
end $$;

do $$ begin
  perform pgque.send('test_ticker_ret', 'rt.test', '{"n":1}'::jsonb);
end $$;

-- The first call after we have new events should insert a tick and return
-- a positive bigint (the new tick id). Use force_tick first to bump the
-- event_seq above ticker_max_count so the throttle never fires.
do $$
declare
  v_force bigint;
  v_tid   bigint;
begin
  v_force := pgque.force_tick('test_ticker_ret');
  -- force_tick inserts (or no-ops) and reports the last tick id; with our
  -- fresh queue + one event, the last tick id must be a positive bigint.
  assert v_force is not null,
    'force_tick on a fresh queue with events must return a tick id';
  assert v_force > 0,
    format('force_tick should return a positive bigint, got %s', v_force);

  -- ticker(queue) returns either a tick id or NULL (if throttled). After
  -- force_tick we just bumped event_seq aggressively, so the next ticker
  -- call should also tick. We can't pin "always non-null" without timing
  -- assumptions, so test the *type* (bigint) and that it's non-negative
  -- when set.
  v_tid := pgque.ticker('test_ticker_ret');
  if v_tid is not null then
    assert v_tid > 0,
      format('ticker(queue) tick id must be positive when present, got %s', v_tid);
  end if;

  raise notice 'PASS: ticker(queue) and force_tick(queue) return bigint';
end $$;

-- =========================================================================
-- Test 2: ticker() (zero-arg) returns the count of queues that ticked
-- =========================================================================
do $$
declare
  v_count bigint;
begin
  -- Add another queue with new events so we know at least one tick will fire.
  perform pgque.create_queue('test_ticker_ret_b');
  perform pgque.send('test_ticker_ret_b', 'rt.test', '{"n":1}'::jsonb);
  perform pgque.force_tick('test_ticker_ret_b');

  -- Insert a fresh event into the first queue and force-tick it so its
  -- event_seq is well ahead of the last tick_event_seq.
  perform pgque.send('test_ticker_ret', 'rt.again', '{"n":2}'::jsonb);
  perform pgque.force_tick('test_ticker_ret');

  v_count := pgque.ticker();
  -- Zero-arg ticker iterates all unpaused queues; the count is the number
  -- that actually inserted a tick. With force_tick run on both queues,
  -- the count must be a non-negative bigint.
  assert v_count is not null, 'ticker() must not return NULL';
  assert v_count >= 0,
    format('ticker() count must be >= 0, got %s', v_count);

  raise notice 'PASS: ticker() returns bigint count (got %)', v_count;
end $$;

-- =========================================================================
-- Test 3: force_tick(unknown queue) returns NULL (no exception)
-- (force_tick swallows the not-found case by design — see the inline
-- comment in the function source. Pin this contract so a refactor does
-- not silently change it to RAISE.)
-- =========================================================================
do $$
declare
  v_tid bigint;
begin
  v_tid := pgque.force_tick('test_ticker_does_not_exist_xyz');
  assert v_tid is null,
    format('force_tick(unknown queue) must return NULL, got %s', v_tid);
  raise notice 'PASS: force_tick(unknown) returns NULL without raising';
end $$;

-- =========================================================================
-- Test 4: ticker(unknown queue) raises 'no such queue'
-- =========================================================================
do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.ticker('test_ticker_does_not_exist_xyz');
    raise exception 'expected ticker(unknown) to raise';
  exception when raise_exception then
    assert sqlerrm like '%no such queue%',
      format('expected "no such queue" message, got: %s', sqlerrm);
    v_ok := true;
  end;
  assert v_ok, 'ticker(unknown) did not raise';
  raise notice 'PASS: ticker(unknown queue) raises no-such-queue';
end $$;

-- =========================================================================
-- Test 5: ticker(queue) on a paused queue raises 'Ticker has been paused'
-- =========================================================================
do $$ begin
  perform pgque.create_queue('test_ticker_paused');
  -- Pause via direct UPDATE: there is no public set_queue_ticker_paused()
  -- helper; the ticker pause flag is queue_ticker_paused on pgque.queue.
  update pgque.queue
     set queue_ticker_paused = true
   where queue_name = 'test_ticker_paused';
end $$;

do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.ticker('test_ticker_paused');
    raise exception 'expected ticker(paused) to raise';
  exception when raise_exception then
    assert sqlerrm like '%paused%',
      format('expected "paused" in message, got: %s', sqlerrm);
    v_ok := true;
  end;
  assert v_ok, 'ticker(paused) did not raise';
  raise notice 'PASS: ticker(paused queue) raises paused';
end $$;

-- force_tick on a paused queue does NOT bump event_seq (the WHERE clause
-- filters it out), but the second SELECT still returns the existing last
-- tick id. Every queue carries an initial tick from create_queue, so this
-- is non-NULL. Verify both halves: the event_seq does NOT advance, AND
-- the return is the existing tick id.
do $$
declare
  v_seq_before bigint;
  v_seq_after  bigint;
  v_tid        bigint;
  v_seq_name   text;
begin
  select queue_event_seq into v_seq_name
    from pgque.queue where queue_name = 'test_ticker_paused';
  v_seq_before := pgque.seq_getval(v_seq_name);

  v_tid := pgque.force_tick('test_ticker_paused');
  assert v_tid is not null,
    'force_tick on a paused queue must return the existing last tick id, '
    'not NULL (every queue carries an initial tick from create_queue)';
  assert v_tid > 0,
    format('force_tick(paused) returned non-positive %s', v_tid);

  v_seq_after := pgque.seq_getval(v_seq_name);
  assert v_seq_after = v_seq_before,
    format('force_tick(paused) must NOT bump event_seq; before=%s after=%s',
      v_seq_before, v_seq_after);

  raise notice 'PASS: force_tick(paused queue) returns last tick id and does not bump event_seq';
end $$;

-- =========================================================================
-- Test 6: ticker(queue) on an external_ticker queue raises
-- =========================================================================
do $$ begin
  perform pgque.create_queue('test_ticker_external');
  update pgque.queue
     set queue_external_ticker = true,
         queue_ticker_paused = false
   where queue_name = 'test_ticker_external';
end $$;

do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.ticker('test_ticker_external');
    raise exception 'expected ticker(external) to raise';
  exception when raise_exception then
    assert sqlerrm like '%external%',
      format('expected "external" in message, got: %s', sqlerrm);
    v_ok := true;
  end;
  assert v_ok, 'ticker(external) did not raise';
  raise notice 'PASS: ticker(external_ticker queue) raises external';
end $$;

-- =========================================================================
-- Test 7: zero-arg ticker() skips paused and external queues
-- (regression guard: a refactor that loops over all queues unconditionally
-- and then invokes ticker(text) per row would cause exceptions to escape
-- and abort the dispatch loop — silently dropping all subsequent queues.
-- The current implementation filters with WHERE not queue_external_ticker
-- and not queue_ticker_paused; verify the behavior holds.)
-- =========================================================================
do $$
declare
  v_count bigint;
begin
  -- Should NOT raise even though paused / external queues exist.
  v_count := pgque.ticker();
  assert v_count is not null, 'ticker() with paused/external queues must not return NULL';
  assert v_count >= 0,
    format('ticker() count must remain non-negative, got %s', v_count);
  raise notice 'PASS: ticker() skips paused/external queues without raising';
end $$;

-- =========================================================================
-- Cleanup
-- =========================================================================
do $$ begin
  perform pgque.unsubscribe('test_ticker_ret', 'rt1');
  perform pgque.drop_queue('test_ticker_ret');
  perform pgque.drop_queue('test_ticker_ret_b');
  -- Un-pause / un-flag so drop_queue runs cleanly under any defensive
  -- assertions added later.
  update pgque.queue
     set queue_ticker_paused = false, queue_external_ticker = false
   where queue_name in ('test_ticker_paused', 'test_ticker_external');
  perform pgque.drop_queue('test_ticker_paused');
  perform pgque.drop_queue('test_ticker_external');
end $$;

\echo 'PASS: test_ticker_returns_contract'
