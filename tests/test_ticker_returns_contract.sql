-- test_ticker_returns_contract.sql -- pin pgque.ticker / force_next_tick return shapes
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- ticker(queue text)       -> bigint  -- new tick id, or NULL when throttled
-- ticker()                 -> bigint  -- count of queues that ticked
-- force_next_tick(queue)   -> bigint  -- last tick id; raises for unknown / paused / external queues
-- force_tick(queue)        -> compatibility alias, covered by test_force_next_tick_alias.sql
-- ticker(unknown)          RAISES 'no such queue'
-- ticker(paused)           RAISES 'Ticker has been paused'
-- ticker(external)         RAISES 'external tick source'

\set ON_ERROR_STOP on

-- Test 1: ticker(queue) returns bigint > 0 when a tick is created.
do $$ begin
  perform pgque.create_queue('test_ticker_ret');
  perform pgque.subscribe('test_ticker_ret', 'rt1');
end $$;

do $$ begin
  perform pgque.send('test_ticker_ret', 'rt.test', '{"n":1}'::jsonb);
end $$;

do $$
declare
  v_force bigint;
  v_tid   bigint;
begin
  v_force := pgque.force_next_tick('test_ticker_ret');
  assert v_force > 0, 'force_next_tick must return a positive tick id';

  -- ticker(queue) may return NULL when throttled; if non-null, must be > 0.
  v_tid := pgque.ticker('test_ticker_ret');
  assert v_tid is null or v_tid > 0, 'ticker(queue) tick id must be positive when present';

  raise notice 'PASS: ticker(queue) and force_next_tick(queue) return bigint';
end $$;

-- Test 2: zero-arg ticker() returns count of queues that ticked.
do $$
declare
  v_count bigint;
begin
  perform pgque.create_queue('test_ticker_ret_b');
  perform pgque.send('test_ticker_ret_b', 'rt.test', '{"n":1}'::jsonb);
  perform pgque.force_next_tick('test_ticker_ret_b');

  perform pgque.send('test_ticker_ret', 'rt.again', '{"n":2}'::jsonb);
  perform pgque.force_next_tick('test_ticker_ret');

  v_count := pgque.ticker();
  assert v_count >= 0, 'ticker() count must be a non-negative bigint';
  raise notice 'PASS: ticker() returns bigint count (got %)', v_count;
end $$;

-- Test 3: force_next_tick(unknown) raises. This is the hardened contract
-- from #195; silent NULL would hide queue-name bugs in manual schedulers.
do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.force_next_tick('test_ticker_does_not_exist_xyz');
  exception when others then
    assert sqlerrm like '%test_ticker_does_not_exist_xyz%' or sqlerrm like '%not found%' or sqlerrm like '%no such queue%',
      'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'force_next_tick(unknown) did not raise';
  raise notice 'PASS: force_next_tick(unknown queue) raises';
end $$;

-- Test 4: ticker(unknown) raises 'no such queue'.
do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.ticker('test_ticker_does_not_exist_xyz');
  exception when raise_exception then
    assert sqlerrm like '%no such queue%', 'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'ticker(unknown) did not raise';
  raise notice 'PASS: ticker(unknown queue) raises no-such-queue';
end $$;

-- Test 5: ticker(paused) and force_next_tick(paused) both raise.
do $$ begin
  perform pgque.create_queue('test_ticker_paused');
  perform pgque.set_queue_config('test_ticker_paused', 'ticker_paused', 'true');
end $$;

do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.ticker('test_ticker_paused');
  exception when raise_exception then
    assert sqlerrm like '%paused%', 'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'ticker(paused) did not raise';

  v_ok := false;
  begin
    perform pgque.force_next_tick('test_ticker_paused');
  exception when others then
    assert sqlerrm like '%paused%' or sqlerrm like '%test_ticker_paused%',
      'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'force_next_tick(paused) did not raise';
  raise notice 'PASS: paused queue rejects ticker and force_next_tick';
end $$;

-- Test 6: ticker(external) and force_next_tick(external) both raise.
do $$ begin
  perform pgque.create_queue('test_ticker_external');
  perform pgque.set_queue_config('test_ticker_external', 'external_ticker', 'true');
end $$;

do $$
declare
  v_ok boolean := false;
begin
  begin
    perform pgque.ticker('test_ticker_external');
  exception when raise_exception then
    assert sqlerrm like '%external%', 'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'ticker(external) did not raise';

  v_ok := false;
  begin
    perform pgque.force_next_tick('test_ticker_external');
  exception when others then
    assert sqlerrm like '%external%' or sqlerrm like '%test_ticker_external%',
      'unexpected message: ' || sqlerrm;
    v_ok := true;
  end;
  assert v_ok, 'force_next_tick(external) did not raise';
  raise notice 'PASS: external-ticker queue rejects ticker and force_next_tick';
end $$;

-- Test 7: zero-arg ticker() skips paused / external queues without raising.
-- A regression that lets exceptions escape the dispatcher would silently
-- drop every queue after the first paused/external one.
do $$
declare
  v_count bigint;
begin
  v_count := pgque.ticker();
  assert v_count >= 0, 'ticker() with paused/external queues must remain non-negative';
  raise notice 'PASS: ticker() skips paused/external queues without raising';
end $$;

do $$ begin
  perform pgque.unsubscribe('test_ticker_ret', 'rt1');
  perform pgque.drop_queue('test_ticker_ret');
  perform pgque.drop_queue('test_ticker_ret_b');
  perform pgque.drop_queue('test_ticker_paused');
  perform pgque.drop_queue('test_ticker_external');
end $$;

\echo 'PASS: test_ticker_returns_contract'
