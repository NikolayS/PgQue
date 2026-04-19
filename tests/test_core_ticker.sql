-- test_core_ticker.sql -- Ticker generates ticks
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_tick_count bigint;
begin
  perform pg_current.create_queue('test_ticker');

  -- Run ticker a few times
  perform pg_current.ticker();
  perform pg_current.ticker();
  perform pg_current.ticker();

  -- Verify ticks exist
  select count(*) into v_tick_count from pg_current.tick
  where tick_queue = (
    select queue_id from pg_current.queue
    where queue_name = 'test_ticker'
  );
  assert v_tick_count >= 1,
    'should have at least 1 tick, got ' || v_tick_count;

  -- Cleanup
  perform pg_current.drop_queue('test_ticker');

  raise notice 'PASS: core_ticker';
end $$;
