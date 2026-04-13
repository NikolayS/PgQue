-- test_core_ticker.sql -- Ticker generates ticks
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_tick_count bigint;
begin
  perform pgque.create_queue('test_ticker');

  -- Run ticker a few times
  perform pgque.ticker();
  perform pgque.ticker();
  perform pgque.ticker();

  -- Verify ticks exist
  select count(*) into v_tick_count from pgque.tick
  where tick_queue = (
    select queue_id from pgque.queue
    where queue_name = 'test_ticker'
  );
  assert v_tick_count >= 1,
    'should have at least 1 tick, got ' || v_tick_count;

  -- Cleanup
  perform pgque.drop_queue('test_ticker');

  raise notice 'PASS: core_ticker';
end $$;
