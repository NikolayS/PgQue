-- test_core_consumer.sql -- Consumer registration and multiple consumers
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_count int;
begin
  perform pgque.create_queue('test_consumer');

  -- Register two consumers
  perform pgque.register_consumer('test_consumer', 'c1');
  perform pgque.register_consumer('test_consumer', 'c2');

  -- Verify both exist
  select count(*) into v_count
    from pgque.get_consumer_info('test_consumer');
  assert v_count = 2, 'should have 2 consumers, got ' || v_count;

  -- Unregister one
  perform pgque.unregister_consumer('test_consumer', 'c1');

  select count(*) into v_count
    from pgque.get_consumer_info('test_consumer');
  assert v_count = 1, 'should have 1 consumer after unregister';

  -- Cleanup
  perform pgque.unregister_consumer('test_consumer', 'c2');
  perform pgque.drop_queue('test_consumer');

  raise notice 'PASS: core_consumer';
end $$;
