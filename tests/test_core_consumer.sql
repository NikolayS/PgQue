-- test_core_consumer.sql -- Consumer registration and multiple consumers
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_count int;
begin
  perform pg_current.create_queue('test_consumer');

  -- Register two consumers
  perform pg_current.register_consumer('test_consumer', 'c1');
  perform pg_current.register_consumer('test_consumer', 'c2');

  -- Verify both exist
  select count(*) into v_count
    from pg_current.get_consumer_info('test_consumer');
  assert v_count = 2, 'should have 2 consumers, got ' || v_count;

  -- Unregister one
  perform pg_current.unregister_consumer('test_consumer', 'c1');

  select count(*) into v_count
    from pg_current.get_consumer_info('test_consumer');
  assert v_count = 1, 'should have 1 consumer after unregister';

  -- Cleanup
  perform pg_current.unregister_consumer('test_consumer', 'c2');
  perform pg_current.drop_queue('test_consumer');

  raise notice 'PASS: core_consumer';
end $$;
