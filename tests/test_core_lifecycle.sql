-- test_core_lifecycle.sql -- Queue lifecycle: create, configure, drop
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_qinfo record;
begin
  -- Create a queue
  perform pg_current.create_queue('test_lifecycle');

  -- Verify it exists
  select * into v_qinfo
    from pg_current.get_queue_info('test_lifecycle');
  assert v_qinfo.queue_name = 'test_lifecycle', 'queue should exist';

  -- Configure it
  perform pg_current.set_queue_config(
    'test_lifecycle', 'ticker_max_count', '500');

  -- Drop it
  perform pg_current.drop_queue('test_lifecycle');

  -- Verify it's gone
  assert not exists (
    select 1 from pg_current.get_queue_info()
    where queue_name = 'test_lifecycle'
  ), 'queue should be gone after drop';

  raise notice 'PASS: core_lifecycle';
end $$;
