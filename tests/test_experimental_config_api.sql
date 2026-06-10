\set ON_ERROR_STOP on

-- Test experimental config sugar API

do $$
declare
  v_max_retries int;
  v_paused bool;
begin
  perform pgque.create_queue('test_opts', '{"max_retries": 10}'::jsonb);

  select queue_max_retries into v_max_retries
  from pgque.queue where queue_name = 'test_opts';

  assert v_max_retries = 10, 'max_retries should be 10, got ' || coalesce(v_max_retries::text, 'NULL');

  perform pgque.pause_queue('test_opts');
  select queue_ticker_paused into v_paused from pgque.queue where queue_name = 'test_opts';
  assert v_paused = true, 'queue should be paused';

  perform pgque.resume_queue('test_opts');
  select queue_ticker_paused into v_paused from pgque.queue where queue_name = 'test_opts';
  assert v_paused = false, 'queue should be resumed';

  perform pgque.drop_queue('test_opts');
  raise notice 'PASS: experimental config API';
end $$;

-- Negative max_retries must be rejected with the same error as the
-- canonical pgque.set_queue_config() path.
do $$
begin
  begin
    perform pgque.create_queue('test_opts_neg', '{"max_retries": -1}'::jsonb);
    raise exception 'create_queue should reject negative max_retries';
  exception
    when others then
      if sqlerrm not like '%max_retries must be >= 0%' then
        raise;
      end if;
  end;

  assert not exists (
    select 1 from pgque.queue where queue_name = 'test_opts_neg'
  ), 'queue should not exist after rejected create_queue';

  raise notice 'PASS: create_queue rejects negative max_retries';
end $$;
