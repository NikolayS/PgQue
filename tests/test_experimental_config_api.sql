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
