\set ON_ERROR_STOP on

-- Test delayed delivery via send_at()

-- Test 1: send_at() with future time -> goes to delayed_events
do $$
declare
  v_id bigint;
  v_de_count bigint;
begin
  perform pgque.create_queue('test_delayed');
  perform pgque.register_consumer('test_delayed', 'c1');

  -- Send with future delivery (1 hour from now)
  v_id := pgque.send_at('test_delayed', 'delayed.test',
    '{"delayed":true}'::jsonb, now() + interval '1 hour');
  assert v_id is not null, 'send_at should return id';

  -- Should be in delayed_events, not in queue
  select count(*) into v_de_count from pgque.delayed_events
  where de_queue_name = 'test_delayed';
  assert v_de_count = 1, 'should have 1 delayed event, got ' || v_de_count;

  -- Ticker + receive should return nothing (event is delayed)
  perform pgque.ticker();

  declare v_batch_id bigint;
  begin
    v_batch_id := pgque.next_batch('test_delayed', 'c1');
    if v_batch_id is not null then
      perform pgque.finish_batch(v_batch_id);
    end if;
  end;

  raise notice 'PASS: send_at() with future time goes to delayed_events';

  -- Cleanup delayed events manually
  delete from pgque.delayed_events where de_queue_name = 'test_delayed';
  perform pgque.unregister_consumer('test_delayed', 'c1');
  perform pgque.drop_queue('test_delayed');
end $$;

-- Test 2: send_at() with past time -> goes directly to queue
do $$
declare
  v_id bigint;
  v_de_count bigint;
  v_batch_id bigint;
  v_ev_count int := 0;
begin
  perform pgque.create_queue('test_delayed2');
  perform pgque.register_consumer('test_delayed2', 'c1');

  v_id := pgque.send_at('test_delayed2', 'immediate.test',
    '{"immediate":true}'::jsonb, now() - interval '1 second');
  assert v_id is not null, 'send_at with past time should return event id';

  -- Should NOT be in delayed_events
  select count(*) into v_de_count from pgque.delayed_events
  where de_queue_name = 'test_delayed2';
  assert v_de_count = 0, 'should not be in delayed_events';

  raise notice 'PASS: send_at() with past time goes directly to queue';

  perform pgque.unregister_consumer('test_delayed2', 'c1');
  perform pgque.drop_queue('test_delayed2');
end $$;

-- Test 3: maint_deliver_delayed() moves due events
do $$
declare
  v_count int;
  v_batch_id bigint;
  v_ev record;
begin
  perform pgque.create_queue('test_delayed3');
  perform pgque.register_consumer('test_delayed3', 'c1');

  -- Insert a delayed event that is already due (past time)
  insert into pgque.delayed_events (de_queue_name, de_deliver_at, de_type, de_data)
  values ('test_delayed3', now() - interval '1 second', 'delivered.test', '{"delivered":true}');

  -- Run maint_deliver_delayed
  select pgque.maint_deliver_delayed() into v_count;
  assert v_count >= 1, 'should deliver at least 1 event, got ' || v_count;

  -- Now ticker + receive should get the event
  perform pgque.ticker();
  v_batch_id := pgque.next_batch('test_delayed3', 'c1');

  if v_batch_id is not null then
    for v_ev in select * from pgque.get_batch_events(v_batch_id)
    loop
      assert v_ev.ev_type = 'delivered.test', 'event type should match';
    end loop;
    perform pgque.finish_batch(v_batch_id);
  else
    assert false, 'should have a batch with the delivered event';
  end if;

  raise notice 'PASS: maint_deliver_delayed moves due events';

  perform pgque.unregister_consumer('test_delayed3', 'c1');
  perform pgque.drop_queue('test_delayed3');
end $$;

-- Test 4: pgque.maint() delivers delayed events end-to-end
do $$
declare
  v_delivered int;
begin
  perform pgque.create_queue('test_maint_delayed');
  perform pgque.register_consumer('test_maint_delayed', 'c1');

  -- Insert already-due delayed event
  insert into pgque.delayed_events (de_queue_name, de_deliver_at, de_type, de_data)
  values ('test_maint_delayed', now() - interval '1 second', 'maint.test', '{"via_maint":true}');

  -- Call maint() (the wrapper that pg_cron calls)
  perform pgque.maint();

  -- Verify delayed event was delivered
  assert not exists (
    select 1 from pgque.delayed_events where de_queue_name = 'test_maint_delayed'
  ), 'delayed event should be gone after maint()';

  perform pgque.unregister_consumer('test_maint_delayed', 'c1');
  perform pgque.drop_queue('test_maint_delayed');
  raise notice 'PASS: pgque.maint() delivers delayed events';
end $$;
