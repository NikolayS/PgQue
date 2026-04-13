\set ON_ERROR_STOP on

-- US-4: Delayed delivery
-- As a developer, I want to schedule a message for future delivery,
-- so that I can implement reminders and scheduled tasks.
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Setup
do $$ begin
  perform pgque.create_queue('us4_reminders');
  perform pgque.subscribe('us4_reminders', 'sender');
end $$;

-- Action: schedule a message for 5 seconds in the future (per spec)
do $$
declare
  v_id bigint;
  v_de_count bigint;
begin
  v_id := pgque.send_at('us4_reminders', 'remind', '{"remind":"call back"}'::jsonb,
    now() + interval '5 seconds');

  assert v_id is not null, 'send_at should return an id';

  -- Verify event is in delayed_events, not in main queue
  select count(*) into v_de_count
  from pgque.delayed_events
  where de_queue_name = 'us4_reminders';
  assert v_de_count = 1, 'should have 1 delayed event, got ' || v_de_count;

  raise notice 'PASS: US-4 send_at stored in delayed_events';
end $$;

-- Ticker (should produce no events yet -- delayed event not due)
do $$ begin
  perform pgque.force_tick('us4_reminders');
  perform pgque.ticker();
end $$;

-- Verify: receive returns empty (event is still delayed)
do $$
declare
  v_count int := 0;
  v_msg pgque.message;
begin
  for v_msg in select * from pgque.receive('us4_reminders', 'sender', 10)
  loop
    v_count := v_count + 1;
  end loop;
  -- Count may be 0 (no batch) -- that is correct
  assert v_count = 0, 'should receive 0 messages before delay, got ' || v_count;
  raise notice 'PASS: US-4 receive empty before delay';
end $$;

-- Ack any empty batch that was opened
do $$
declare
  v_batch_id bigint;
begin
  select sub_batch into v_batch_id
  from pgque.subscription s
  join pgque.queue q on q.queue_id = s.sub_queue
  where q.queue_name = 'us4_reminders'
  and s.sub_batch is not null;

  if v_batch_id is not null then
    perform pgque.ack(v_batch_id);
  end if;
end $$;

-- Wait for the delay to pass (5 seconds per spec)
do $$ begin
  perform pg_sleep(5);
end $$;

-- Run maint (which calls maint_deliver_delayed) to move due events
do $$ begin
  perform pgque.maint();
end $$;

-- Ticker to capture the now-delivered event (force_tick bypasses throttle)
do $$ begin
  perform pgque.force_tick('us4_reminders');
  perform pgque.ticker();
end $$;

-- Verify: receive now returns the event
do $$
declare
  v_msg pgque.message;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('us4_reminders', 'sender', 10)
  loop
    v_count := v_count + 1;
    assert v_msg.type = 'remind',
      'type should be remind, got ' || coalesce(v_msg.type, 'NULL');
    assert v_msg.payload::jsonb = '{"remind":"call back"}'::jsonb,
      'payload should match, got ' || coalesce(v_msg.payload, 'NULL');

    perform pgque.ack(v_msg.batch_id);
  end loop;

  assert v_count = 1, 'should receive 1 delayed message, got ' || v_count;
  raise notice 'PASS: US-4 delayed event delivered after delay';
end $$;

-- Teardown
do $$ begin
  -- Clean up any leftover delayed events
  delete from pgque.delayed_events where de_queue_name = 'us4_reminders';
  perform pgque.unsubscribe('us4_reminders', 'sender');
  perform pgque.drop_queue('us4_reminders');
end $$;

\echo 'US-4: PASSED'
