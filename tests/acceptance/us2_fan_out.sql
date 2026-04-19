\set ON_ERROR_STOP on

-- US-2: Multiple consumers (fan-out)
-- As a platform team, I want multiple independent consumers on one queue,
-- so that analytics, notifications, and audit each process the same events
-- at their own pace.
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Setup: create queue and subscribe 3 consumers
do $$ begin
  perform logres.create_queue('us2_events');
  perform logres.subscribe('us2_events', 'analytics');
  perform logres.subscribe('us2_events', 'notifier');
  perform logres.subscribe('us2_events', 'audit');
end $$;

-- Action: send 5 events
do $$ begin
  perform logres.send('us2_events', 'user.signup', '{"user":1}'::jsonb);
  perform logres.send('us2_events', 'user.signup', '{"user":2}'::jsonb);
  perform logres.send('us2_events', 'user.signup', '{"user":3}'::jsonb);
  perform logres.send('us2_events', 'user.signup', '{"user":4}'::jsonb);
  perform logres.send('us2_events', 'user.signup', '{"user":5}'::jsonb);
end $$;

-- Tick (force_tick bypasses throttle)
do $$ begin
  perform logres.force_tick('us2_events');
  perform logres.ticker();
end $$;

-- Verify: analytics receives all 5
do $$
declare
  v_msg logres.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from logres.receive('us2_events', 'analytics', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 5, 'analytics should receive 5 events, got ' || v_count;
  perform logres.ack(v_batch_id);
  raise notice 'PASS: US-2 analytics receives all 5 events';
end $$;

-- Verify: notifier also receives all 5 (independent consumer)
do $$
declare
  v_msg logres.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from logres.receive('us2_events', 'notifier', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 5, 'notifier should receive 5 events, got ' || v_count;
  perform logres.ack(v_batch_id);
  raise notice 'PASS: US-2 notifier receives all 5 events';
end $$;

-- Verify: audit also receives all 5 (independent consumer)
do $$
declare
  v_msg logres.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from logres.receive('us2_events', 'audit', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 5, 'audit should receive 5 events, got ' || v_count;
  perform logres.ack(v_batch_id);
  raise notice 'PASS: US-2 audit receives all 5 events';
end $$;

-- Verify: all consumers are now caught up (no more messages)
do $$
declare
  v_msg logres.message;
  v_count int := 0;
begin
  for v_msg in select * from logres.receive('us2_events', 'analytics', 100)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'analytics should have no more messages';

  for v_msg in select * from logres.receive('us2_events', 'notifier', 100)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'notifier should have no more messages';

  for v_msg in select * from logres.receive('us2_events', 'audit', 100)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'audit should have no more messages';

  raise notice 'PASS: US-2 all consumers caught up';
end $$;

-- Teardown
do $$ begin
  perform logres.unsubscribe('us2_events', 'analytics');
  perform logres.unsubscribe('us2_events', 'notifier');
  perform logres.unsubscribe('us2_events', 'audit');
  perform logres.drop_queue('us2_events');
end $$;

\echo 'US-2: PASSED'
