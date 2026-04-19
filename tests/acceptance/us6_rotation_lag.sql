\set ON_ERROR_STOP on

-- US-6: Graceful rotation under consumer lag
-- As a platform team, I want a fast consumer to keep processing while
-- a slow consumer is lagging, and the system to recover once the slow
-- consumer catches up.
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Setup: create queue with short rotation_period, subscribe two consumers
do $$ begin
  perform logres.create_queue('us6_stream',
    '{"rotation_period": "30 seconds"}'::jsonb);
  perform logres.subscribe('us6_stream', 'fast');
  perform logres.subscribe('us6_stream', 'slow');
end $$;

-- Send events (batch 1)
do $$ begin
  perform logres.send('us6_stream', 'event.type', '{"batch":1,"seq":1}'::jsonb);
  perform logres.send('us6_stream', 'event.type', '{"batch":1,"seq":2}'::jsonb);
  perform logres.send('us6_stream', 'event.type', '{"batch":1,"seq":3}'::jsonb);
end $$;

-- Tick
do $$ begin
  perform logres.force_tick('us6_stream');
  perform logres.ticker();
end $$;

-- "fast" consumer: receive and ack
do $$
declare
  v_msg logres.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from logres.receive('us6_stream', 'fast', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 3, 'fast should receive 3 events, got ' || v_count;
  perform logres.ack(v_batch_id);
  raise notice 'PASS: US-6 fast consumer received and acked batch 1';
end $$;

-- "slow" consumer: do nothing (leave batch unprocessed)

-- Send more events (batch 2)
do $$ begin
  perform logres.send('us6_stream', 'event.type', '{"batch":2,"seq":1}'::jsonb);
  perform logres.send('us6_stream', 'event.type', '{"batch":2,"seq":2}'::jsonb);
end $$;

-- Tick again
do $$ begin
  perform logres.force_tick('us6_stream');
  perform logres.ticker();
end $$;

-- Verify: stuck_consumers() identifies the slow consumer as lagging
-- (using 0 seconds threshold to detect any lag at all)
do $$
declare
  v_row record;
  v_found_slow bool := false;
begin
  for v_row in select * from logres.stuck_consumers('0 seconds'::interval)
  loop
    if v_row.queue_name = 'us6_stream' and v_row.consumer_name = 'slow' then
      v_found_slow := true;
    end if;
  end loop;
  assert v_found_slow,
    'stuck_consumers should identify slow consumer as lagging';
  raise notice 'PASS: US-6 stuck_consumers() identifies slow consumer lag';
end $$;

-- Verify: queue_health() includes a consumer_lag check for slow
do $$
declare
  v_row record;
  v_found_slow bool := false;
begin
  for v_row in select * from logres.queue_health()
  loop
    if v_row.queue_name = 'us6_stream'
       and v_row.check_name = 'consumer_lag:slow' then
      v_found_slow := true;
      -- slow consumer lag is present (status may be ok/warning/critical
      -- depending on how fast the test runs relative to rotation_period)
    end if;
  end loop;
  assert v_found_slow,
    'queue_health should include consumer_lag check for slow';
  raise notice 'PASS: US-6 queue_health() includes slow consumer lag check';
end $$;

-- Verify: fast consumer can still receive new events (batch 2)
do $$
declare
  v_msg logres.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from logres.receive('us6_stream', 'fast', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 2, 'fast should receive 2 new events, got ' || v_count;
  perform logres.ack(v_batch_id);
  raise notice 'PASS: US-6 fast consumer continues receiving despite slow lag';
end $$;

-- Now slow consumer catches up: receive all pending events
do $$
declare
  v_msg logres.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from logres.receive('us6_stream', 'slow', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  -- slow should get batch 1 events (3 events)
  assert v_count = 3,
    'slow should receive first batch of 3 events, got ' || v_count;
  perform logres.ack(v_batch_id);
  raise notice 'PASS: US-6 slow consumer received batch 1 (% events)', v_count;
end $$;

-- Tick to advance after slow ack
do $$ begin
  perform logres.force_tick('us6_stream');
  perform logres.ticker();
end $$;

-- Slow consumer gets batch 2
do $$
declare
  v_msg logres.message;
  v_count int := 0;
  v_batch_id bigint;
begin
  for v_msg in select * from logres.receive('us6_stream', 'slow', 100)
  loop
    v_count := v_count + 1;
    v_batch_id := v_msg.batch_id;
  end loop;

  assert v_count = 2,
    'slow should receive batch 2 (2 events), got ' || v_count;
  perform logres.ack(v_batch_id);
  raise notice 'PASS: US-6 slow consumer caught up with batch 2';
end $$;

-- Verify: system recovered -- both consumers caught up, no more messages
do $$
declare
  v_msg logres.message;
  v_count int := 0;
begin
  for v_msg in select * from logres.receive('us6_stream', 'fast', 100)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'fast should have no more messages';

  for v_msg in select * from logres.receive('us6_stream', 'slow', 100)
  loop
    v_count := v_count + 1;
  end loop;
  assert v_count = 0, 'slow should have no more messages';

  raise notice 'PASS: US-6 system recovered, all consumers caught up';
end $$;

-- Teardown
do $$ begin
  perform logres.unsubscribe('us6_stream', 'fast');
  perform logres.unsubscribe('us6_stream', 'slow');
  perform logres.drop_queue('us6_stream');
end $$;

\echo 'US-6: PASSED'
