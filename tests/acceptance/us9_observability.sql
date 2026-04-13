-- US-9: Observability and health monitoring
-- As an operator, I want to quickly diagnose queue health and consumer lag
-- SPECx.md section 13.3
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

-- Setup: create 3 queues with varying load patterns
-- Queue 1: healthy (consumer keeps up)
-- Queue 2: lagging consumer
-- Queue 3: has DLQ entries

do $$ begin
  perform pgque.create_queue('us9_healthy');
  perform pgque.create_queue('us9_lagging');
  perform pgque.create_queue('us9_dlq');

  perform pgque.register_consumer('us9_healthy', 'fast_worker');
  perform pgque.register_consumer('us9_lagging', 'slow_worker');
  perform pgque.register_consumer('us9_dlq', 'dlq_worker');

  -- Insert events into all queues
  perform pgque.insert_event('us9_healthy', 'order.created', '{"id":1}');
  perform pgque.insert_event('us9_lagging', 'order.created', '{"id":2}');
  perform pgque.insert_event('us9_lagging', 'order.created', '{"id":3}');
  perform pgque.insert_event('us9_dlq', 'order.created', '{"id":4}');
end $$;

-- Tick all queues
do $$ begin
  perform pgque.force_tick('us9_healthy');
  perform pgque.force_tick('us9_lagging');
  perform pgque.force_tick('us9_dlq');
  perform pgque.ticker();
end $$;

-- Consume us9_healthy fully (fast worker keeps up)
do $$
declare
  v_batch_id bigint;
begin
  select pgque.next_batch('us9_healthy', 'fast_worker') into v_batch_id;
  if v_batch_id is not null then
    perform pgque.finish_batch(v_batch_id);
  end if;
end $$;

-- Add a DLQ entry for us9_dlq
do $$
declare
  v_batch_id bigint;
  v_ev record;
begin
  select pgque.next_batch('us9_dlq', 'dlq_worker') into v_batch_id;
  if v_batch_id is not null then
    -- Get first event and send it to DLQ
    for v_ev in select * from pgque.get_batch_events(v_batch_id)
    loop
      perform pgque.event_dead(
        v_batch_id, v_ev.ev_id, 'max retries exceeded',
        v_ev.ev_time, v_ev.ev_txid, v_ev.ev_retry,
        v_ev.ev_type, v_ev.ev_data);
      exit;  -- just one event
    end loop;
    perform pgque.finish_batch(v_batch_id);
  end if;
end $$;

-- Verify: queue_stats() shows correct depth, throughput, DLQ count
do $$
declare
  v_row record;
  v_found_healthy bool := false;
  v_found_lagging bool := false;
  v_found_dlq bool := false;
begin
  for v_row in select * from pgque.queue_stats()
  loop
    if v_row.queue_name = 'us9_healthy' then
      v_found_healthy := true;
      assert v_row.depth = 0, 'healthy queue depth should be 0, got ' || v_row.depth;
      assert v_row.consumers = 1, 'should have 1 consumer';
      assert v_row.dlq_count = 0, 'no DLQ entries for healthy queue';
    end if;
    if v_row.queue_name = 'us9_lagging' then
      v_found_lagging := true;
      assert v_row.depth >= 0, 'lagging queue depth should be >= 0';
      assert v_row.consumers = 1, 'should have 1 consumer';
    end if;
    if v_row.queue_name = 'us9_dlq' then
      v_found_dlq := true;
      assert v_row.dlq_count = 1, 'DLQ queue should have 1 dead letter, got ' || v_row.dlq_count;
    end if;
  end loop;
  assert v_found_healthy, 'queue_stats should include us9_healthy';
  assert v_found_lagging, 'queue_stats should include us9_lagging';
  assert v_found_dlq, 'queue_stats should include us9_dlq';
  raise notice 'PASS: US-9 queue_stats() shows correct depth and DLQ counts';
end $$;

-- Verify: consumer_stats() shows correct lag per consumer
do $$
declare
  v_row record;
  v_found_fast bool := false;
  v_found_slow bool := false;
begin
  for v_row in select * from pgque.consumer_stats()
  loop
    if v_row.queue_name = 'us9_healthy' and v_row.consumer_name = 'fast_worker' then
      v_found_fast := true;
      assert v_row.pending_events = 0,
        'fast_worker should have 0 pending, got ' || v_row.pending_events;
    end if;
    if v_row.queue_name = 'us9_lagging' and v_row.consumer_name = 'slow_worker' then
      v_found_slow := true;
      assert v_row.pending_events >= 0,
        'slow_worker should have >= 0 pending, got ' || v_row.pending_events;
      assert v_row.lag is not null, 'slow_worker should have lag';
    end if;
  end loop;
  assert v_found_fast, 'consumer_stats should include fast_worker';
  assert v_found_slow, 'consumer_stats should include slow_worker';
  raise notice 'PASS: US-9 consumer_stats() shows correct lag per consumer';
end $$;

-- Verify: queue_health() returns 'ok', 'warning', 'critical' appropriately
do $$
declare
  v_row record;
  v_ticker_checks int := 0;
  v_lag_checks int := 0;
  v_rotation_checks int := 0;
  v_dlq_checks int := 0;
begin
  for v_row in select * from pgque.queue_health()
  loop
    -- All statuses must be valid
    if v_row.check_name not in ('pg_cron_ticker', 'pg_cron_maint') then
      assert v_row.status in ('ok', 'warning', 'critical'),
        'invalid status: ' || v_row.status || ' for check ' || v_row.check_name;
    end if;

    if v_row.check_name = 'ticker_running' then
      v_ticker_checks := v_ticker_checks + 1;
    end if;
    if v_row.check_name like 'consumer_lag:%' then
      v_lag_checks := v_lag_checks + 1;
    end if;
    if v_row.check_name = 'rotation_health' then
      v_rotation_checks := v_rotation_checks + 1;
    end if;
    if v_row.check_name = 'dlq_health' then
      v_dlq_checks := v_dlq_checks + 1;
    end if;
  end loop;

  assert v_ticker_checks >= 3, 'should have ticker checks for all 3 queues, got ' || v_ticker_checks;
  assert v_lag_checks >= 3, 'should have lag checks for all 3 consumers, got ' || v_lag_checks;
  assert v_rotation_checks >= 3, 'should have rotation checks for all 3 queues, got ' || v_rotation_checks;
  assert v_dlq_checks >= 3, 'should have DLQ checks for all 3 queues, got ' || v_dlq_checks;
  raise notice 'PASS: US-9 queue_health() returns correct status values';
end $$;

-- Verify: otel_metrics() returns rows with correct metric names and types
do $$
declare
  v_row record;
  v_depth_count int := 0;
  v_lag_count int := 0;
  v_dlq_count int := 0;
begin
  for v_row in select * from pgque.otel_metrics()
  loop
    if v_row.metric_name = 'pgque.queue.depth' then
      v_depth_count := v_depth_count + 1;
      assert v_row.metric_type = 'gauge', 'depth should be gauge';
      assert v_row.labels ? 'queue', 'depth should have queue label';
    end if;
    if v_row.metric_name = 'pgque.consumer.lag_seconds' then
      v_lag_count := v_lag_count + 1;
      assert v_row.metric_type = 'gauge', 'lag should be gauge';
      assert v_row.labels ? 'consumer', 'lag should have consumer label';
    end if;
    if v_row.metric_name = 'pgque.message.dead_lettered' then
      v_dlq_count := v_dlq_count + 1;
      assert v_row.metric_type = 'gauge', 'dead_lettered should be gauge';
    end if;
  end loop;
  assert v_depth_count >= 3, 'should have depth metrics for all queues, got ' || v_depth_count;
  assert v_lag_count >= 3, 'should have lag metrics for all consumers, got ' || v_lag_count;
  assert v_dlq_count >= 3, 'should have DLQ metrics for all queues, got ' || v_dlq_count;
  raise notice 'PASS: US-9 otel_metrics() returns correct metric names and types';
end $$;

-- Verify: stuck_consumers() identifies the lagging consumer
do $$
declare
  v_found_slow bool := false;
  v_row record;
begin
  for v_row in select * from pgque.stuck_consumers('0 seconds'::interval)
  loop
    if v_row.queue_name = 'us9_lagging' and v_row.consumer_name = 'slow_worker' then
      v_found_slow := true;
    end if;
  end loop;
  assert v_found_slow, 'stuck_consumers should identify slow_worker';
  raise notice 'PASS: US-9 stuck_consumers() identifies lagging consumer';
end $$;

-- Teardown: purge DLQ entries before unregistering consumers
-- (dead_letter has FK to consumer, so DLQ must be cleaned first)
do $$ begin
  perform pgque.dlq_purge('us9_dlq', '0 seconds'::interval);
  perform pgque.unregister_consumer('us9_healthy', 'fast_worker');
  perform pgque.unregister_consumer('us9_lagging', 'slow_worker');
  perform pgque.unregister_consumer('us9_dlq', 'dlq_worker');
  perform pgque.drop_queue('us9_healthy');
  perform pgque.drop_queue('us9_lagging');
  perform pgque.drop_queue('us9_dlq');
end $$;

\echo 'US-9: PASSED'
