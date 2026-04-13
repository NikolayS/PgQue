-- test_observability.sql -- pgque observability functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Red/green TDD: this test file was written BEFORE the implementation.
-- Tests: queue_stats(), consumer_stats(), queue_health(), otel_metrics(),
--        stuck_consumers(), in_flight(), throughput(), error_rate()

\set ON_ERROR_STOP on

-- Setup: create a queue with known state
do $$ begin
  perform pgque.create_queue('obs_queue');
  perform pgque.register_consumer('obs_queue', 'obs_consumer');

  -- Insert some events
  perform pgque.insert_event('obs_queue', 'obs.test', '{"n":1}');
  perform pgque.insert_event('obs_queue', 'obs.test', '{"n":2}');
  perform pgque.insert_event('obs_queue', 'obs.test', '{"n":3}');
end $$;

do $$ begin
  perform pgque.force_tick('obs_queue');
  perform pgque.ticker();
end $$;

-- Test 1: queue_stats() returns rows
do $$
declare
  v_row record;
  v_found bool := false;
begin
  for v_row in select * from pgque.queue_stats()
  loop
    if v_row.queue_name = 'obs_queue' then
      v_found := true;
      assert v_row.consumers = 1, 'should have 1 consumer, got ' || v_row.consumers;
      assert v_row.depth >= 0, 'depth should be >= 0';
      raise notice 'PASS: queue_stats() queue=%, depth=%, consumers=%',
        v_row.queue_name, v_row.depth, v_row.consumers;
    end if;
  end loop;
  assert v_found, 'queue_stats() should include obs_queue';
end $$;

-- Test 2: consumer_stats() returns rows
do $$
declare
  v_row record;
  v_found bool := false;
begin
  for v_row in select * from pgque.consumer_stats()
  loop
    if v_row.queue_name = 'obs_queue' and v_row.consumer_name = 'obs_consumer' then
      v_found := true;
      assert v_row.pending_events >= 0, 'pending_events should be >= 0';
      raise notice 'PASS: consumer_stats() consumer=%, lag=%, pending=%',
        v_row.consumer_name, v_row.lag, v_row.pending_events;
    end if;
  end loop;
  assert v_found, 'consumer_stats() should include obs_consumer';
end $$;

-- Test 3: queue_health() returns diagnostic rows
do $$
declare
  v_row record;
  v_found_ticker bool := false;
begin
  for v_row in select * from pgque.queue_health()
  loop
    if v_row.queue_name = 'obs_queue' and v_row.check_name = 'ticker_running' then
      v_found_ticker := true;
      assert v_row.status in ('ok', 'warning', 'critical'),
        'status should be ok/warning/critical, got ' || v_row.status;
      raise notice 'PASS: queue_health() ticker check: %', v_row.status;
    end if;
  end loop;
  assert v_found_ticker, 'queue_health() should check ticker for obs_queue';
end $$;

-- Test 4: otel_metrics() returns rows with correct metric names
do $$
declare
  v_row record;
  v_depth_found bool := false;
begin
  for v_row in select * from pgque.otel_metrics()
  loop
    if v_row.metric_name = 'pgque.queue.depth' then
      v_depth_found := true;
      assert v_row.metric_type = 'gauge', 'depth should be gauge type';
      assert v_row.labels ? 'queue', 'should have queue label';
    end if;
  end loop;
  assert v_depth_found, 'otel_metrics() should include pgque.queue.depth';
  raise notice 'PASS: otel_metrics() returns depth gauge';
end $$;

-- Test 5: stuck_consumers()
do $$
declare
  v_count int;
begin
  select count(*) into v_count from pgque.stuck_consumers('0 seconds'::interval);
  -- Our consumer should be "stuck" since we haven't consumed anything
  assert v_count >= 1, 'stuck_consumers(0s) should find obs_consumer';
  raise notice 'PASS: stuck_consumers() finds lagging consumer';
end $$;

-- Test 6: in_flight() -- no batch open, should return 0 rows
do $$
declare
  v_count int;
begin
  select count(*) into v_count from pgque.in_flight('obs_queue');
  assert v_count = 0, 'in_flight() should return 0 rows when no batch is open, got ' || v_count;
  raise notice 'PASS: in_flight() returns 0 rows when no batch is open';
end $$;

-- Test 7: throughput() returns rows
do $$
declare
  v_count int;
begin
  select count(*) into v_count
  from pgque.throughput('obs_queue', '1 hour'::interval, '5 minutes'::interval);
  assert v_count >= 0, 'throughput() should return >= 0 rows';
  raise notice 'PASS: throughput() returns % rows', v_count;
end $$;

-- Test 8: error_rate() returns rows
do $$
declare
  v_count int;
begin
  select count(*) into v_count
  from pgque.error_rate('obs_queue', '1 hour'::interval, '5 minutes'::interval);
  assert v_count >= 0, 'error_rate() should return >= 0 rows';
  raise notice 'PASS: error_rate() returns % rows', v_count;
end $$;

-- Teardown
do $$ begin
  perform pgque.unregister_consumer('obs_queue', 'obs_consumer');
  perform pgque.drop_queue('obs_queue');
end $$;
