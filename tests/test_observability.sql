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
  perform pgque.force_next_tick('obs_queue');
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

-- Test 9: throughput() with sub-minute bucket size
do $$
declare
  v_row record;
  v_count int := 0;
begin
  for v_row in
    select * from pgque.throughput('obs_queue', '1 hour'::interval, '30 seconds'::interval)
  loop
    v_count := v_count + 1;
    assert extract(epoch from v_row.bucket_start)::numeric % 30 = 0,
      'bucket_start should align to 30-second boundaries, got ' || v_row.bucket_start;
  end loop;
  assert v_count >= 1, 'throughput(30s) should return at least one bucket';
  raise notice 'PASS: throughput() handles 30-second buckets (% rows)', v_count;
end $$;

-- Test 10: throughput() buckets non-minute-multiple sizes correctly
do $$
declare
  v_queue_id int4;
  v_seq0 bigint;
  v_anchor timestamptz;
  v_row record;
  v_count int := 0;
begin
  perform pgque.create_queue('obs_bucket_queue');

  select q.queue_id into v_queue_id
  from pgque.queue q
  where q.queue_name = 'obs_bucket_queue';

  select t.tick_event_seq into v_seq0
  from pgque.tick t
  where t.tick_queue = v_queue_id
  order by t.tick_id desc
  limit 1;

  -- Anchor on a 90-second epoch boundary in the recent past, then insert
  -- synthetic ticks at known offsets: +10 s and +40 s fall in bucket
  -- [anchor, anchor+90), +100 s falls in [anchor+90, anchor+180).
  v_anchor := to_timestamp(
    floor(extract(epoch from now() - interval '15 minutes') / 90) * 90);

  insert into pgque.tick (tick_queue, tick_id, tick_time, tick_event_seq)
  values
    (v_queue_id, 1001, v_anchor + interval '10 seconds', v_seq0),
    (v_queue_id, 1002, v_anchor + interval '40 seconds', v_seq0 + 100),
    (v_queue_id, 1003, v_anchor + interval '100 seconds', v_seq0 + 250);

  for v_row in
    select * from pgque.throughput('obs_bucket_queue', '1 hour'::interval, '90 seconds'::interval)
  loop
    v_count := v_count + 1;
    if v_count = 1 then
      assert v_row.bucket_start = v_anchor,
        'first bucket should start at ' || v_anchor || ', got ' || v_row.bucket_start;
      assert v_row.events = 100,
        'first bucket should have 100 events, got ' || v_row.events;
    elsif v_count = 2 then
      assert v_row.bucket_start = v_anchor + interval '90 seconds',
        'second bucket should start at ' || (v_anchor + interval '90 seconds')
          || ', got ' || v_row.bucket_start;
      assert v_row.events = 150,
        'second bucket should have 150 events, got ' || v_row.events;
    end if;
  end loop;
  assert v_count = 2, 'throughput(90s) should return 2 buckets, got ' || v_count;

  perform pgque.drop_queue('obs_bucket_queue');
  raise notice 'PASS: throughput() buckets 90-second intervals from epoch';
end $$;

-- Test 11: throughput() rejects non-positive bucket size
do $$
begin
  begin
    perform count(*)
    from pgque.throughput('obs_queue', '1 hour'::interval, '0 seconds'::interval);
    raise exception 'throughput() should reject non-positive bucket size';
  exception
    when others then
      if sqlerrm not like '%bucket size must be > 0%' then
        raise;
      end if;
  end;
  raise notice 'PASS: throughput() rejects non-positive bucket size';
end $$;

-- Teardown
do $$ begin
  perform pgque.unregister_consumer('obs_queue', 'obs_consumer');
  perform pgque.drop_queue('obs_queue');
end $$;
