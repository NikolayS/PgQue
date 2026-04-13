-- pgque consumer read throughput benchmark

\timing on

select pgque.create_queue('bench_read');
select pgque.register_consumer('bench_read', 'bench_consumer');

-- Insert 10,000 events
do $$
begin
  for i in 1..10000 loop
    perform pgque.insert_event('bench_read', 'bench.read', '{"n":' || i || '}');
  end loop;
end $$;

select pgque.ticker();

-- Read benchmark
do $$
declare
  v_start timestamptz;
  v_end timestamptz;
  v_batch_id bigint;
  v_count int := 0;
  v_ev record;
begin
  v_batch_id := pgque.next_batch('bench_read', 'bench_consumer');

  v_start := clock_timestamp();
  for v_ev in select * from pgque.get_batch_events(v_batch_id)
  loop
    v_count := v_count + 1;
  end loop;
  v_end := clock_timestamp();

  raise notice 'Consumer read: % events in % = % ev/s',
    v_count,
    v_end - v_start,
    round(v_count / extract(epoch from v_end - v_start));

  perform pgque.finish_batch(v_batch_id);
end $$;

-- Cleanup
select pgque.unregister_consumer('bench_read', 'bench_consumer');
select pgque.drop_queue('bench_read');
