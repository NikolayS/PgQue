-- pg_current insert throughput benchmark
-- Run: psql -d pg_current_test -f benchmarks/insert_bench.sql

\timing on

-- Setup
select pg_current.create_queue('bench_queue');

-- Single-insert benchmark (1000 events)
do $$
declare
  v_start timestamptz;
  v_end timestamptz;
  v_count int := 1000;
begin
  v_start := clock_timestamp();
  for i in 1..v_count loop
    perform pg_current.insert_event('bench_queue', 'bench.test', '{"n":' || i || '}');
  end loop;
  v_end := clock_timestamp();
  raise notice 'Single-insert: % events in % = % ev/s',
    v_count,
    v_end - v_start,
    round(v_count / extract(epoch from v_end - v_start));
end $$;

-- Batch-insert benchmark (10,000 events in one TX)
do $$
declare
  v_start timestamptz;
  v_end timestamptz;
  v_count int := 10000;
begin
  v_start := clock_timestamp();
  for i in 1..v_count loop
    perform pg_current.insert_event('bench_queue', 'bench.batch', '{"n":' || i || '}');
  end loop;
  v_end := clock_timestamp();
  raise notice 'Batch-insert (10k/TX): % events in % = % ev/s',
    v_count,
    v_end - v_start,
    round(v_count / extract(epoch from v_end - v_start));
end $$;

-- Cleanup
select pg_current.drop_queue('bench_queue');
