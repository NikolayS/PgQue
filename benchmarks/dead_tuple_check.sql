-- Verify zero dead tuples after sustained load
-- This is the key pg_current differentiator

select pg_current.create_queue('bench_dt');
select pg_current.register_consumer('bench_dt', 'dt_consumer');

-- Insert events
do $$
begin
  for i in 1..5000 loop
    perform pg_current.insert_event('bench_dt', 'dt.test', '{"n":' || i || '}');
  end loop;
end $$;

select pg_current.ticker();

-- Consume all events
do $$
declare
  v_batch_id bigint;
begin
  v_batch_id := pg_current.next_batch('bench_dt', 'dt_consumer');
  perform pg_current.finish_batch(v_batch_id);
end $$;

-- Run maintenance to trigger rotation
select pg_current.ticker();

-- Check dead tuples
select relname, n_dead_tup, n_live_tup
from pg_stat_user_tables
where schemaname = 'pg_current'
and relname like '%event%'
order by relname;

-- Cleanup
select pg_current.unregister_consumer('bench_dt', 'dt_consumer');
select pg_current.drop_queue('bench_dt');
