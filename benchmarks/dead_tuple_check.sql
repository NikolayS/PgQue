-- Verify zero dead tuples after sustained load
-- This is the key logres differentiator

select logres.create_queue('bench_dt');
select logres.register_consumer('bench_dt', 'dt_consumer');

-- Insert events
do $$
begin
  for i in 1..5000 loop
    perform logres.insert_event('bench_dt', 'dt.test', '{"n":' || i || '}');
  end loop;
end $$;

select logres.ticker();

-- Consume all events
do $$
declare
  v_batch_id bigint;
begin
  v_batch_id := logres.next_batch('bench_dt', 'dt_consumer');
  perform logres.finish_batch(v_batch_id);
end $$;

-- Run maintenance to trigger rotation
select logres.ticker();

-- Check dead tuples
select relname, n_dead_tup, n_live_tup
from pg_stat_user_tables
where schemaname = 'logres'
and relname like '%event%'
order by relname;

-- Cleanup
select logres.unregister_consumer('bench_dt', 'dt_consumer');
select logres.drop_queue('bench_dt');
