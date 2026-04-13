-- Verify zero dead tuples after sustained load
-- This is the key pgque differentiator

select pgque.create_queue('bench_dt');
select pgque.register_consumer('bench_dt', 'dt_consumer');

-- Insert events
do $$
begin
  for i in 1..5000 loop
    perform pgque.insert_event('bench_dt', 'dt.test', '{"n":' || i || '}');
  end loop;
end $$;

select pgque.ticker();

-- Consume all events
do $$
declare
  v_batch_id bigint;
begin
  v_batch_id := pgque.next_batch('bench_dt', 'dt_consumer');
  perform pgque.finish_batch(v_batch_id);
end $$;

-- Run maintenance to trigger rotation
select pgque.ticker();

-- Check dead tuples
select relname, n_dead_tup, n_live_tup
from pg_stat_user_tables
where schemaname = 'pgque'
and relname like '%event%'
order by relname;

-- Cleanup
select pgque.unregister_consumer('bench_dt', 'dt_consumer');
select pgque.drop_queue('bench_dt');
