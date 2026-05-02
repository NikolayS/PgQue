-- W-pgque: install pgque from source, create the bench queue, register a consumer.
-- pgque uses TRUNCATE-rotation across event_N tables — the hot path generates
-- zero dead tuples on the queue tables.

\i /pgque-sql/pgque.sql

select pgque.create_queue('bench');
select pgque.register_consumer('bench', 'bench_worker');

-- Reduce per-queue rotation period so the bench rotates frequently.
update pgque.queue
set queue_rotation_period = '15 seconds',
    queue_ticker_max_count = 100,
    queue_ticker_max_lag = '1 second',
    queue_ticker_idle_period = '1 second'
where queue_name = 'bench';

-- Helper: a single "consume one batch" call usable from pgbench.
create or replace function public.bench_consume_one()
returns integer
language plpgsql
as $$
declare
  v_batch_id bigint;
  v_count integer := 0;
begin
  v_batch_id := pgque.next_batch('bench', 'bench_worker');
  if v_batch_id is null then
    return 0;
  end if;
  select count(*) into v_count from pgque.get_batch_events(v_batch_id);
  perform pgque.finish_batch(v_batch_id);
  update bench_counters
  set dequeued = dequeued + v_count
  where workload = 'pgque';
  return v_count;
end;
$$;
