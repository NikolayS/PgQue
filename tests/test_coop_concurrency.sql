\set ON_ERROR_STOP on

-- Test: cooperative consumers serialize concurrent batch allocation.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create extension if not exists dblink;

create table public.coop_concurrency_results (
  worker text primary key,
  msg_ids bigint[],
  batch_ids bigint[],
  row_count integer
);

do $$
begin
  perform pgque.create_queue('coop_concurrent_alloc');
  perform pgque.register_subconsumer('coop_concurrent_alloc', 'main_c', 'w1');
  perform pgque.register_subconsumer('coop_concurrent_alloc', 'main_c', 'w2');
end $$;

-- Two tick windows: if main-row locking is broken, both workers can race into
-- the same first window. Correct behavior serializes on coop_main and returns
-- distinct batches/events.
select pgque.send('coop_concurrent_alloc', 't', 'event-1');
select pgque.force_tick('coop_concurrent_alloc');
select pgque.ticker('coop_concurrent_alloc');
select pgque.send('coop_concurrent_alloc', 't', 'event-2');
select pgque.force_tick('coop_concurrent_alloc');
select pgque.ticker('coop_concurrent_alloc');

select dblink_connect('coop_w1', 'dbname=' || current_database());
select dblink_send_query('coop_w1', $dblink$
with ins as (
  insert into public.coop_concurrency_results(worker, msg_ids, batch_ids, row_count)
  select 'w1',
         coalesce(array_agg(msg_id order by msg_id), '{}'),
         coalesce(array_agg(distinct batch_id order by batch_id), '{}'),
         count(*)
  from pgque.receive_coop('coop_concurrent_alloc', 'main_c', 'w1', 10)
  returning 1
), hold_lock as (
  select pg_sleep(1)
)
select count(*)
from ins, hold_lock;
$dblink$);

select pg_sleep(0.2);

insert into public.coop_concurrency_results(worker, msg_ids, batch_ids, row_count)
select 'w2',
       coalesce(array_agg(msg_id order by msg_id), '{}'),
       coalesce(array_agg(distinct batch_id order by batch_id), '{}'),
       count(*)
from pgque.receive_coop('coop_concurrent_alloc', 'main_c', 'w2', 10);

do $$
begin
  while dblink_is_busy('coop_w1') = 1 loop
    perform pg_sleep(0.1);
  end loop;
end $$;

select *
from dblink_get_result('coop_w1') as t(done bigint);

select dblink_disconnect('coop_w1');

do $$
declare
  v_rows integer;
  v_total integer;
  v_distinct_msgs integer;
  v_distinct_batches integer;
  v_duplicates bigint[];
begin
  select count(*), coalesce(sum(row_count), 0)
  into v_rows, v_total
  from public.coop_concurrency_results;

  assert v_rows = 2,
    'concurrent allocation test should collect two worker results';
  assert v_total = 2,
    'concurrent workers should receive exactly two messages total, got ' || v_total;

  select count(distinct msg_id), count(distinct batch_id)
  into v_distinct_msgs, v_distinct_batches
  from public.coop_concurrency_results r
  cross join unnest(r.msg_ids, r.batch_ids) as u(msg_id, batch_id);

  assert v_distinct_msgs = 2,
    'concurrent workers must not receive duplicate events';
  assert v_distinct_batches = 2,
    'concurrent workers should allocate distinct batches';

  select array_agg(msg_id order by msg_id)
  into v_duplicates
  from (
    select msg_id
    from public.coop_concurrency_results r
    cross join unnest(r.msg_ids) as u(msg_id)
    group by msg_id
    having count(*) > 1
  ) as d;

  assert v_duplicates is null,
    'concurrent workers duplicated events: ' || v_duplicates::text;
end $$;

do $$
begin
  raise notice 'PASS: cooperative concurrent allocation serialization';
end $$;

drop table public.coop_concurrency_results;
