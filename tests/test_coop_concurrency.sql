\set ON_ERROR_STOP on

-- Test: cooperative consumers serialize concurrent batch allocation.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create extension if not exists dblink;

create table public.coop_concurrency_results (
  worker text primary key,
  msg_ids bigint[],
  batch_ids bigint[],
  row_count integer,
  wait_ms numeric
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

/*
 * Worker-1 receives a batch and holds the FOR UPDATE on the coop_main row
 * for 3 seconds. dblink_send_query runs in autocommit, so the entire CTE
 * is one transaction; the lock acquired by receive_coop is held until
 * pg_sleep returns. Worker-2's measured wait (asserted below) proves
 * contention so this assumption cannot silently break.
 *
 * Hold and head-start are sized for slow CI runners: w1 holds 3 s, w2
 * gets a 1 s head start before racing, expected wait ~2 s, asserted at
 * > 1.5 s. The original 0.5 s head-start + 2 s hold proved fragile under
 * load (w1's dblink open + receive_coop traversal can exceed 0.5 s on a
 * loaded PG matrix runner, which would let w2 grab the lock first and
 * make the assertion fail spuriously).
 */
select dblink_connect('coop_w1', 'dbname=' || current_database());
select dblink_send_query('coop_w1', $dblink$
with ins as (
  insert into public.coop_concurrency_results(worker, msg_ids, batch_ids, row_count, wait_ms)
  select 'w1',
         coalesce(array_agg(msg_id order by msg_id), '{}'),
         coalesce(array_agg(distinct batch_id order by batch_id), '{}'),
         count(*),
         0
  from pgque.receive_coop('coop_concurrent_alloc', 'main_c', 'w1', 10)
  returning 1
), hold_lock as (
  select pg_sleep(3)
)
select count(*)
from ins, hold_lock;
$dblink$);

-- Give worker-1 a head start to acquire the lock, then race worker-2 against
-- it. Worker-2 must block on the FOR UPDATE; measure the wait so a future
-- regression that drops the lock would surface as a near-zero wait.
select pg_sleep(1);

do $$
declare
  v_t0 timestamptz;
  v_msg_ids bigint[];
  v_batch_ids bigint[];
  v_row_count integer;
  v_wait_ms numeric;
begin
  v_t0 := clock_timestamp();

  select coalesce(array_agg(msg_id order by msg_id), '{}'),
         coalesce(array_agg(distinct batch_id order by batch_id), '{}'),
         count(*)
  into v_msg_ids, v_batch_ids, v_row_count
  from pgque.receive_coop('coop_concurrent_alloc', 'main_c', 'w2', 10);

  v_wait_ms := extract(epoch from clock_timestamp() - v_t0) * 1000;

  insert into public.coop_concurrency_results(worker, msg_ids, batch_ids, row_count, wait_ms)
  values ('w2', v_msg_ids, v_batch_ids, v_row_count, v_wait_ms);
end $$;

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

/*
 * Prove the FOR UPDATE on coop_main actually serialized worker-2 behind
 * worker-1. Worker-1 holds for 3 s after a 1 s head start; worker-2
 * should wait roughly 2 s. Anything under 1.5 s indicates the lock did
 * not block.
 */
do $$
declare
  v_w2_wait_ms numeric;
begin
  select wait_ms
  into v_w2_wait_ms
  from public.coop_concurrency_results
  where worker = 'w2';

  assert v_w2_wait_ms is not null,
    'w2 wait_ms was not recorded';
  raise notice 'w2 receive_coop blocked %ms on coop_main FOR UPDATE', round(v_w2_wait_ms, 1);
  assert v_w2_wait_ms > 1500,
    format('w2 receive_coop did not block on coop_main FOR UPDATE; observed wait %s ms (expected > 1500)', v_w2_wait_ms);
end $$;

do $$
begin
  raise notice 'PASS: cooperative concurrent allocation serialization';
end $$;

drop table public.coop_concurrency_results;
