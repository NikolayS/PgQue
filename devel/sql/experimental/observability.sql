-- pgque-api/observability.sql -- Observability functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Implements SPECx.md sections 5.1 and 5.2:
--   pgque.queue_stats()        -- real-time queue health
--   pgque.consumer_stats()     -- per-consumer metrics
--   pgque.queue_health()       -- operational diagnostics
--   pgque.otel_metrics()       -- OTel-compatible metric export
--   pgque.stuck_consumers()    -- consumers that haven't processed
--   pgque.in_flight()          -- messages currently being processed
--   pgque.throughput()         -- throughput over time (bucketed)
--   pgque.error_rate()         -- error rate over time (bucketed)

-- pgque.queue_stats() -- real-time queue health
create or replace function pgque.queue_stats()
returns table (
    queue_name          text,
    queue_id            int4,
    depth               bigint,
    oldest_msg_age      interval,
    consumers           int4,
    events_per_sec      numeric,
    cur_table           int4,
    rotation_age        interval,
    rotation_period     interval,
    ticker_paused       boolean,
    last_tick_time      timestamptz,
    last_tick_id        bigint,
    dlq_count           bigint
) as $$
begin
    return query
    select
        q.queue_name,
        q.queue_id,
        coalesce(
            (select max(t_cur.tick_event_seq) - min(t_sub.tick_event_seq)
             from pgque.subscription s
             join pgque.tick t_sub on t_sub.tick_queue = q.queue_id
                 and t_sub.tick_id = s.sub_last_tick
             cross join lateral (
                 select tick_event_seq from pgque.tick
                 where tick_queue = q.queue_id
                 order by tick_id desc limit 1
             ) t_cur
             where s.sub_queue = q.queue_id
            ), 0)::bigint as depth,
        (select now() - min(t.tick_time)
         from pgque.subscription s
         join pgque.tick t on t.tick_queue = q.queue_id
             and t.tick_id = s.sub_last_tick
         where s.sub_queue = q.queue_id
        ) as oldest_msg_age,
        (select count(*)::int4 from pgque.subscription
         where sub_queue = q.queue_id) as consumers,
        (select case
            when t2.tick_time = t1.tick_time then 0
            else (t2.tick_event_seq - t1.tick_event_seq)::numeric
                / extract(epoch from t2.tick_time - t1.tick_time)
         end
         from pgque.tick t1, pgque.tick t2
         where t1.tick_queue = q.queue_id and t2.tick_queue = q.queue_id
           and t2.tick_id = (select max(tick_id) from pgque.tick
                             where tick_queue = q.queue_id)
           and t1.tick_id = t2.tick_id - 1
        ) as events_per_sec,
        q.queue_cur_table,
        now() - q.queue_switch_time as rotation_age,
        q.queue_rotation_period,
        q.queue_ticker_paused,
        (select max(tick_time) from pgque.tick
         where tick_queue = q.queue_id) as last_tick_time,
        (select max(tick_id) from pgque.tick
         where tick_queue = q.queue_id) as last_tick_id,
        (select count(*) from pgque.dead_letter
         where dl_queue_id = q.queue_id)::bigint as dlq_count
    from pgque.queue q
    order by q.queue_name;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.consumer_stats() -- per-consumer metrics
create or replace function pgque.consumer_stats()
returns table (
    queue_name      text,
    consumer_name   text,
    lag             interval,
    pending_events  bigint,
    last_batch_start timestamptz,
    batch_active    boolean,
    batch_id        bigint
) as $$
begin
    return query
    select
        q.queue_name,
        c.co_name,
        now() - t.tick_time as lag,
        coalesce(
            (select max(t2.tick_event_seq) from pgque.tick t2
             where t2.tick_queue = q.queue_id) - t.tick_event_seq,
            0)::bigint as pending_events,
        s.sub_active,
        s.sub_batch is not null,
        s.sub_batch
    from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    join pgque.consumer c on c.co_id = s.sub_consumer
    left join pgque.tick t on t.tick_queue = s.sub_queue
        and t.tick_id = s.sub_last_tick
    order by q.queue_name, c.co_name;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.queue_health() -- operational diagnostics
create or replace function pgque.queue_health()
returns table (
    queue_name  text,
    check_name  text,
    status      text,
    detail      text
) as $$
begin
    -- Check: ticker is running (handle queues with no ticks yet)
    return query
    select q.queue_name, 'ticker_running'::text,
        case when max(t.tick_time) is null then 'critical'
             when now() - max(t.tick_time) > interval '10 seconds'
             then 'critical' else 'ok' end,
        'Last tick: ' || coalesce(max(t.tick_time)::text, 'never')
    from pgque.queue q
    left join pgque.tick t on t.tick_queue = q.queue_id
    where not q.queue_ticker_paused
    group by q.queue_name;

    -- Consumer lag check: handle consumers that never processed (sub_last_tick is NULL)
    return query
    select q.queue_name,
        ('consumer_lag:' || c.co_name)::text,
        case
            when t.tick_time is null then 'warning'  -- never consumed
            when now() - t.tick_time > q.queue_rotation_period then 'critical'
            when now() - t.tick_time > q.queue_rotation_period / 2 then 'warning'
            else 'ok'
        end,
        c.co_name || ' lag: ' || coalesce((now() - t.tick_time)::text, 'never consumed')
    from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    join pgque.consumer c on c.co_id = s.sub_consumer
    left join pgque.tick t on t.tick_queue = s.sub_queue and t.tick_id = s.sub_last_tick;

    -- Check: rotation overdue
    return query
    select q.queue_name, 'rotation_health'::text,
        case
            when q.queue_switch_step2 is null then 'warning'
            when now() - q.queue_switch_time > q.queue_rotation_period * 2
                then 'warning'
            else 'ok'
        end,
        case
            when q.queue_switch_step2 is null then 'mid-rotation (step2 pending)'
            else 'last rotation: ' || q.queue_switch_time::text
        end
    from pgque.queue q;

    -- Check: DLQ growing
    return query
    select q.queue_name, 'dlq_health'::text,
        case
            when count(dl.*) > 1000 then 'warning'
            when count(dl.*) > 0 then 'ok'
            else 'ok'
        end,
        count(dl.*)::text || ' dead letter events'
    from pgque.queue q
    left join pgque.dead_letter dl on dl.dl_queue_id = q.queue_id
    group by q.queue_name;

    -- Check: pg_cron jobs
    return query
    select 'system'::text, 'pg_cron_ticker'::text,
        case when cfg.ticker_job_id is null then 'critical'
             else 'ok' end,
        case when cfg.ticker_job_id is null
             then 'ticker job not scheduled'
             else 'job_id=' || cfg.ticker_job_id::text end
    from pgque.config cfg;

    return query
    select 'system'::text, 'pg_cron_maint'::text,
        case when cfg.maint_job_id is null then 'critical'
             else 'ok' end,
        case when cfg.maint_job_id is null
             then 'maint job not scheduled'
             else 'job_id=' || cfg.maint_job_id::text end
    from pgque.config cfg;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.otel_metrics() -- OTel-compatible metric export
create or replace function pgque.otel_metrics()
returns table (
    metric_name text,
    metric_type text,
    value       numeric,
    labels      jsonb
) as $$
begin
    -- Queue depth gauges
    return query
    select 'pgque.queue.depth'::text, 'gauge'::text,
           qs.depth::numeric,
           jsonb_build_object('queue', qs.queue_name)
    from pgque.queue_stats() qs;

    -- Oldest message age gauges
    return query
    select 'pgque.queue.oldest_message_age_seconds'::text, 'gauge'::text,
           coalesce(extract(epoch from qs.oldest_msg_age), 0)::numeric,
           jsonb_build_object('queue', qs.queue_name)
    from pgque.queue_stats() qs;

    -- Consumer lag gauges
    return query
    select 'pgque.consumer.lag_seconds'::text, 'gauge'::text,
           extract(epoch from cs.lag)::numeric,
           jsonb_build_object('queue', cs.queue_name,
                              'consumer', cs.consumer_name)
    from pgque.consumer_stats() cs;

    -- Consumer pending events gauges
    return query
    select 'pgque.consumer.pending_events'::text, 'gauge'::text,
           cs.pending_events::numeric,
           jsonb_build_object('queue', cs.queue_name,
                              'consumer', cs.consumer_name)
    from pgque.consumer_stats() cs;

    -- DLQ gauges
    return query
    select 'pgque.message.dead_lettered'::text, 'gauge'::text,
           qs.dlq_count::numeric,
           jsonb_build_object('queue', qs.queue_name)
    from pgque.queue_stats() qs;

    -- Events per sec gauges
    return query
    select 'pgque.queue.throughput'::text, 'gauge'::text,
           coalesce(qs.events_per_sec, 0),
           jsonb_build_object('queue', qs.queue_name)
    from pgque.queue_stats() qs;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.stuck_consumers() -- consumers that haven't processed in a long time
create or replace function pgque.stuck_consumers(
    i_threshold interval default '1 hour')
returns table (
    queue_name      text,
    consumer_name   text,
    lag             interval,
    last_active     timestamptz
) as $$
begin
    return query
    select
        q.queue_name,
        c.co_name,
        now() - t.tick_time as lag,
        s.sub_active
    from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    join pgque.consumer c on c.co_id = s.sub_consumer
    left join pgque.tick t on t.tick_queue = s.sub_queue
        and t.tick_id = s.sub_last_tick
    where now() - coalesce(t.tick_time, s.sub_active) > i_threshold
    order by lag desc nulls first;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.in_flight() -- messages currently being processed
create or replace function pgque.in_flight(i_queue_name text)
returns table (
    consumer_name   text,
    batch_id        bigint,
    batch_age       interval,
    estimated_events bigint
) as $$
begin
    return query
    select
        c.co_name,
        s.sub_batch,
        now() - s.sub_active as batch_age,
        coalesce(
            (select t_next.tick_event_seq - t_cur.tick_event_seq
             from pgque.tick t_cur
             join pgque.tick t_next on t_next.tick_queue = t_cur.tick_queue
                 and t_next.tick_id = s.sub_next_tick
             where t_cur.tick_queue = q.queue_id
               and t_cur.tick_id = s.sub_last_tick
            ), 0)::bigint as estimated_events
    from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    join pgque.consumer c on c.co_id = s.sub_consumer
    where q.queue_name = i_queue_name
      and s.sub_batch is not null
    order by c.co_name;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.throughput() -- throughput over time (bucketed from tick history)
create or replace function pgque.throughput(
    i_queue_name text,
    i_period interval,
    i_bucket_size interval)
returns table (
    bucket_start    timestamptz,
    events          bigint,
    events_per_sec  numeric
) as $$
declare
    v_queue_id int4;
begin
    select q.queue_id into v_queue_id
    from pgque.queue q where q.queue_name = i_queue_name;

    if not found then
        raise exception 'queue not found: %', i_queue_name;
    end if;

    return query
    with ticks as (
        select
            t.tick_time,
            t.tick_event_seq,
            lag(t.tick_event_seq) over (order by t.tick_id) as prev_event_seq
        from pgque.tick t
        where t.tick_queue = v_queue_id
          and t.tick_time >= now() - i_period
    ),
    bucketed as (
        select
            date_trunc('minute', tk.tick_time)
                - (extract(minute from date_trunc('minute', tk.tick_time))::int
                   % (extract(epoch from i_bucket_size)::int / 60))
                  * interval '1 minute' as bucket,
            sum(coalesce(tk.tick_event_seq - tk.prev_event_seq, 0))::bigint as events
        from ticks tk
        where tk.prev_event_seq is not null
        group by 1
    )
    select
        b.bucket as bucket_start,
        b.events,
        case when extract(epoch from i_bucket_size) > 0
            then b.events::numeric / extract(epoch from i_bucket_size)
            else 0 end as events_per_sec
    from bucketed b
    order by b.bucket;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.error_rate() -- error rate over time (retries + DLQ per time period)
create or replace function pgque.error_rate(
    i_queue_name text,
    i_period interval,
    i_bucket_size interval)
returns table (
    bucket_start    timestamptz,
    retries         bigint,
    dead_letters    bigint
) as $$
declare
    v_queue_id int4;
begin
    select q.queue_id into v_queue_id
    from pgque.queue q where q.queue_name = i_queue_name;

    if not found then
        raise exception 'queue not found: %', i_queue_name;
    end if;

    -- Generate time buckets and count retry_queue and dead_letter entries
    return query
    with buckets as (
        select generate_series(
            date_trunc('minute', now() - i_period),
            date_trunc('minute', now()),
            i_bucket_size
        ) as bucket
    )
    select
        b.bucket as bucket_start,
        coalesce((
            select count(*)
            from pgque.retry_queue rq
            where rq.ev_queue = v_queue_id
              and rq.ev_retry_after >= b.bucket
              and rq.ev_retry_after < b.bucket + i_bucket_size
        ), 0)::bigint as retries,
        coalesce((
            select count(*)
            from pgque.dead_letter dl
            where dl.dl_queue_id = v_queue_id
              and dl.dl_time >= b.bucket
              and dl.dl_time < b.bucket + i_bucket_size
        ), 0)::bigint as dead_letters
    from buckets b
    order by b.bucket;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
