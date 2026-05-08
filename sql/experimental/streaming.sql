-- sql/experimental/streaming.sql -- streaming SQL prototype
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Status: experimental, not part of the default install.
-- Load explicitly: \i sql/experimental/streaming.sql
--
-- Reduces to PgQ primitives: every helper here is a thin SQL wrapper that
-- reads from the queue's parent event_<id> table (which inherits all active
-- partitions) and applies a window or join expression. No streaming runtime,
-- no incremental state -- this is a query-time view of the retention window.
--
-- Surface:
--   pgque.stream(queue, since)
--       Set-returning reader over a queue's retained events.
--   pgque.tumble(queue, bucket_size, since)
--       Tumbling (non-overlapping) windows over ev_time.
--   pgque.hop(queue, window, slide, since)
--       Hopping (sliding) windows over ev_time. Each event appears in every
--       window that contains it.
--   pgque.session(queue, gap, partition_extra, since)
--       Session windows: events partitioned by ev_extraN, split when the
--       inter-arrival gap exceeds the threshold.
--   pgque.stream_join(queue_a, queue_b, max_skew, join_extra, since)
--       Temporal equi-join on ev_extraN with a max time skew.
--   pgque.create_continuous_query(name, sql, target_table, every)
--   pgque.run_continuous_query(name)
--   pgque.drop_continuous_query(name)
--   pgque.list_continuous_queries()
--       Register a SQL query to be re-evaluated on an interval, appending
--       results to a target table. The {since} and {until} placeholders in
--       the registered SQL get substituted with timestamptz literals at run.

-- ---------------------------------------------------------------------------
-- pgque.stream() -- base reader over the queue's retained events.
-- ---------------------------------------------------------------------------
create or replace function pgque.stream(
    i_queue text,
    i_since interval default '1 hour')
returns table (
    ev_id     bigint,
    ev_time   timestamptz,
    ev_type   text,
    ev_data   text,
    ev_extra1 text,
    ev_extra2 text,
    ev_extra3 text,
    ev_extra4 text
) as $$
declare
    v_pfx text;
begin
    select queue_data_pfx into v_pfx
    from pgque.queue
    where queue_name = i_queue;

    if not found then
        raise exception 'queue not found: %', i_queue;
    end if;

    return query execute format(
        'select ev_id, ev_time, ev_type, ev_data, '
        '       ev_extra1, ev_extra2, ev_extra3, ev_extra4 '
        'from %s '
        'where ev_time >= now() - $1 '
        'order by ev_time, ev_id', v_pfx)
    using i_since;
end;
$$ language plpgsql stable security definer
   set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- pgque.tumble() -- tumbling windows. Buckets are aligned to epoch so they
-- are stable across calls within the same Postgres instance.
-- ---------------------------------------------------------------------------
create or replace function pgque.tumble(
    i_queue text,
    i_bucket_size interval,
    i_since interval default '1 hour')
returns table (
    bucket_start timestamptz,
    bucket_end   timestamptz,
    ev_id        bigint,
    ev_time      timestamptz,
    ev_type      text,
    ev_data      text,
    ev_extra1    text,
    ev_extra2    text,
    ev_extra3    text,
    ev_extra4    text
) as $$
declare
    v_secs numeric;
begin
    v_secs := extract(epoch from i_bucket_size);
    if v_secs is null or v_secs <= 0 then
        raise exception 'bucket_size must be > 0, got %', i_bucket_size;
    end if;

    return query
    select
        to_timestamp(floor(extract(epoch from s.ev_time) / v_secs) * v_secs)
            as bucket_start,
        to_timestamp(floor(extract(epoch from s.ev_time) / v_secs) * v_secs
                     + v_secs) as bucket_end,
        s.ev_id, s.ev_time, s.ev_type, s.ev_data,
        s.ev_extra1, s.ev_extra2, s.ev_extra3, s.ev_extra4
    from pgque.stream(i_queue, i_since) s
    order by 1, s.ev_id;
end;
$$ language plpgsql stable security definer
   set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- pgque.hop() -- hopping/sliding windows.
-- Generates window starts at multiples of slide and joins each event into
-- every window that contains it.
-- ---------------------------------------------------------------------------
create or replace function pgque.hop(
    i_queue text,
    i_window interval,
    i_slide interval,
    i_since interval default '1 hour')
returns table (
    bucket_start timestamptz,
    bucket_end   timestamptz,
    ev_id        bigint,
    ev_time      timestamptz,
    ev_type      text,
    ev_data      text,
    ev_extra1    text,
    ev_extra2    text,
    ev_extra3    text,
    ev_extra4    text
) as $$
declare
    v_window_secs numeric;
    v_slide_secs  numeric;
begin
    v_window_secs := extract(epoch from i_window);
    v_slide_secs  := extract(epoch from i_slide);
    if v_window_secs is null or v_window_secs <= 0 then
        raise exception 'window must be > 0, got %', i_window;
    end if;
    if v_slide_secs is null or v_slide_secs <= 0 then
        raise exception 'slide must be > 0, got %', i_slide;
    end if;

    return query
    with events as (
        select * from pgque.stream(i_queue, i_since)
    ),
    windows as (
        select to_timestamp(gs) as w_start,
               to_timestamp(gs + v_window_secs) as w_end
        from generate_series(
            floor(extract(epoch from now() - i_since - i_window)
                  / v_slide_secs) * v_slide_secs,
            floor(extract(epoch from now()) / v_slide_secs) * v_slide_secs,
            v_slide_secs
        ) gs
    )
    select w.w_start, w.w_end,
           e.ev_id, e.ev_time, e.ev_type, e.ev_data,
           e.ev_extra1, e.ev_extra2, e.ev_extra3, e.ev_extra4
    from windows w
    join events e
      on e.ev_time >= w.w_start
     and e.ev_time <  w.w_end
    order by w.w_start, e.ev_id;
end;
$$ language plpgsql stable security definer
   set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- pgque.session() -- session windows on ev_extraN partition keys.
-- A session breaks when the gap between consecutive events on the same key
-- exceeds i_gap. session_id is dense per (partition_key) starting at 1.
-- ---------------------------------------------------------------------------
create or replace function pgque.session(
    i_queue text,
    i_gap interval,
    i_partition_extra int default 1,
    i_since interval default '1 hour')
returns table (
    session_id    int,
    session_start timestamptz,
    session_end   timestamptz,
    partition_key text,
    ev_id         bigint,
    ev_time       timestamptz,
    ev_type       text,
    ev_data       text,
    ev_extra1     text,
    ev_extra2     text,
    ev_extra3     text,
    ev_extra4     text
) as $$
begin
    if i_partition_extra not between 1 and 4 then
        raise exception 'partition_extra must be in 1..4, got %',
            i_partition_extra;
    end if;

    return query
    with events as (
        select s.*,
               case i_partition_extra
                   when 1 then s.ev_extra1
                   when 2 then s.ev_extra2
                   when 3 then s.ev_extra3
                   when 4 then s.ev_extra4
               end as part_key
        from pgque.stream(i_queue, i_since) s
    ),
    flagged as (
        select e.*,
               case
                   when lag(e.ev_time) over (
                            partition by e.part_key order by e.ev_time, e.ev_id
                        ) is null then 1
                   when e.ev_time - lag(e.ev_time) over (
                            partition by e.part_key order by e.ev_time, e.ev_id
                        ) > i_gap then 1
                   else 0
               end as new_session
        from events e
    ),
    numbered as (
        select f.*,
               sum(f.new_session) over (
                   partition by f.part_key order by f.ev_time, f.ev_id
               )::int as sid
        from flagged f
    ),
    bounds as (
        select n.*,
               min(n.ev_time) over (partition by n.part_key, n.sid) as s_start,
               max(n.ev_time) over (partition by n.part_key, n.sid) as s_end
        from numbered n
    )
    select b.sid, b.s_start, b.s_end, b.part_key,
           b.ev_id, b.ev_time, b.ev_type, b.ev_data,
           b.ev_extra1, b.ev_extra2, b.ev_extra3, b.ev_extra4
    from bounds b
    order by b.part_key, b.sid, b.ev_time, b.ev_id;
end;
$$ language plpgsql stable security definer
   set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- pgque.stream_join() -- temporal equi-join across two queues.
-- Matches a.ev_extraN = b.ev_extraN and |a.ev_time - b.ev_time| <= max_skew.
-- ---------------------------------------------------------------------------
create or replace function pgque.stream_join(
    i_queue_a text,
    i_queue_b text,
    i_max_skew interval,
    i_join_extra int default 1,
    i_since interval default '1 hour')
returns table (
    a_ev_id   bigint,
    a_ev_time timestamptz,
    a_ev_type text,
    a_ev_data text,
    b_ev_id   bigint,
    b_ev_time timestamptz,
    b_ev_type text,
    b_ev_data text,
    join_key  text,
    time_skew interval
) as $$
begin
    if i_join_extra not between 1 and 4 then
        raise exception 'join_extra must be in 1..4, got %', i_join_extra;
    end if;

    return query
    with a as (
        select s.ev_id, s.ev_time, s.ev_type, s.ev_data,
               case i_join_extra
                   when 1 then s.ev_extra1 when 2 then s.ev_extra2
                   when 3 then s.ev_extra3 when 4 then s.ev_extra4
               end as k
        from pgque.stream(i_queue_a, i_since) s
    ),
    b as (
        select s.ev_id, s.ev_time, s.ev_type, s.ev_data,
               case i_join_extra
                   when 1 then s.ev_extra1 when 2 then s.ev_extra2
                   when 3 then s.ev_extra3 when 4 then s.ev_extra4
               end as k
        from pgque.stream(i_queue_b, i_since) s
    )
    select a.ev_id, a.ev_time, a.ev_type, a.ev_data,
           b.ev_id, b.ev_time, b.ev_type, b.ev_data,
           a.k,
           case when a.ev_time >= b.ev_time
                then a.ev_time - b.ev_time
                else b.ev_time - a.ev_time
           end as skew
    from a
    join b on b.k = a.k
          and b.ev_time between a.ev_time - i_max_skew
                            and a.ev_time + i_max_skew
    order by a.ev_time, a.ev_id, b.ev_id;
end;
$$ language plpgsql stable security definer
   set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Continuous queries: a registered SQL statement that runs on a schedule and
-- appends rows to a sink table. Substitution: {since} and {until} are
-- replaced with timestamptz literals at run time. {since} is the previous
-- watermark (or now() - i_every for the first run); {until} is now().
--
-- Caller responsibility:
--   - Create the target table with matching column types.
--   - Grant insert on the target table to whoever runs the cq.
--   - Make sure the SQL projects columns in the target's order.
--
-- This is admin surface: the SQL is executed verbatim with whatever
-- permissions the caller has.
-- ---------------------------------------------------------------------------
create table if not exists pgque.continuous_query (
    cq_name           text primary key,
    cq_sql            text not null,
    cq_target_table   text not null,
    cq_every          interval not null default '1 minute',
    cq_last_run       timestamptz,
    cq_last_watermark timestamptz,
    cq_runs           bigint not null default 0,
    cq_rows_written   bigint not null default 0,
    cq_created_at     timestamptz not null default now()
);

create or replace function pgque.create_continuous_query(
    i_name text,
    i_sql text,
    i_target_table text,
    i_every interval default '1 minute')
returns void as $$
begin
    if i_name is null or i_name = '' then
        raise exception 'continuous query name is required';
    end if;
    if i_sql is null or i_sql = '' then
        raise exception 'continuous query sql is required';
    end if;
    if i_target_table is null or i_target_table = '' then
        raise exception 'continuous query target_table is required';
    end if;

    -- regclass-validate the target so typos fail at registration, not at run.
    perform i_target_table::regclass;

    insert into pgque.continuous_query (
        cq_name, cq_sql, cq_target_table, cq_every)
    values (i_name, i_sql, i_target_table, i_every)
    on conflict (cq_name) do update set
        cq_sql = excluded.cq_sql,
        cq_target_table = excluded.cq_target_table,
        cq_every = excluded.cq_every;
end;
$$ language plpgsql security definer
   set search_path = pgque, pg_catalog;

create or replace function pgque.run_continuous_query(i_name text)
returns bigint as $$
declare
    v_cq        pgque.continuous_query%rowtype;
    v_since     timestamptz;
    v_until     timestamptz := now();
    v_sql       text;
    v_inserted  bigint;
begin
    select * into v_cq from pgque.continuous_query
    where cq_name = i_name;
    if not found then
        raise exception 'continuous query not found: %', i_name;
    end if;

    v_since := coalesce(v_cq.cq_last_watermark, v_until - v_cq.cq_every);

    -- Substitute {since}/{until} with timestamptz literals. We use
    -- quote_literal() so the embedded timestamps survive any quoting in
    -- the registered SQL.
    v_sql := replace(v_cq.cq_sql, '{since}', quote_literal(v_since) || '::timestamptz');
    v_sql := replace(v_sql,        '{until}', quote_literal(v_until) || '::timestamptz');

    execute format('insert into %s %s', v_cq.cq_target_table, v_sql);
    get diagnostics v_inserted = row_count;

    update pgque.continuous_query
       set cq_last_run = v_until,
           cq_last_watermark = v_until,
           cq_runs = cq_runs + 1,
           cq_rows_written = cq_rows_written + v_inserted
     where cq_name = i_name;

    return v_inserted;
end;
$$ language plpgsql security definer
   set search_path = pgque, pg_catalog;

create or replace function pgque.drop_continuous_query(i_name text)
returns boolean as $$
declare
    v_deleted bigint;
begin
    delete from pgque.continuous_query where cq_name = i_name;
    get diagnostics v_deleted = row_count;
    return v_deleted > 0;
end;
$$ language plpgsql security definer
   set search_path = pgque, pg_catalog;

create or replace function pgque.list_continuous_queries()
returns table (
    name           text,
    target_table   text,
    every          interval,
    last_run       timestamptz,
    runs           bigint,
    rows_written   bigint,
    created_at     timestamptz
) as $$
begin
    return query
    select cq_name, cq_target_table, cq_every, cq_last_run,
           cq_runs, cq_rows_written, cq_created_at
    from pgque.continuous_query
    order by cq_name;
end;
$$ language plpgsql stable security definer
   set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------
-- Read-only window helpers: pgque_reader. They project event payload, which
-- the role can already select directly via the parent event_<id> table.
grant execute on function pgque.stream(text, interval)              to pgque_reader;
grant execute on function pgque.tumble(text, interval, interval)    to pgque_reader;
grant execute on function pgque.hop(text, interval, interval, interval) to pgque_reader;
grant execute on function pgque.session(text, interval, int, interval)  to pgque_reader;
grant execute on function pgque.stream_join(text, text, interval, int, interval)
    to pgque_reader;

-- Continuous query control: admin only. run_continuous_query() executes
-- arbitrary SQL with the caller's privileges; do not expose to reader/writer.
grant execute on function pgque.create_continuous_query(text, text, text, interval)
    to pgque_admin;
grant execute on function pgque.run_continuous_query(text)  to pgque_admin;
grant execute on function pgque.drop_continuous_query(text) to pgque_admin;
grant execute on function pgque.list_continuous_queries()   to pgque_admin;
grant select, insert, update, delete on pgque.continuous_query to pgque_admin;
