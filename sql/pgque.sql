-- pgque.sql -- PgQ Universal Edition
-- Version: 1.0.0-dev
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Install: \i pgque.sql
-- Start:   SELECT pgque.start();
-- Usage:   See https://github.com/NikolayS/pgque

create schema if not exists pgque;

-- ======================================================================
-- Section 1: Tables (derived from PgQ)
-- Origin: pgq/structure/tables.sql
--
-- PgQue transformations applied:
--   1. Schema rename: pgq → pgque (all identifiers, grants, references)
--   2. txid_current() → pg_current_xact_id()::text::bigint (PG14+ API)
--   3. txid_snapshot → pg_snapshot (type rename)
--   4. ev_txid kept as xid8 (required by pg_visible_in_snapshot)
--   5. queue_per_tx_limit column removed (not supported without C)
--   6. set default_with_oids removed (deprecated since PG 12)
--   7. CREATE TABLE → CREATE TABLE IF NOT EXISTS (idempotent install)
-- ======================================================================

-- ----------------------------------------------------------------------
-- Section: Internal Tables
--
-- Overview:
--      pgque.queue                   - Queue configuration
--      pgque.consumer                - Consumer names
--      pgque.subscription            - Consumer registrations
--      pgque.tick                    - Per-queue snapshots (ticks)
--      pgque.event_*                 - Data tables
--      pgque.retry_queue             - Events to be retried later
--
-- 
-- Standard triggers store events in the pgque.event_* data tables
-- There is one top event table pgque.event_<queue_id> for each queue
-- inherited from pgque.event_template wuith three tables for actual data
-- pgque.event_<queue_id>_0 to pgque.event_<queue_id>_2.
--
-- The active table is rotated at interval, so that if all the consubers
-- have passed some poin the oldes one can be emptied using TRUNCATE command
-- for efficiency
-- 
-- 
-- ----------------------------------------------------------------------



-- ----------------------------------------------------------------------
-- Table: pgque.consumer
--
--      Name to id lookup for consumers
--
-- Columns:
--      co_id       - consumer's id for internal usage
--      co_name     - consumer's id for external usage
-- ----------------------------------------------------------------------
create table if not exists pgque.consumer (
        co_id       serial,
        co_name     text        not null,

        constraint consumer_pkey primary key (co_id),
        constraint consumer_name_uq UNIQUE (co_name)
);


-- ----------------------------------------------------------------------
-- Table: pgque.queue
--
--     Information about available queues
--
-- Columns:
--      queue_id                    - queue id for internal usage
--      queue_name                  - queue name visible outside
--      queue_ntables               - how many data tables the queue has
--      queue_cur_table             - which data table is currently active
--      queue_rotation_period       - period for data table rotation
--      queue_switch_step1          - tx when rotation happened
--      queue_switch_step2          - tx after rotation was committed
--      queue_switch_time           - time when switch happened
--      queue_external_ticker       - ticks come from some external sources
--      queue_ticker_paused         - ticker is paused
--      queue_disable_insert        - disallow pgque.insert_event()
--      queue_ticker_max_count      - batch should not contain more events
--      queue_ticker_max_lag        - events should not age more
--      queue_ticker_idle_period    - how often to tick when no events happen
--      queue_data_pfx              - prefix for data table names
--      queue_event_seq             - sequence for event id's
--      queue_tick_seq              - sequence for tick id's
--      queue_extra_maint           - array of functon names to call during maintenance
-- ----------------------------------------------------------------------
create table if not exists pgque.queue (
        queue_id                    serial,
        queue_name                  text        not null,

        queue_ntables               integer     not null default 3,
        queue_cur_table             integer     not null default 0,
        queue_rotation_period       interval    not null default '2 hours',
        queue_switch_step1          bigint      not null default pg_current_xact_id()::text::bigint, -- PgQue transformation: txid_current()→pg_current_xact_id()::text::bigint (PG14+)
        queue_switch_step2          bigint               default pg_current_xact_id()::text::bigint,
        queue_switch_time           timestamptz not null default now(),

        queue_external_ticker       boolean     not null default false,
        queue_disable_insert        boolean     not null default false,
        queue_ticker_paused         boolean     not null default false,

        queue_ticker_max_count      integer     not null default 500,
        queue_ticker_max_lag        interval    not null default '3 seconds',
        queue_ticker_idle_period    interval    not null default '1 minute',

        queue_data_pfx              text        not null,
        queue_event_seq             text        not null,
        queue_tick_seq              text        not null,

        queue_extra_maint           text[],

        constraint queue_pkey primary key (queue_id),
        constraint queue_name_uq unique (queue_name)
);

-- ----------------------------------------------------------------------
-- Table: pgque.tick
--
--      Snapshots for event batching
--
-- Columns:
--      tick_queue      - queue id whose tick it is
--      tick_id         - ticks id (per-queue)
--      tick_time       - time when tick happened
--      tick_snapshot   - transaction state
--      tick_event_seq  - last value for event seq
-- ----------------------------------------------------------------------
create table if not exists pgque.tick (
        tick_queue                  int4            not null,
        tick_id                     bigint          not null,
        tick_time                   timestamptz     not null default now(),
        tick_snapshot               pg_snapshot   not null default pg_current_snapshot(),
        tick_event_seq              bigint          not null, -- may be NULL on upgraded dbs

        constraint tick_pkey primary key (tick_queue, tick_id),
        constraint tick_queue_fkey foreign key (tick_queue)
                                   references pgque.queue (queue_id)
);

-- ----------------------------------------------------------------------
-- Sequence: pgque.batch_id_seq
--
--      Sequence for batch id's.
-- ----------------------------------------------------------------------
create sequence if not exists pgque.batch_id_seq;

-- ----------------------------------------------------------------------
-- Table: pgque.subscription
--
--      Consumer registration on a queue.
--
-- Columns:
--
--      sub_id          - subscription id for internal usage
--      sub_queue       - queue id
--      sub_consumer    - consumer's id
--      sub_last_tick   - last tick the consumer processed
--      sub_batch       - shortcut for queue_id/consumer_id/tick_id
--      sub_next_tick   - batch end pos
-- ----------------------------------------------------------------------
create table if not exists pgque.subscription (
        sub_id                          serial      not null,
        sub_queue                       int4        not null,
        sub_consumer                    int4        not null,
        sub_last_tick                   bigint,
        sub_active                      timestamptz not null default now(),
        sub_batch                       bigint,
        sub_next_tick                   bigint,

        constraint subscription_pkey primary key (sub_queue, sub_consumer),
        constraint subscription_batch_idx unique (sub_batch),
        constraint sub_queue_fkey foreign key (sub_queue)
                                   references pgque.queue (queue_id),
        constraint sub_consumer_fkey foreign key (sub_consumer)
                                   references pgque.consumer (co_id)
);

-- ----------------------------------------------------------------------
-- Table: pgque.event_template
--
--      Parent table for all event tables
--
-- Columns:
--      ev_id               - event's id, supposed to be unique per queue
--      ev_time             - when the event was inserted
--      ev_txid             - transaction id which inserted the event
--      ev_owner            - subscription id that wanted to retry this
--      ev_retry            - how many times the event has been retried, NULL for new events
--      ev_type             - consumer/producer can specify what the data fields contain
--      ev_data             - data field
--      ev_extra1           - extra data field
--      ev_extra2           - extra data field
--      ev_extra3           - extra data field
--      ev_extra4           - extra data field
-- ----------------------------------------------------------------------
create table if not exists pgque.event_template (
        ev_id               bigint          not null,
        ev_time             timestamptz     not null,

        ev_txid             xid8          not null default pg_current_xact_id(), -- PgQue transformation: bigint→xid8 (needed for pg_visible_in_snapshot)
        ev_owner            int4,
        ev_retry            int4,

        ev_type             text,
        ev_data             text,
        ev_extra1           text,
        ev_extra2           text,
        ev_extra3           text,
        ev_extra4           text
);

-- ----------------------------------------------------------------------
-- Table: pgque.retry_queue
--
--      Events to be retried.  When retry time reaches, they will
--      be put back into main queue.
--
-- Columns:
--      ev_retry_after          - time when it should be re-inserted to main queue
--      ev_queue                - queue id, used to speed up event copy into queue
--      *                       - same as pgque.event_template
-- ----------------------------------------------------------------------
create table if not exists pgque.retry_queue (
    ev_retry_after          timestamptz     not null,
    ev_queue                int4            not null,

    like pgque.event_template,

    constraint rq_pkey primary key (ev_owner, ev_id),
    constraint rq_queue_id_fkey foreign key (ev_queue)
                             references pgque.queue (queue_id)
);
alter table pgque.retry_queue alter column ev_owner set not null;
alter table pgque.retry_queue alter column ev_txid drop not null;
create index if not exists rq_retry_idx on pgque.retry_queue (ev_retry_after);

-- ======================================================================
-- Section 2: Internal functions (derived from PgQ)
-- Origin: pgq/functions/*.sql
--
-- PgQue transformations applied:
--   1. Schema rename: pgq → pgque
--   2. txid_* → pg_* function renames (PG14+ snapshot API)
--   3. pg_snapshot_xmin/xmax wrapped with ::text::bigint (xid8→bigint)
--   4. pg_current_xact_id() cast to ::text::bigint (xid8→bigint)
--   5. SECURITY DEFINER functions get SET search_path = pgque, pg_catalog
--   6. pgq_node/Londiste hooks removed from maint_operations
--   7. pg_notify() injected into ticker for LISTEN/NOTIFY wakeup
-- ======================================================================


create or replace function pgque.upgrade_schema()
returns int4 as $$
-- updates table structure if necessary
declare
    cnt int4 = 0;
begin

    -- pgque.subscription.sub_last_tick: NOT NULL -> NULL
    perform 1 from information_schema.columns
      where table_schema = 'pgque'
        and table_name = 'subscription'
        and column_name ='sub_last_tick'
        and is_nullable = 'NO';
    if found then
        alter table pgque.subscription
            alter column sub_last_tick
            drop not null;
        cnt := cnt + 1;
    end if;

    -- create roles
    perform 1 from pg_catalog.pg_roles where rolname = 'pgque_reader';
    if not found then
        create role pgque_reader;
        cnt := cnt + 1;
    end if;
    perform 1 from pg_catalog.pg_roles where rolname = 'pgque_writer';
    if not found then
        create role pgque_writer;
        cnt := cnt + 1;
    end if;
    perform 1 from pg_catalog.pg_roles where rolname = 'pgque_admin';
    if not found then
        create role pgque_admin in role pgque_reader, pgque_writer;
        cnt := cnt + 1;
    end if;

    perform 1 from pg_attribute
        where attrelid = 'pgque.queue'::regclass
          and attname = 'queue_extra_maint';
    if not found then
        alter table pgque.queue add column queue_extra_maint text[];
    end if;

    return 0;
end;
$$ language plpgsql;

create or replace function pgque.batch_event_sql(x_batch_id bigint)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgque.batch_event_sql(1)
--      Creates SELECT statement that fetches events for this batch.
--
-- Parameters:
--      x_batch_id    - ID of a active batch.
--
-- Returns:
--      SQL statement.
-- ----------------------------------------------------------------------

-- ----------------------------------------------------------------------
-- Algorithm description:
--      Given 2 snapshots, sn1 and sn2 with sn1 having xmin1, xmax1
--      and sn2 having xmin2, xmax2 create expression that filters
--      right txid's from event table.
--
--      Simplest solution would be
--      > WHERE ev_txid >= xmin1 AND ev_txid <= xmax2
--      >   AND NOT pg_visible_in_snapshot(ev_txid, sn1)
--      >   AND pg_visible_in_snapshot(ev_txid, sn2)
--
--      The simple solution has a problem with long transactions (xmin1 very low).
--      All the batches that happen when the long tx is active will need
--      to scan all events in that range.  Here is 2 optimizations used:
--
--      1)  Use [xmax1..xmax2] for range scan.  That limits the range to
--      txids that actually happened between two snapshots.  For txids
--      in the range [xmin1..xmax1] look which ones were actually
--      committed between snapshots and search for them using exact
--      values using IN (..) list.
--
--      2) As most TX are short, there could be lot of them that were
--      just below xmax1, but were committed before xmax2.  So look
--      if there are ID's near xmax1 and lower the range to include
--      them, thus decresing size of IN (..) list.
-- ----------------------------------------------------------------------
declare
    rec             record;
    sql             text;
    tbl             text;
    arr             text;
    part            text;
    select_fields   text;
    retry_expr      text;
    batch           record;
begin
    select s.sub_last_tick, s.sub_next_tick, s.sub_id, s.sub_queue,
           pg_snapshot_xmax(last.tick_snapshot)::text::bigint as tx_start,
           pg_snapshot_xmax(cur.tick_snapshot)::text::bigint as tx_end,
           last.tick_snapshot as last_snapshot,
           cur.tick_snapshot as cur_snapshot
        into batch
        from pgque.subscription s, pgque.tick last, pgque.tick cur
        where s.sub_batch = x_batch_id
          and last.tick_queue = s.sub_queue
          and last.tick_id = s.sub_last_tick
          and cur.tick_queue = s.sub_queue
          and cur.tick_id = s.sub_next_tick;
    if not found then
        raise exception 'batch not found';
    end if;

    -- load older transactions
    arr := '';
    for rec in
        -- active tx-es in prev_snapshot that were committed in cur_snapshot
        select id1 from
            pg_snapshot_xip(batch.last_snapshot) id1 left join
            pg_snapshot_xip(batch.cur_snapshot) id2 on (id1 = id2)
        where id2 is null
        order by 1 desc
    loop
        -- try to avoid big IN expression, so try to include nearby
        -- tx'es into range
        if batch.tx_start - 100 <= rec.id1::text::bigint then
            batch.tx_start := rec.id1::text::bigint;
        else
            if arr = '' then
                arr := '''' || rec.id1::text || '''::xid8';
            else
                arr := arr || ',''' || rec.id1::text || '''::xid8';
            end if;
        end if;
    end loop;

    -- must match pgque.event_template
    select_fields := 'select ev_id, ev_time, ev_txid, ev_retry, ev_type,'
        || ' ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4';
    retry_expr :=  ' and (ev_owner is null or ev_owner = '
        || batch.sub_id::text || ')';

    -- now generate query that goes over all potential tables
    sql := '';
    for rec in
        select xtbl from pgque.batch_event_tables(x_batch_id) xtbl
    loop
        tbl := pgque.quote_fqname(rec.xtbl);
        -- this gets newer queries that definitely are not in prev_snapshot
        part := select_fields
            || ' from pgque.tick cur, pgque.tick last, ' || tbl || ' ev '
            || ' where cur.tick_id = ' || batch.sub_next_tick::text
            || ' and cur.tick_queue = ' || batch.sub_queue::text
            || ' and last.tick_id = ' || batch.sub_last_tick::text
            || ' and last.tick_queue = ' || batch.sub_queue::text
            || ' and ev.ev_txid >= ''' || batch.tx_start::text || '''::xid8'
            || ' and ev.ev_txid <= ''' || batch.tx_end::text || '''::xid8'
            || ' and pg_visible_in_snapshot(ev.ev_txid, cur.tick_snapshot)'
            || ' and not pg_visible_in_snapshot(ev.ev_txid, last.tick_snapshot)'
            || retry_expr;
        -- now include older tx-es, that were ongoing
        -- at the time of prev_snapshot
        if arr <> '' then
            part := part || ' union all '
                || select_fields || ' from ' || tbl || ' ev '
                || ' where ev.ev_txid in (' || arr || ')'
                || retry_expr;
        end if;
        if sql = '' then
            sql := part;
        else
            sql := sql || ' union all ' || part;
        end if;
    end loop;
    if sql = '' then
        raise exception 'could not construct sql for batch %', x_batch_id;
    end if;
    return sql || ' order by 1';
end;
$$ language plpgsql;  -- no perms needed

create or replace function pgque.batch_event_tables(x_batch_id bigint)
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgque.batch_event_tables(1)
--
--     Returns set of table names where this batch events may reside.
--
-- Parameters:
--     x_batch_id    - ID of a active batch.
-- ----------------------------------------------------------------------
declare
    nr                    integer;
    tbl                   text;
    use_prev              integer;
    use_next              integer;
    batch                 record;
begin
    select
           pg_snapshot_xmin(last.tick_snapshot)::text::bigint as tx_min, -- absolute minimum
           pg_snapshot_xmax(cur.tick_snapshot)::text::bigint as tx_max, -- absolute maximum
           q.queue_data_pfx, q.queue_ntables,
           q.queue_cur_table, q.queue_switch_step1, q.queue_switch_step2
        into batch
        from pgque.tick last, pgque.tick cur, pgque.subscription s, pgque.queue q
        where cur.tick_id = s.sub_next_tick
          and cur.tick_queue = s.sub_queue
          and last.tick_id = s.sub_last_tick
          and last.tick_queue = s.sub_queue
          and s.sub_batch = x_batch_id
          and q.queue_id = s.sub_queue;
    if not found then
        raise exception 'Cannot find data for batch %', x_batch_id;
    end if;

    -- if its definitely not in one or other, look into both
    if batch.tx_max < batch.queue_switch_step1 then
        use_prev := 1;
        use_next := 0;
    elsif batch.queue_switch_step2 is not null
      and (batch.tx_min > batch.queue_switch_step2)
    then
        use_prev := 0;
        use_next := 1;
    else
        use_prev := 1;
        use_next := 1;
    end if;

    if use_prev then
        nr := batch.queue_cur_table - 1;
        if nr < 0 then
            nr := batch.queue_ntables - 1;
        end if;
        tbl := batch.queue_data_pfx || '_' || nr::text;
        return next tbl;
    end if;

    if use_next then
        tbl := batch.queue_data_pfx || '_' || batch.queue_cur_table::text;
        return next tbl;
    end if;

    return;
end;
$$ language plpgsql; -- no perms needed

create or replace function pgque.event_retry_raw(
    x_queue text,
    x_consumer text,
    x_retry_after timestamptz,
    x_ev_id bigint,
    x_ev_time timestamptz,
    x_ev_retry integer,
    x_ev_type text,
    x_ev_data text,
    x_ev_extra1 text,
    x_ev_extra2 text,
    x_ev_extra3 text,
    x_ev_extra4 text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.event_retry_raw(12)
--
--      Allows full control over what goes to retry queue.
--
-- Parameters:
--      x_queue         - name of the queue
--      x_consumer      - name of the consumer
--      x_retry_after   - when the event should be processed again
--      x_ev_id         - event id
--      x_ev_time       - creation time
--      x_ev_retry      - retry count
--      x_ev_type       - user data
--      x_ev_data       - user data
--      x_ev_extra1     - user data
--      x_ev_extra2     - user data
--      x_ev_extra3     - user data
--      x_ev_extra4     - user data
--
-- Returns:
--      Event ID.
-- ----------------------------------------------------------------------
declare
    q record;
    id bigint;
begin
    select sub_id, queue_event_seq, sub_queue into q
      from pgque.consumer, pgque.queue, pgque.subscription
     where queue_name = x_queue
       and co_name = x_consumer
       and sub_consumer = co_id
       and sub_queue = queue_id;
    if not found then
        raise exception 'consumer not registered';
    end if;

    id := x_ev_id;
    if id is null then
        id := nextval(q.queue_event_seq);
    end if;

    insert into pgque.retry_queue (ev_retry_after, ev_queue,
            ev_id, ev_time, ev_owner, ev_retry,
            ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    values (x_retry_after, q.sub_queue,
            id, x_ev_time, q.sub_id, x_ev_retry,
            x_ev_type, x_ev_data, x_ev_extra1, x_ev_extra2,
            x_ev_extra3, x_ev_extra4);

    return id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog; -- PgQue transformation: pin search_path (SECURITY DEFINER hardening)

create or replace function pgque.find_tick_helper(
    in i_queue_id int4,
    in i_prev_tick_id int8,
    in i_prev_tick_time timestamptz,
    in i_prev_tick_seq int8,
    in i_min_count int8,
    in i_min_interval interval,
    out next_tick_id int8,
    out next_tick_time timestamptz,
    out next_tick_seq int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgque.find_tick_helper(6)
--
--      Helper function for pgque.next_batch_custom() to do extended tick search.
-- ----------------------------------------------------------------------
declare
    sure    boolean;
    can_set boolean;
    t       record;
    cnt     int8;
    ival    interval;
begin
    -- first, fetch last tick of the queue
    select tick_id, tick_time, tick_event_seq into t
        from pgque.tick
        where tick_queue = i_queue_id
          and tick_id > i_prev_tick_id
        order by tick_queue desc, tick_id desc
        limit 1;
    if not found then
        return;
    end if;
    
    -- check whether batch would end up within reasonable limits
    sure := true;
    can_set := false;
    if i_min_count is not null then
        cnt = t.tick_event_seq - i_prev_tick_seq;
        if cnt >= i_min_count then
            can_set := true;
        end if;
        if cnt > i_min_count * 2 then
            sure := false;
        end if;
    end if;
    if i_min_interval is not null then
        ival = t.tick_time - i_prev_tick_time;
        if ival >= i_min_interval then
            can_set := true;
        end if;
        if ival > i_min_interval * 2 then
            sure := false;
        end if;
    end if;

    -- if last tick too far away, do large scan
    if not sure then
        select tick_id, tick_time, tick_event_seq into t
            from pgque.tick
            where tick_queue = i_queue_id
              and tick_id > i_prev_tick_id
              and ((i_min_count is not null and (tick_event_seq - i_prev_tick_seq) >= i_min_count)
                  or
                   (i_min_interval is not null and (tick_time - i_prev_tick_time) >= i_min_interval))
            order by tick_queue asc, tick_id asc
            limit 1;
        can_set := true;
    end if;
    if can_set then
        next_tick_id := t.tick_id;
        next_tick_time := t.tick_time;
        next_tick_seq := t.tick_event_seq;
    end if;
    return;
end;
$$ language plpgsql stable;

create or replace function pgque.ticker(i_queue_name text, i_tick_id bigint, i_orig_timestamp timestamptz, i_event_seq bigint)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.ticker(3)
--
--     External ticker: Insert a tick with a particular tick_id and timestamp.
--
-- Parameters:
--     i_queue_name     - Name of the queue
--     i_tick_id        - Id of new tick.
--
-- Returns:
--     Tick id.
-- ----------------------------------------------------------------------
begin
    insert into pgque.tick (tick_queue, tick_id, tick_time, tick_event_seq)
    select queue_id, i_tick_id, i_orig_timestamp, i_event_seq
        from pgque.queue
        where queue_name = i_queue_name
          and queue_external_ticker
          and not queue_ticker_paused;
    if not found then
        raise exception 'queue not found or ticker disabled: %', i_queue_name;
    end if;

    -- make sure seqs stay current
    perform pgque.seq_setval(queue_tick_seq, i_tick_id),
            pgque.seq_setval(queue_event_seq, i_event_seq)
        from pgque.queue
        where queue_name = i_queue_name;


    -- pgque: notify listeners after tick
    perform pg_notify('pgque_' || i_queue_name, i_tick_id::text); -- PgQue transformation: LISTEN/NOTIFY wakeup (not in original PgQ)
    return i_tick_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.ticker(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.ticker(1)
--
--     Check if tick is needed for the queue and insert it.
--
--     For pgqadm usage.
--
-- Parameters:
--     i_queue_name     - Name of the queue
--
-- Returns:
--     Tick id or NULL if no tick was done.
-- ----------------------------------------------------------------------
declare
    res bigint;
    q record;
    state record;
    last2 record;
begin
    select queue_id, queue_tick_seq, queue_external_ticker,
            queue_ticker_max_count, queue_ticker_max_lag,
            queue_ticker_idle_period, queue_event_seq,
            pgque.seq_getval(queue_event_seq) as event_seq,
            queue_ticker_paused
        into q
        from pgque.queue where queue_name = i_queue_name;
    if not found then
        raise exception 'no such queue';
    end if;

    if q.queue_external_ticker then
        raise exception 'This queue has external tick source.';
    end if;

    if q.queue_ticker_paused then
        raise exception 'Ticker has been paused for this queue';
    end if;

    -- load state from last tick
    select now() - tick_time as lag,
           q.event_seq - tick_event_seq as new_events,
           tick_id, tick_time, tick_event_seq,
           pg_snapshot_xmax(tick_snapshot)::text::bigint as sxmax,
           pg_snapshot_xmin(tick_snapshot)::text::bigint as sxmin
        into state
        from pgque.tick
        where tick_queue = q.queue_id
        order by tick_queue desc, tick_id desc
        limit 1;

    if found then
        if state.sxmin > pg_current_xact_id()::text::bigint then
            raise exception 'Invalid PgQ state: old xmin=%, old xmax=%, cur txid=%',
                            state.sxmin, state.sxmax, pg_current_xact_id()::text::bigint;
        end if;
        if state.new_events < 0 then
            raise warning 'Negative new_events?  old=% cur=%', state.tick_event_seq, q.event_seq;
        end if;
        if state.sxmax > pg_current_xact_id()::text::bigint then
            raise warning 'Dubious PgQ state: old xmax=%, cur txid=%', state.sxmax, pg_current_xact_id()::text::bigint;
        end if;

        if state.new_events > 0 then
            -- there are new events, should we wait a bit?
            if state.new_events < q.queue_ticker_max_count
                and state.lag < q.queue_ticker_max_lag
            then
                return NULL;
            end if;
        else
            -- no new events, should we apply idle period?
            -- check previous event from the last one.
            select state.tick_time - tick_time as lag
                into last2
                from pgque.tick
                where tick_queue = q.queue_id
                    and tick_id < state.tick_id
                order by tick_queue desc, tick_id desc
                limit 1;
            if found then
                -- gradually decrease the tick frequency
                if (state.lag < q.queue_ticker_max_lag / 2)
                    or
                   (state.lag < last2.lag * 2
                    and state.lag < q.queue_ticker_idle_period)
                then
                    return NULL;
                end if;
            end if;
        end if;
    end if;

    insert into pgque.tick (tick_queue, tick_id, tick_event_seq)
        values (q.queue_id, nextval(q.queue_tick_seq), q.event_seq);


    -- pgque: notify listeners after tick
    perform pg_notify('pgque_' || i_queue_name, currval(q.queue_tick_seq)::text); -- PgQue transformation: LISTEN/NOTIFY wakeup (not in original PgQ)
    return currval(q.queue_tick_seq);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.ticker() returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.ticker(0)
--
--     Creates ticks for all unpaused queues which dont have external ticker.
--
-- Returns:
--     Number of queues that were processed.
-- ----------------------------------------------------------------------
declare
    res bigint;
    q record;
begin
    res := 0;
    for q in
        select queue_name from pgque.queue
            where not queue_external_ticker
                  and not queue_ticker_paused
            order by queue_name
    loop
        if pgque.ticker(q.queue_name) > 0 then
            res := res + 1;
        end if;
    end loop;
    return res;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.maint_retry_events()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_retry_events(0)
--
--      Moves retry events back to main queue.
--
--      It moves small amount at a time.  It should be called
--      until it returns 0
--
-- Returns:
--      Number of events processed.
-- ----------------------------------------------------------------------
declare
    cnt    integer;
    rec    record;
begin
    cnt := 0;

    -- allow only single event mover at a time, without affecting inserts
    lock table pgque.retry_queue in share update exclusive mode;

    for rec in
        select queue_name,
               ev_id, ev_time, ev_owner, ev_retry, ev_type, ev_data,
               ev_extra1, ev_extra2, ev_extra3, ev_extra4
          from pgque.retry_queue, pgque.queue
         where ev_retry_after <= current_timestamp
           and queue_id = ev_queue
         order by ev_retry_after
         limit 10
    loop
        cnt := cnt + 1;
        perform pgque.insert_event_raw(rec.queue_name,
                    rec.ev_id, rec.ev_time, rec.ev_owner, rec.ev_retry,
                    rec.ev_type, rec.ev_data, rec.ev_extra1, rec.ev_extra2,
                    rec.ev_extra3, rec.ev_extra4);
        delete from pgque.retry_queue
         where ev_owner = rec.ev_owner
           and ev_id = rec.ev_id;
    end loop;
    return cnt;
end;
$$ language plpgsql; -- need admin access

create or replace function pgque.maint_rotate_tables_step1(i_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_rotate_tables_step1(1)
--
--      Rotate tables for one queue.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--
-- Returns:
--      0
-- ----------------------------------------------------------------------
declare
    badcnt          integer;
    cf              record;
    nr              integer;
    tbl             text;
    lowest_tick_id  int8;
    lowest_xmin     int8;
begin
    -- check if needed and load record
    select * from pgque.queue into cf
        where queue_name = i_queue_name
          and queue_rotation_period is not null
          and queue_switch_step2 is not null
          and queue_switch_time + queue_rotation_period < current_timestamp
        for update;
    if not found then
        return 0;
    end if;

    -- if DB is in invalid state, stop
    if pg_current_xact_id()::text::bigint < cf.queue_switch_step1 then
        raise exception 'queue % maint failure: step1=%, current=%',
                i_queue_name, cf.queue_switch_step1, pg_current_xact_id()::text::bigint;
    end if;

    -- find lowest tick for that queue
    select min(sub_last_tick) into lowest_tick_id
      from pgque.subscription
     where sub_queue = cf.queue_id;

    -- if some consumer exists
    if lowest_tick_id is not null then
        -- is the slowest one still on previous table?
        select pg_snapshot_xmin(tick_snapshot)::text::bigint into lowest_xmin
          from pgque.tick
         where tick_queue = cf.queue_id
           and tick_id = lowest_tick_id;
        if not found then
            raise exception 'queue % maint failure: tick % not found', i_queue_name, lowest_tick_id;
        end if;
        if lowest_xmin <= cf.queue_switch_step2 then
            return 0; -- skip rotation then
        end if;
    end if;

    -- nobody on previous table, we can rotate
    
    -- calc next table number and name
    nr := cf.queue_cur_table + 1;
    if nr = cf.queue_ntables then
        nr := 0;
    end if;
    tbl := cf.queue_data_pfx || '_' || nr::text;

    -- there may be long lock on the table from pg_dump,
    -- detect it and skip rotate then
    begin
        execute 'lock table ' || pgque.quote_fqname(tbl) || ' nowait';
        execute 'truncate ' || pgque.quote_fqname(tbl);
    exception
        when lock_not_available then
            -- cannot truncate, skipping rotate
            return 0;
    end;

    -- remember the moment
    update pgque.queue
        set queue_cur_table = nr,
            queue_switch_time = current_timestamp,
            queue_switch_step1 = pg_current_xact_id()::text::bigint,
            queue_switch_step2 = NULL
        where queue_id = cf.queue_id;

    -- Clean ticks by using step2 txid from previous rotation.
    -- That should keep all ticks for all batches that are completely
    -- in old table.  This keeps them for longer than needed, but:
    -- 1. we want the pgque.tick table to be big, to avoid Postgres
    --    accitentally switching to seqscans on that.
    -- 2. that way we guarantee to consumers that they an be moved
    --    back on the queue at least for one rotation_period.
    --    (may help in disaster recovery)
    delete from pgque.tick
        where tick_queue = cf.queue_id
          and pg_snapshot_xmin(tick_snapshot)::text::bigint < cf.queue_switch_step2;

    return 0;
end;
$$ language plpgsql; -- need admin access


create or replace function pgque.maint_rotate_tables_step2()
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_rotate_tables_step2(0)
--
--      Stores the txid when the rotation was visible.  It should be
--      called in separate transaction than pgque.maint_rotate_tables_step1()
-- ----------------------------------------------------------------------
begin
    update pgque.queue
       set queue_switch_step2 = pg_current_xact_id()::text::bigint
     where queue_switch_step2 is null;
    return 0;
end;
$$ language plpgsql; -- need admin access

create or replace function pgque.maint_tables_to_vacuum()
returns setof text as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_tables_to_vacuum(0)
--
--      Returns list of tablenames that need frequent vacuuming.
--
--      The goal is to avoid hardcoding them into maintenance process.
--
-- Returns:
--      List of table names.
-- ----------------------------------------------------------------------
declare
    scm text;
    tbl text;
    fqname text;
begin
    -- assume autovacuum handles them fine
    if current_setting('autovacuum') = 'on' then
        return;
    end if;

    for scm, tbl in values
        ('pgque', 'subscription'),
        ('pgque', 'consumer'),
        ('pgque', 'queue'),
        ('pgque', 'tick'),
        ('pgque', 'retry_queue'),
        ('pgq_ext', 'completed_tick'),
        ('pgq_ext', 'completed_batch'),
        ('pgq_ext', 'completed_event'),
        ('pgq_ext', 'partial_batch'),
        --('pgq_node', 'node_location'),
        --('pgq_node', 'node_info'),
        ('pgq_node', 'local_state'),
        --('pgq_node', 'subscriber_info'),
        --('londiste', 'table_info'),
        ('londiste', 'seq_info'),
        --('londiste', 'applied_execute'),
        --('londiste', 'pending_fkeys'),
        ('txid', 'epoch'),
        ('londiste', 'completed')
    loop
        select n.nspname || '.' || t.relname into fqname
            from pg_class t, pg_namespace n
            where n.oid = t.relnamespace
                and n.nspname = scm
                and t.relname = tbl;
        if found then
            return next fqname;
        end if;
    end loop;
    return;
end;
$$ language plpgsql;

create or replace function pgque.maint_operations(out func_name text, out func_arg text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.maint_operations(0)
--
--      Returns list of functions to call for maintenance.
--
--      The goal is to avoid hardcoding them into maintenance process.
--
-- Function signature:
--      Function should take either 1 or 0 arguments and return 1 if it wants
--      to be called immediately again, 0 if not.
--
-- Returns:
--      func_name   - Function to call
--      func_arg    - Optional argument to function (queue name)
-- ----------------------------------------------------------------------
declare
    ops text[];
    nrot int4;
begin
    -- rotate step 1
    nrot := 0;
    func_name := 'pgque.maint_rotate_tables_step1';
    for func_arg in
        select queue_name from pgque.queue
            where queue_rotation_period is not null
                and queue_switch_step2 is not null
                and queue_switch_time + queue_rotation_period < current_timestamp
            order by 1
    loop
        nrot := nrot + 1;
        return next;
    end loop;

    -- rotate step 2
    if nrot = 0 then
        select count(1) from pgque.queue
            where queue_rotation_period is not null
                and queue_switch_step2 is null
            into nrot;
    end if;
    if nrot > 0 then
        func_name := 'pgque.maint_rotate_tables_step2';
        func_arg := NULL;
        return next;
    end if;

    -- check if extra field exists
    perform 1 from pg_attribute
      where attrelid = 'pgque.queue'::regclass
        and attname = 'queue_extra_maint';
    if found then
        -- add extra ops
        for func_arg, ops in
            select q.queue_name, queue_extra_maint from pgque.queue q
             where queue_extra_maint is not null
             order by 1
        loop
            for i in array_lower(ops, 1) .. array_upper(ops, 1)
            loop
                func_name = ops[i];
                return next;
            end loop;
        end loop;
    end if;

    -- vacuum tables
    func_name := 'vacuum';
    for func_arg in
        select * from pgque.maint_tables_to_vacuum()
    loop
        return next;
    end loop;

    return;
end;
$$ language plpgsql;

create or replace function pgque.grant_perms(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.grant_perms(1)
--
--      Make event tables readable by public.
--
-- Parameters:
--      x_queue_name        - Name of the queue.
--
-- Returns:
--      nothing
-- ----------------------------------------------------------------------
declare
    q           record;
    i           integer;
    pos         integer;
    tbl_perms   text;
    seq_perms   text;
    dst_schema  text;
    dst_table   text;
    part_table  text;
begin
    select * from pgque.queue into q
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Queue not found';
    end if;

    -- split data table name to components
    pos := position('.' in q.queue_data_pfx);
    if pos > 0 then
        dst_schema := substring(q.queue_data_pfx for pos - 1);
        dst_table := substring(q.queue_data_pfx from pos + 1);
    else
        dst_schema := 'public';
        dst_table := q.queue_data_pfx;
    end if;

    -- tick seq, normal users don't need to modify it
    execute 'grant select on ' || pgque.quote_fqname(q.queue_tick_seq) || ' to public';

    -- event seq
    execute 'grant select on ' || pgque.quote_fqname(q.queue_event_seq) || ' to public';
    execute 'grant usage on ' || pgque.quote_fqname(q.queue_event_seq) || ' to pgque_admin';

    -- set grants on parent table
    perform pgque._grant_perms_from('pgque', 'event_template', dst_schema, dst_table);

    -- set grants on real event tables
    for i in 0 .. q.queue_ntables - 1 loop
        part_table := dst_table  || '_' || i::text;
        perform pgque._grant_perms_from('pgque', 'event_template', dst_schema, part_table);
    end loop;

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque._grant_perms_from(src_schema text, src_table text, dst_schema text, dst_table text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.grant_perms_from(1)
--
--      Copy grants from one table to another.
--      Workaround for missing GRANTS option for CREATE TABLE LIKE.
-- ----------------------------------------------------------------------
declare
    fq_table text;
    sql text;
    g record;
    q_grantee text;
begin
    fq_table := quote_ident(dst_schema) || '.' || quote_ident(dst_table);

    for g in
        select grantor, grantee, privilege_type, is_grantable
            from information_schema.table_privileges
            where table_schema = src_schema
                and table_name = src_table
    loop
        if g.grantee = 'PUBLIC' then
            q_grantee = 'public';
        else
            q_grantee = quote_ident(g.grantee);
        end if;
        sql := 'grant ' || g.privilege_type || ' on ' || fq_table
            || ' to ' || q_grantee;
        if g.is_grantable = 'YES' then
            sql := sql || ' with grant option';
        end if;
        execute sql;
    end loop;

    return 1;
end;
$$ language plpgsql strict;

create or replace function pgque.tune_storage(i_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.tune_storage(1)
--
--      Tunes storage settings for queue data tables
-- ----------------------------------------------------------------------
declare
    tbl  text;
    tbloid oid;
    q record;
    i int4;
    sql text;
    pgver int4;
begin
    pgver := current_setting('server_version_num');

    select * into q
      from pgque.queue where queue_name = i_queue_name;
    if not found then
        return 0;
    end if;

    for i in 0 .. (q.queue_ntables - 1) loop
        tbl := q.queue_data_pfx || '_' || i::text;

        -- set fillfactor
        sql := 'alter table ' || tbl || ' set (fillfactor = 100';

        -- autovacuum for 8.4+
        if pgver >= 80400 then
            sql := sql || ', autovacuum_enabled=off, toast.autovacuum_enabled =off';
        end if;
        sql := sql || ')';
        execute sql;

        -- autovacuum for 8.3
        if pgver < 80400 then
            tbloid := tbl::regclass::oid;
            delete from pg_catalog.pg_autovacuum where vacrelid = tbloid;
            insert into pg_catalog.pg_autovacuum values (tbloid, false, -1,-1,-1,-1,-1,-1,-1,-1);
        end if;
    end loop;

    return 1;
end;
$$ language plpgsql strict;



create or replace function pgque.force_tick(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.force_tick(2)
--
--      Simulate lots of events happening to force ticker to tick.
--
--      Should be called in loop, with some delay until last tick
--      changes or too much time is passed.
--
--      Such function is needed because paraller calls of pgque.ticker() are
--      dangerous, and cannot be protected with locks as snapshot
--      is taken before locking.
--
-- Parameters:
--      i_queue_name     - Name of the queue
--
-- Returns:
--      Currently last tick id.
-- ----------------------------------------------------------------------
declare
    q  record;
    t  record;
begin
    -- bump seq and get queue id
    select queue_id,
           setval(queue_event_seq, nextval(queue_event_seq)
                                   + queue_ticker_max_count * 2 + 1000) as tmp
      into q from pgque.queue
     where queue_name = i_queue_name
       and not queue_external_ticker
       and not queue_ticker_paused;

    --if not found then
    --    raise notice 'queue not found or ticks not allowed';
    --end if;

    -- return last tick id
    select tick_id into t
      from pgque.tick, pgque.queue
     where tick_queue = queue_id and queue_name = i_queue_name
     order by tick_queue desc, tick_id desc limit 1;

    return t.tick_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.seq_getval(i_seq_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.seq_getval(1)
--
--      Read current last_val from seq, without affecting it.
--
-- Parameters:
--      i_seq_name     - Name of the sequence
--
-- Returns:
--      last value.
-- ----------------------------------------------------------------------
declare
    res     int8;
    fqname  text;
    pos     integer;
    s       text;
    n       text;
begin
    pos := position('.' in i_seq_name);
    if pos > 0 then
        s := substring(i_seq_name for pos - 1);
        n := substring(i_seq_name from pos + 1);
    else
        s := 'public';
        n := i_seq_name;
    end if;
    fqname := quote_ident(s) || '.' || quote_ident(n);

    execute 'select last_value from ' || fqname into res;
    return res;
end;
$$ language plpgsql strict;

create or replace function pgque.seq_setval(i_seq_name text, i_new_value int8)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.seq_setval(2)
--
--      Like setval() but does not allow going back.
--
-- Parameters:
--      i_seq_name      - Name of the sequence
--      i_new_value     - new value
--
-- Returns:
--      current last value.
-- ----------------------------------------------------------------------
declare
    res     int8;
    fqname  text;
begin
    fqname := pgque.quote_fqname(i_seq_name);

    res := pgque.seq_getval(i_seq_name);
    if res < i_new_value then
        perform setval(fqname, i_new_value);
        return i_new_value;
    end if;
    return res;
end;
$$ language plpgsql strict;


create or replace function pgque.quote_fqname(i_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgque.quote_fqname(1)
--
--      Quete fully-qualified object name for SQL.
--
--      First dot is taken as schema separator.
--
--      If schema is missing, 'public' is assumed.
--
-- Parameters:
--      i_name  - fully qualified object name.
--
-- Returns:
--      Quoted name.
-- ----------------------------------------------------------------------
declare
    res     text;
    pos     integer;
    s       text;
    n       text;
begin
    pos := position('.' in i_name);
    if pos > 0 then
        s := substring(i_name for pos - 1);
        n := substring(i_name from pos + 1);
    else
        s := 'public';
        n := i_name;
    end if;
    return quote_ident(s) || '.' || quote_ident(n);
end;
$$ language plpgsql strict immutable;

create or replace function pgque.create_queue(i_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.create_queue(1)
--
--      Creates new queue with given name.
--
-- Returns:
--      0 - queue already exists
--      1 - queue created
-- Calls:
--      pgque.grant_perms(i_queue_name);
--      pgque.ticker(i_queue_name);
--      pgque.tune_storage(i_queue_name);
-- Tables directly manipulated:
--      insert - pgque.queue
--      create - pgque.event_N () inherits (pgque.event_template)
--      create - pgque.event_N_0 .. pgque.event_N_M () inherits (pgque.event_N)
-- ----------------------------------------------------------------------
declare
    tblpfx   text;
    tblname  text;
    idxpfx   text;
    idxname  text;
    sql      text;
    id       integer;
    tick_seq text;
    ev_seq text;
    n_tables integer;
begin
    if i_queue_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if exists
    perform 1 from pgque.queue where queue_name = i_queue_name;
    if found then
        return 0;
    end if;

    -- insert event
    id := nextval('pgque.queue_queue_id_seq');
    tblpfx := 'pgque.event_' || id::text;
    idxpfx := 'event_' || id::text;
    tick_seq := 'pgque.event_' || id::text || '_tick_seq';
    ev_seq := 'pgque.event_' || id::text || '_id_seq';
    insert into pgque.queue (queue_id, queue_name,
            queue_data_pfx, queue_event_seq, queue_tick_seq)
        values (id, i_queue_name, tblpfx, ev_seq, tick_seq);

    select queue_ntables into n_tables from pgque.queue
        where queue_id = id;

    -- create seqs
    execute 'CREATE SEQUENCE ' || pgque.quote_fqname(tick_seq);
    execute 'CREATE SEQUENCE ' || pgque.quote_fqname(ev_seq);

    -- create data tables
    execute 'CREATE TABLE ' || pgque.quote_fqname(tblpfx) || ' () '
            || ' INHERITS (pgque.event_template)';
    for i in 0 .. (n_tables - 1) loop
        tblname := tblpfx || '_' || i::text;
        idxname := idxpfx || '_' || i::text || '_txid_idx';
        execute 'CREATE TABLE ' || pgque.quote_fqname(tblname) || ' () '
                || ' INHERITS (' || pgque.quote_fqname(tblpfx) || ')';
        execute 'ALTER TABLE ' || pgque.quote_fqname(tblname) || ' ALTER COLUMN ev_id '
                || ' SET DEFAULT nextval(' || quote_literal(ev_seq) || ')';
        execute 'create index ' || quote_ident(idxname) || ' on '
                || pgque.quote_fqname(tblname) || ' (ev_txid)';
    end loop;

    perform pgque.grant_perms(i_queue_name);

    perform pgque.ticker(i_queue_name);

    perform pgque.tune_storage(i_queue_name);

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.drop_queue(x_queue_name text, x_force bool)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.drop_queue(2)
--
--     Drop queue and all associated tables.
--
-- Parameters:
--      x_queue_name    - queue name
--      x_force         - ignore (drop) existing consumers
-- Returns:
--      1 - success
-- Calls:
--      pgque.unregister_consumer(queue_name, consumer_name)
--      perform pgque.ticker(i_queue_name);
--      perform pgque.tune_storage(i_queue_name);
-- Tables directly manipulated:
--      delete - pgque.queue
--      drop - pgque.event_N (), pgque.event_N_0 .. pgque.event_N_M 
-- ----------------------------------------------------------------------
declare
    tblname  text;
    q record;
    num integer;
begin
    -- check if exists
    select * into q from pgque.queue
        where queue_name = x_queue_name
        for update;
    if not found then
        raise exception 'No such event queue';
    end if;

    if x_force then
        perform pgque.unregister_consumer(queue_name, consumer_name)
           from pgque.get_consumer_info(x_queue_name);
    else
        -- check if no consumers
        select count(*) into num from pgque.subscription
            where sub_queue = q.queue_id;
        if num > 0 then
            raise exception 'cannot drop queue, consumers still attached';
        end if;
    end if;

    -- drop data tables
    for i in 0 .. (q.queue_ntables - 1) loop
        tblname := q.queue_data_pfx || '_' || i::text;
        execute 'DROP TABLE ' || pgque.quote_fqname(tblname);
    end loop;
    execute 'DROP TABLE ' || pgque.quote_fqname(q.queue_data_pfx);

    -- delete ticks
    delete from pgque.tick where tick_queue = q.queue_id;

    -- drop seqs
    -- FIXME: any checks needed here?
    execute 'DROP SEQUENCE ' || pgque.quote_fqname(q.queue_tick_seq);
    execute 'DROP SEQUENCE ' || pgque.quote_fqname(q.queue_event_seq);

    -- delete event
    delete from pgque.queue
        where queue_name = x_queue_name;

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.drop_queue(x_queue_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.drop_queue(1)
--
--     Drop queue and all associated tables.
--     No consumers must be listening on the queue.
--
-- ----------------------------------------------------------------------
begin
    return pgque.drop_queue(x_queue_name, false);
end;
$$ language plpgsql strict;


create or replace function pgque.set_queue_config(
    x_queue_name    text,
    x_param_name    text,
    x_param_value   text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.set_queue_config(3)
--
--
--     Set configuration for specified queue.
--
-- Parameters:
--      x_queue_name    - Name of the queue to configure.
--      x_param_name    - Configuration parameter name.
--      x_param_value   - Configuration parameter value.
--  
-- Returns:
--     0 if event was already in queue, 1 otherwise.
-- Calls:
--      None
-- Tables directly manipulated:
--      update - pgque.queue
-- ----------------------------------------------------------------------
declare
    v_param_name    text;
begin
    -- discard NULL input
    if x_queue_name is null or x_param_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if queue exists
    perform 1 from pgque.queue where queue_name = x_queue_name;
    if not found then
        raise exception 'No such event queue';
    end if;

    -- check if valid parameter name
    v_param_name := 'queue_' || x_param_name;
    if v_param_name not in (
        'queue_ticker_max_count',
        'queue_ticker_max_lag',
        'queue_ticker_idle_period',
        'queue_ticker_paused',
        'queue_rotation_period',
        'queue_external_ticker')
    then
        raise exception 'cannot change parameter "%s"', x_param_name;
    end if;

    execute 'update pgque.queue set ' 
        || v_param_name || ' = ' || quote_literal(x_param_value)
        || ' where queue_name = ' || quote_literal(x_queue_name);

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.insert_event(queue_name text, ev_type text, ev_data text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.insert_event(3)
--
--      Insert a event into queue.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_type         - User-specified type for the event
--      ev_data         - User data for the event
--
-- Returns:
--      Event ID
-- Calls:
--      pgque.insert_event(7)
-- ----------------------------------------------------------------------
begin
    return pgque.insert_event(queue_name, ev_type, ev_data, null, null, null, null);
end;
$$ language plpgsql;



create or replace function pgque.insert_event(
    queue_name text, ev_type text, ev_data text,
    ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.insert_event(7)
--
--      Insert a event into queue with all the extra fields.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_type         - User-specified type for the event
--      ev_data         - User data for the event
--      ev_extra1       - Extra data field for the event
--      ev_extra2       - Extra data field for the event
--      ev_extra3       - Extra data field for the event
--      ev_extra4       - Extra data field for the event
--
-- Returns:
--      Event ID
-- Calls:
--      pgque.insert_event_raw(11)
-- Tables directly manipulated:
--      insert - pgque.insert_event_raw(11), a C function, inserts into current event_N_M table
-- ----------------------------------------------------------------------
begin
    return pgque.insert_event_raw(queue_name, null, now(), null, null,
            ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.current_event_table(x_queue_name text)
returns text as $$
-- ----------------------------------------------------------------------
-- Function: pgque.current_event_table(1)
--
--      Return active event table for particular queue.
--      Event can be added to it without going via functions,
--      e.g. by COPY.
--
--      If queue is disabled and GUC session_replication_role <> 'replica'
--      then raises exception.
--
--      or expressed in a different way - an even table of a disabled queue
--      is returned only on replica
--
-- Note:
--      The result is valid only during current transaction.
--
-- Permissions:
--      Actual insertion requires superuser access.
--
-- Parameters:
--      x_queue_name    - Queue name.
-- ----------------------------------------------------------------------
declare
    res text;
    disabled boolean;
begin
    select queue_data_pfx || '_' || queue_cur_table::text,
           queue_disable_insert
        into res, disabled
        from pgque.queue where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not found';
    end if;
    if disabled then
        if current_setting('session_replication_role') <> 'replica' then
            raise exception 'Writing to queue disabled';
        end if;
    end if;
    return res;
end;
$$ language plpgsql; -- no perms needed

create or replace function pgque.register_consumer(
    x_queue_name text,
    x_consumer_id text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.register_consumer(2)
--
--      Subscribe consumer on a queue.
--
--      From this moment forward, consumer will see all events in the queue.
--
-- Parameters:
--      x_queue_name        - Name of queue
--      x_consumer_name     - Name of consumer
--
-- Returns:
--      0  - if already registered
--      1  - if new registration
-- Calls:
--      pgque.register_consumer_at(3)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    return pgque.register_consumer_at(x_queue_name, x_consumer_id, NULL);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.register_consumer_at(
    x_queue_name text,
    x_consumer_name text,
    x_tick_pos bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.register_consumer_at(3)
--
--      Extended registration, allows to specify tick_id.
--
-- Note:
--      For usage in special situations.
--
-- Parameters:
--      x_queue_name        - Name of a queue
--      x_consumer_name     - Name of consumer
--      x_tick_pos          - Tick ID
--
-- Returns:
--      0/1 whether consumer has already registered.
-- Calls:
--      None
-- Tables directly manipulated:
--      update/insert - pgque.subscription
-- ----------------------------------------------------------------------
declare
    tmp         text;
    last_tick   bigint;
    x_queue_id  integer;
    x_consumer_id integer;
    queue integer;
    sub record;
begin
    select queue_id into x_queue_id from pgque.queue
        where queue_name = x_queue_name;
    if not found then
        raise exception 'Event queue not created yet';
    end if;

    -- get consumer and create if new
    select co_id into x_consumer_id from pgque.consumer
        where co_name = x_consumer_name
        for update;
    if not found then
        insert into pgque.consumer (co_name) values (x_consumer_name);
        x_consumer_id := currval('pgque.consumer_co_id_seq');
    end if;

    -- if particular tick was requested, check if it exists
    if x_tick_pos is not null then
        perform 1 from pgque.tick
            where tick_queue = x_queue_id
              and tick_id = x_tick_pos;
        if not found then
            raise exception 'cannot reposition, tick not found: %', x_tick_pos;
        end if;
    end if;

    -- check if already registered
    select sub_last_tick, sub_batch into sub
        from pgque.subscription
        where sub_consumer = x_consumer_id
          and sub_queue  = x_queue_id;
    if found then
        if x_tick_pos is not null then
            -- if requested, update tick pos and drop partial batch
            update pgque.subscription
                set sub_last_tick = x_tick_pos,
                    sub_batch = null,
                    sub_next_tick = null,
                    sub_active = now()
                where sub_consumer = x_consumer_id
                  and sub_queue = x_queue_id;
        end if;
        -- already registered
        return 0;
    end if;

    --  new registration
    if x_tick_pos is null then
        -- start from current tick
        select tick_id into last_tick from pgque.tick
            where tick_queue = x_queue_id
            order by tick_queue desc, tick_id desc
            limit 1;
        if not found then
            raise exception 'No ticks for this queue.  Please run ticker on database.';
        end if;
    else
        last_tick := x_tick_pos;
    end if;

    -- register
    insert into pgque.subscription (sub_queue, sub_consumer, sub_last_tick)
        values (x_queue_id, x_consumer_id, last_tick);
    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.unregister_consumer(
    x_queue_name text,
    x_consumer_name text)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.unregister_consumer(2)
--
--      Unsubscribe consumer from the queue.
--      Also consumer's retry events are deleted.
--
-- Parameters:
--      x_queue_name        - Name of the queue
--      x_consumer_name     - Name of the consumer
--
-- Returns:
--      number of (sub)consumers unregistered
-- Calls:
--      None
-- Tables directly manipulated:
--      delete - pgque.retry_queue
--      delete - pgque.subscription
-- ----------------------------------------------------------------------
declare
    x_sub_id integer;
    _sub_id_cnt integer;
    _consumer_id integer;
    _is_subconsumer boolean;
begin
    select s.sub_id, c.co_id,
           -- subconsumers can only have both null or both not null - main consumer for subconsumers has only one not null
           (s.sub_last_tick IS NULL AND s.sub_next_tick IS NULL) OR (s.sub_last_tick IS NOT NULL AND s.sub_next_tick IS NOT NULL)
      into x_sub_id, _consumer_id, _is_subconsumer
      from pgque.subscription s, pgque.consumer c, pgque.queue q
     where s.sub_queue = q.queue_id
       and s.sub_consumer = c.co_id
       and q.queue_name = x_queue_name
       and c.co_name = x_consumer_name
       for update of s, c;
    if not found then
        return 0;
    end if;

    -- consumer + subconsumer count
    select count(*) into _sub_id_cnt
        from pgque.subscription
       where sub_id = x_sub_id;

    -- delete only one subconsumer
    if _sub_id_cnt > 1 and _is_subconsumer then
        delete from pgque.subscription
              where sub_id = x_sub_id
                and sub_consumer = _consumer_id;
        return 1;
    else
        -- delete main consumer (including possible subconsumers)

        -- retry events
        delete from pgque.retry_queue
            where ev_owner = x_sub_id;

        -- this will drop subconsumers too
        delete from pgque.subscription
            where sub_id = x_sub_id;

        perform 1 from pgque.subscription
            where sub_consumer = _consumer_id;
        if not found then
            delete from pgque.consumer
                where co_id = _consumer_id;
        end if;

        return _sub_id_cnt;
    end if;

end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.next_batch_info(
    in i_queue_name text,
    in i_consumer_name text,
    out batch_id int8,
    out cur_tick_id int8,
    out prev_tick_id int8,
    out cur_tick_time timestamptz,
    out prev_tick_time timestamptz,
    out cur_tick_event_seq int8,
    out prev_tick_event_seq int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgque.next_batch_info(2)
--
--      Makes next block of events active.
--
--      If it returns NULL, there is no events available in queue.
--      Consumer should sleep then.
--
--      The values from event_id sequence may give hint how big the
--      batch may be.  But they are inexact, they do not give exact size.
--      Client *MUST NOT* use them to detect whether the batch contains any
--      events at all - the values are unfit for that purpose.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--      i_consumer_name     - Name of the consumer
--
-- Returns:
--      batch_id            - Batch ID or NULL if there are no more events available.
--      cur_tick_id         - End tick id.
--      cur_tick_time       - End tick time.
--      cur_tick_event_seq  - Value from event id sequence at the time tick was issued.
--      prev_tick_id        - Start tick id.
--      prev_tick_time      - Start tick time.
--      prev_tick_event_seq - value from event id sequence at the time tick was issued.
-- Calls:
--      pgque.next_batch_custom(5)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
begin
    select f.batch_id, f.cur_tick_id, f.prev_tick_id,
           f.cur_tick_time, f.prev_tick_time,
           f.cur_tick_event_seq, f.prev_tick_event_seq
        into batch_id, cur_tick_id, prev_tick_id, cur_tick_time, prev_tick_time,
             cur_tick_event_seq, prev_tick_event_seq
        from pgque.next_batch_custom(i_queue_name, i_consumer_name, NULL, NULL, NULL) f;
    return;
end;
$$ language plpgsql;

create or replace function pgque.next_batch(
    in i_queue_name text,
    in i_consumer_name text)
returns int8 as $$
-- ----------------------------------------------------------------------
-- Function: pgque.next_batch(2)
--
--      Old function that returns just batch_id.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--      i_consumer_name     - Name of the consumer
--
-- Returns:
--      Batch ID or NULL if there are no more events available.
-- ----------------------------------------------------------------------
declare
    res int8;
begin
    select batch_id into res
        from pgque.next_batch_info(i_queue_name, i_consumer_name);
    return res;
end;
$$ language plpgsql;

create or replace function pgque.next_batch_custom(
    in i_queue_name text,
    in i_consumer_name text,
    in i_min_lag interval,
    in i_min_count int4,
    in i_min_interval interval,
    out batch_id int8,
    out cur_tick_id int8,
    out prev_tick_id int8,
    out cur_tick_time timestamptz,
    out prev_tick_time timestamptz,
    out cur_tick_event_seq int8,
    out prev_tick_event_seq int8)
as $$
-- ----------------------------------------------------------------------
-- Function: pgque.next_batch_custom(5)
--
--      Makes next block of events active.  Block size can be tuned
--      with i_min_count, i_min_interval parameters.  Events age can
--      be tuned with i_min_lag.
--
--      If it returns NULL, there is no events available in queue.
--      Consumer should sleep then.
--
--      The values from event_id sequence may give hint how big the
--      batch may be.  But they are inexact, they do not give exact size.
--      Client *MUST NOT* use them to detect whether the batch contains any
--      events at all - the values are unfit for that purpose.
--
-- Note:
--      i_min_lag together with i_min_interval/i_min_count is inefficient.
--
-- Parameters:
--      i_queue_name        - Name of the queue
--      i_consumer_name     - Name of the consumer
--      i_min_lag           - Consumer wants events older than that
--      i_min_count         - Consumer wants batch to contain at least this many events
--      i_min_interval      - Consumer wants batch to cover at least this much time
--
-- Returns:
--      batch_id            - Batch ID or NULL if there are no more events available.
--      cur_tick_id         - End tick id.
--      cur_tick_time       - End tick time.
--      cur_tick_event_seq  - Value from event id sequence at the time tick was issued.
--      prev_tick_id        - Start tick id.
--      prev_tick_time      - Start tick time.
--      prev_tick_event_seq - value from event id sequence at the time tick was issued.
-- Calls:
--      pgque.insert_event_raw(11)
-- Tables directly manipulated:
--      update - pgque.subscription
-- ----------------------------------------------------------------------
declare
    errmsg          text;
    queue_id        integer;
    sub_id          integer;
    cons_id         integer;
begin
    select s.sub_queue, s.sub_consumer, s.sub_id, s.sub_batch,
            t1.tick_id, t1.tick_time, t1.tick_event_seq,
            t2.tick_id, t2.tick_time, t2.tick_event_seq
        into queue_id, cons_id, sub_id, batch_id,
             prev_tick_id, prev_tick_time, prev_tick_event_seq,
             cur_tick_id, cur_tick_time, cur_tick_event_seq
        from pgque.consumer c,
             pgque.queue q,
             pgque.subscription s
             left join pgque.tick t1
                on (t1.tick_queue = s.sub_queue
                    and t1.tick_id = s.sub_last_tick)
             left join pgque.tick t2
                on (t2.tick_queue = s.sub_queue
                    and t2.tick_id = s.sub_next_tick)
        where q.queue_name = i_queue_name
          and c.co_name = i_consumer_name
          and s.sub_queue = q.queue_id
          and s.sub_consumer = c.co_id;
    if not found then
        errmsg := 'Not subscriber to queue: '
            || coalesce(i_queue_name, 'NULL')
            || '/'
            || coalesce(i_consumer_name, 'NULL');
        raise exception '%', errmsg;
    end if;

    -- sanity check
    if prev_tick_id is null then
        raise exception 'PgQ corruption: Consumer % on queue % does not see tick %', i_consumer_name, i_queue_name, prev_tick_id;
    end if;

    -- has already active batch
    if batch_id is not null then
        return;
    end if;

    if i_min_interval is null and i_min_count is null then
        -- find next tick
        select tick_id, tick_time, tick_event_seq
            into cur_tick_id, cur_tick_time, cur_tick_event_seq
            from pgque.tick
            where tick_id > prev_tick_id
              and tick_queue = queue_id
            order by tick_queue asc, tick_id asc
            limit 1;
    else
        -- find custom tick
        select next_tick_id, next_tick_time, next_tick_seq
          into cur_tick_id, cur_tick_time, cur_tick_event_seq
          from pgque.find_tick_helper(queue_id, prev_tick_id,
                                    prev_tick_time, prev_tick_event_seq,
                                    i_min_count, i_min_interval);
    end if;

    if i_min_lag is not null then
        -- enforce min lag
        if now() - cur_tick_time < i_min_lag then
            cur_tick_id := NULL;
            cur_tick_time := NULL;
            cur_tick_event_seq := NULL;
        end if;
    end if;

    if cur_tick_id is null then
        -- nothing to do
        prev_tick_id := null;
        prev_tick_time := null;
        prev_tick_event_seq := null;
        return;
    end if;

    -- get next batch
    batch_id := nextval('pgque.batch_id_seq');
    update pgque.subscription
        set sub_batch = batch_id,
            sub_next_tick = cur_tick_id,
            sub_active = now()
        where sub_queue = queue_id
          and sub_consumer = cons_id;
    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.get_batch_events(
    in x_batch_id   bigint,
    out ev_id       bigint,
    out ev_time     timestamptz,
    out ev_txid     bigint,
    out ev_retry    int4,
    out ev_type     text,
    out ev_data     text,
    out ev_extra1   text,
    out ev_extra2   text,
    out ev_extra3   text,
    out ev_extra4   text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_batch_events(1)
--
--      Get all events in batch.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--
-- Returns:
--      List of events.
-- ----------------------------------------------------------------------
declare
    sql text;
begin
    sql := pgque.batch_event_sql(x_batch_id);
    for ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4
        in execute sql
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql; -- no perms needed


create or replace function pgque.get_batch_cursor(
    in i_batch_id       bigint,
    in i_cursor_name    text,
    in i_quick_limit    int4,
    in i_extra_where    text,

    out ev_id       bigint,
    out ev_time     timestamptz,
    out ev_txid     bigint,
    out ev_retry    int4,
    out ev_type     text,
    out ev_data     text,
    out ev_extra1   text,
    out ev_extra2   text,
    out ev_extra3   text,
    out ev_extra4   text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_batch_cursor(4)
--
--      Get events in batch using a cursor.
--
-- Parameters:
--      i_batch_id      - ID of active batch.
--      i_cursor_name   - Name for new cursor
--      i_quick_limit   - Number of events to return immediately
--      i_extra_where   - optional where clause to filter events
--
-- Returns:
--      List of events.
-- Calls:
--      pgque.batch_event_sql(i_batch_id) - internal function which generates SQL optimised specially for getting events in this batch
-- ----------------------------------------------------------------------
declare
    _cname  text;
    _sql    text;
begin
    if i_batch_id is null or i_cursor_name is null or i_quick_limit is null then
        return;
    end if;

    _cname := quote_ident(i_cursor_name);
    _sql := pgque.batch_event_sql(i_batch_id);

    -- apply extra where
    if i_extra_where is not null then
        _sql := replace(_sql, ' order by 1', '');
        _sql := 'select * from (' || _sql
            || ') _evs where ' || i_extra_where
            || ' order by 1';
    end if;

    -- create cursor
    execute 'declare ' || _cname || ' no scroll cursor for ' || _sql;

    -- if no events wanted, don't bother with execute
    if i_quick_limit <= 0 then
        return;
    end if;

    -- return first block of events
    for ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4
        in execute 'fetch ' || i_quick_limit::text || ' from ' || _cname
    loop
        return next;
    end loop;

    return;
end;
$$ language plpgsql; -- no perms needed

create or replace function pgque.get_batch_cursor(
    in i_batch_id       bigint,
    in i_cursor_name    text,
    in i_quick_limit    int4,

    out ev_id       bigint,
    out ev_time     timestamptz,
    out ev_txid     bigint,
    out ev_retry    int4,
    out ev_type     text,
    out ev_data     text,
    out ev_extra1   text,
    out ev_extra2   text,
    out ev_extra3   text,
    out ev_extra4   text)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_batch_cursor(3)
--
--      Get events in batch using a cursor.
--
-- Parameters:
--      i_batch_id      - ID of active batch.
--      i_cursor_name   - Name for new cursor
--      i_quick_limit   - Number of events to return immediately
--
-- Returns:
--      List of events.
-- Calls:
--      pgque.get_batch_cursor(4)
-- ----------------------------------------------------------------------
begin
    for ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4
    in
        select * from pgque.get_batch_cursor(i_batch_id,
            i_cursor_name, i_quick_limit, null)
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql strict; -- no perms needed

create or replace function pgque.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_time timestamptz)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.event_retry(3a)
--
--     Put the event into retry queue, to be processed again later.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - event id
--      x_retry_time    - Time when the event should be put back into queue
--
-- Returns:
--     1 - success
--     0 - event already in retry queue
-- Calls:
--      None
-- Tables directly manipulated:
--      insert - pgque.retry_queue
-- ----------------------------------------------------------------------
begin
    insert into pgque.retry_queue (ev_retry_after, ev_queue,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    select x_retry_time, sub_queue,
           ev_id, ev_time, NULL, sub_id, coalesce(ev_retry, 0) + 1,
           ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4
      from pgque.get_batch_events(x_batch_id),
           pgque.subscription
     where sub_batch = x_batch_id
       and ev_id = x_event_id;
    if not found then
        raise exception 'event not found';
    end if;
    return 1;

-- dont worry if the event is already in queue
exception
    when unique_violation then
        return 0;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.event_retry(
    x_batch_id bigint,
    x_event_id bigint,
    x_retry_seconds integer)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.event_retry(3b)
--
--     Put the event into retry queue, to be processed later again.
--
-- Parameters:
--      x_batch_id      - ID of active batch.
--      x_event_id      - event id
--      x_retry_seconds - Time when the event should be put back into queue
--
-- Returns:
--     1 - success
--     0 - event already in retry queue
-- Calls:
--      pgque.event_retry(3a)
-- Tables directly manipulated:
--      None
-- ----------------------------------------------------------------------
declare
    new_retry  timestamptz;
begin
    new_retry := current_timestamp + ((x_retry_seconds::text || ' seconds')::interval);
    return pgque.event_retry(x_batch_id, x_event_id, new_retry);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.batch_retry(
    i_batch_id bigint,
    i_retry_seconds integer)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.batch_retry(2)
--
--     Put whole batch into retry queue, to be processed again later.
--
-- Parameters:
--      i_batch_id      - ID of active batch.
--      i_retry_time    - Time when the event should be put back into queue
--
-- Returns:
--     number of events inserted
-- Calls:
--      None
-- Tables directly manipulated:
--      pgque.retry_queue
-- ----------------------------------------------------------------------
declare
    _retry timestamptz;
    _cnt   integer;
    _s     record;
begin
    _retry := current_timestamp + ((i_retry_seconds::text || ' seconds')::interval);

    select * into _s from pgque.subscription where sub_batch = i_batch_id;
    if not found then
        raise exception 'batch_retry: batch % not found', i_batch_id;
    end if;

    insert into pgque.retry_queue (ev_retry_after, ev_queue,
        ev_id, ev_time, ev_txid, ev_owner, ev_retry,
        ev_type, ev_data, ev_extra1, ev_extra2,
        ev_extra3, ev_extra4)
    select distinct _retry, _s.sub_queue,
           b.ev_id, b.ev_time, NULL::int8, _s.sub_id, coalesce(b.ev_retry, 0) + 1,
           b.ev_type, b.ev_data, b.ev_extra1, b.ev_extra2,
           b.ev_extra3, b.ev_extra4
      from pgque.get_batch_events(i_batch_id) b
           left join pgque.retry_queue rq
                  on (rq.ev_id = b.ev_id
                      and rq.ev_owner = _s.sub_id
                      and rq.ev_queue = _s.sub_queue)
      where rq.ev_id is null;

    GET DIAGNOSTICS _cnt = ROW_COUNT;
    return _cnt;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.finish_batch(
    x_batch_id bigint)
returns integer as $$
-- ----------------------------------------------------------------------
-- Function: pgque.finish_batch(1)
--
--      Closes a batch.  No more operations can be done with events
--      of this batch.
--
-- Parameters:
--      x_batch_id      - id of batch.
--
-- Returns:
--      1 if batch was found, 0 otherwise.
-- Calls:
--      None
-- Tables directly manipulated:
--      update - pgque.subscription
-- ----------------------------------------------------------------------
begin
    update pgque.subscription
        set sub_active = now(),
            sub_last_tick = sub_next_tick,
            sub_next_tick = null,
            sub_batch = null
        where sub_batch = x_batch_id;
    if not found then
        raise warning 'finish_batch: batch % not found', x_batch_id;
        return 0;
    end if;

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

drop function if exists pgque.get_queue_info();
drop function if exists pgque.get_queue_info(text);

create or replace function pgque.get_queue_info(
    out queue_name                  text,
    out queue_ntables               integer,
    out queue_cur_table             integer,
    out queue_rotation_period       interval,
    out queue_switch_time           timestamptz,
    out queue_external_ticker       boolean,
    out queue_ticker_paused         boolean,
    out queue_ticker_max_count      integer,
    out queue_ticker_max_lag        interval,
    out queue_ticker_idle_period    interval,
    out ticker_lag                  interval,
    out ev_per_sec                  float8,
    out ev_new                      bigint,
    out last_tick_id                bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_queue_info(0)
--
--      Get info about all queues.
--
-- Returns:
--      List of pgque.ret_queue_info records.
--     queue_name                  - queue name
--     queue_ntables               - number of tables in this queue
--     queue_cur_table             - ???
--     queue_rotation_period       - how often the event_N_M tables in this queue are rotated
--     queue_switch_time           - ??? when was this queue last rotated
--     queue_external_ticker       - ???
--     queue_ticker_paused         - ??? is ticker paused in this queue
--     queue_ticker_max_count      - max number of events before a tick is issued
--     queue_ticker_max_lag        - maks time without a tick
--     queue_ticker_idle_period    - how often the ticker should check this queue
--     ticker_lag                  - time from last tick
--     ev_per_sec                  - how many events per second this queue serves
--     ev_new                      - ???
--     last_tick_id                - last tick id for this queue
--
-- ----------------------------------------------------------------------
begin
    for queue_name, queue_ntables, queue_cur_table, queue_rotation_period,
        queue_switch_time, queue_external_ticker, queue_ticker_paused,
        queue_ticker_max_count, queue_ticker_max_lag, queue_ticker_idle_period,
        ticker_lag, ev_per_sec, ev_new, last_tick_id
    in select
        f.queue_name, f.queue_ntables, f.queue_cur_table, f.queue_rotation_period,
        f.queue_switch_time, f.queue_external_ticker, f.queue_ticker_paused,
        f.queue_ticker_max_count, f.queue_ticker_max_lag, f.queue_ticker_idle_period,
        f.ticker_lag, f.ev_per_sec, f.ev_new, f.last_tick_id
        from pgque.get_queue_info(null) f
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql;

create or replace function pgque.get_queue_info(
    in i_queue_name                 text,
    out queue_name                  text,
    out queue_ntables               integer,
    out queue_cur_table             integer,
    out queue_rotation_period       interval,
    out queue_switch_time           timestamptz,
    out queue_external_ticker       boolean,
    out queue_ticker_paused         boolean,
    out queue_ticker_max_count      integer,
    out queue_ticker_max_lag        interval,
    out queue_ticker_idle_period    interval,
    out ticker_lag                  interval,
    out ev_per_sec                  float8,
    out ev_new                      bigint,
    out last_tick_id                bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_queue_info(1)
--
--      Get info about particular queue.
--
-- Returns:
--      One pgque.ret_queue_info record.
--      contente same as forpgque.get_queue_info() 
-- ----------------------------------------------------------------------
declare
    _ticker_lag interval;
    _top_tick_id bigint;
    _ht_tick_id bigint;
    _top_tick_time timestamptz;
    _top_tick_event_seq bigint;
    _ht_tick_time timestamptz;
    _ht_tick_event_seq bigint;
    _queue_id integer;
    _queue_event_seq text;
begin
    for queue_name, queue_ntables, queue_cur_table, queue_rotation_period,
        queue_switch_time, queue_external_ticker, queue_ticker_paused,
        queue_ticker_max_count, queue_ticker_max_lag, queue_ticker_idle_period,
        _queue_id, _queue_event_seq
    in select
        q.queue_name, q.queue_ntables, q.queue_cur_table,
        q.queue_rotation_period, q.queue_switch_time,
        q.queue_external_ticker, q.queue_ticker_paused,
        q.queue_ticker_max_count, q.queue_ticker_max_lag,
        q.queue_ticker_idle_period,
        q.queue_id, q.queue_event_seq
        from pgque.queue q
        where (i_queue_name is null or q.queue_name = i_queue_name)
        order by q.queue_name
    loop
        -- most recent tick
        select (current_timestamp - t.tick_time),
               tick_id, t.tick_time, t.tick_event_seq
            into ticker_lag, _top_tick_id, _top_tick_time, _top_tick_event_seq
            from pgque.tick t
            where t.tick_queue = _queue_id
            order by t.tick_queue desc, t.tick_id desc
            limit 1;
        -- slightly older tick
        select ht.tick_id, ht.tick_time, ht.tick_event_seq
            into _ht_tick_id, _ht_tick_time, _ht_tick_event_seq
            from pgque.tick ht
            where ht.tick_queue = _queue_id
             and ht.tick_id >= _top_tick_id - 20
            order by ht.tick_queue asc, ht.tick_id asc
            limit 1;
        if _ht_tick_time < _top_tick_time then
            ev_per_sec = (_top_tick_event_seq - _ht_tick_event_seq) / extract(epoch from (_top_tick_time - _ht_tick_time));
        else
            ev_per_sec = null;
        end if;
        ev_new = pgque.seq_getval(_queue_event_seq) - _top_tick_event_seq;
        last_tick_id = _top_tick_id;
        return next;
    end loop;
    return;
end;
$$ language plpgsql;


create or replace function pgque.get_consumer_info(
    out queue_name      text,
    out consumer_name   text,
    out lag             interval,
    out last_seen       interval,
    out last_tick       bigint,
    out current_batch   bigint,
    out next_tick       bigint,
    out pending_events  bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_consumer_info(0)
--
--      Returns info about all consumers on all queues.
--
-- Returns:
--      See pgque.get_consumer_info(2)
-- ----------------------------------------------------------------------
begin
    for queue_name, consumer_name, lag, last_seen,
        last_tick, current_batch, next_tick, pending_events
    in
        select f.queue_name, f.consumer_name, f.lag, f.last_seen,
               f.last_tick, f.current_batch, f.next_tick, f.pending_events
            from pgque.get_consumer_info(null, null) f
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;



create or replace function pgque.get_consumer_info(
    in i_queue_name     text,
    out queue_name      text,
    out consumer_name   text,
    out lag             interval,
    out last_seen       interval,
    out last_tick       bigint,
    out current_batch   bigint,
    out next_tick       bigint,
    out pending_events  bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_consumer_info(1)
--
--      Returns info about all consumers on single queue.
--
-- Returns:
--      See pgque.get_consumer_info(2)
-- ----------------------------------------------------------------------
begin
    for queue_name, consumer_name, lag, last_seen,
        last_tick, current_batch, next_tick, pending_events
    in
        select f.queue_name, f.consumer_name, f.lag, f.last_seen,
               f.last_tick, f.current_batch, f.next_tick, f.pending_events
            from pgque.get_consumer_info(i_queue_name, null) f
    loop
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;



create or replace function pgque.get_consumer_info(
    in i_queue_name     text,
    in i_consumer_name  text,
    out queue_name      text,
    out consumer_name   text,
    out lag             interval,
    out last_seen       interval,
    out last_tick       bigint,
    out current_batch   bigint,
    out next_tick       bigint,
    out pending_events  bigint)
returns setof record as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_consumer_info(2)
--
--      Get info about particular consumer on particular queue.
--
-- Parameters:
--      i_queue_name        - name of a queue. (null = all)
--      i_consumer_name     - name of a consumer (null = all)
--
-- Returns:
--      queue_name          - Queue name
--      consumer_name       - Consumer name
--      lag                 - How old are events the consumer is processing
--      last_seen           - When the consumer seen by pgq
--      last_tick           - Tick ID of last processed tick
--      current_batch       - Current batch ID, if one is active or NULL
--      next_tick           - If batch is active, then its final tick.
-- ----------------------------------------------------------------------
declare
    _pending_events bigint;
    _queue_id bigint;
begin
    for queue_name, consumer_name, lag, last_seen,
        last_tick, current_batch, next_tick, _pending_events, _queue_id
    in
        select q.queue_name, c.co_name,
               current_timestamp - t.tick_time,
               current_timestamp - s.sub_active,
               s.sub_last_tick, s.sub_batch, s.sub_next_tick,
               t.tick_event_seq, q.queue_id
          from pgque.queue q,
               pgque.consumer c,
               pgque.subscription s
               left join pgque.tick t
                 on (t.tick_queue = s.sub_queue and t.tick_id = s.sub_last_tick)
         where q.queue_id = s.sub_queue
           and c.co_id = s.sub_consumer
           and (i_queue_name is null or q.queue_name = i_queue_name)
           and (i_consumer_name is null or c.co_name = i_consumer_name)
         order by 1,2
    loop
        select t.tick_event_seq - _pending_events
            into pending_events
            from pgque.tick t
            where t.tick_queue = _queue_id
            order by t.tick_queue desc, t.tick_id desc
            limit 1;
        return next;
    end loop;
    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;


create or replace function pgque.get_batch_info(
    in x_batch_id       bigint,
    out queue_name      text,
    out consumer_name   text,
    out batch_start     timestamptz,
    out batch_end       timestamptz,
    out prev_tick_id    bigint,
    out tick_id         bigint,
    out lag             interval,
    out seq_start       bigint,
    out seq_end         bigint)
as $$
-- ----------------------------------------------------------------------
-- Function: pgque.get_batch_info(1)
--
--      Returns detailed info about a batch.
--
-- Parameters:
--      x_batch_id      - id of a active batch.
--
-- Returns: ??? pls check
--      queue_name      - which queue this batch came from
--      consumer_name   - batch processed by
--      batch_start     - start time of batch
--      batch_end       - end time of batch
--      prev_tick_id    - start tick for this batch
--      tick_id         - end tick for this batch
--      lag             - now() - tick_id.time 
--      seq_start       - start event id for batch
--      seq_end         - end event id for batch
-- ----------------------------------------------------------------------
begin
    select q.queue_name, c.co_name,
           prev.tick_time, cur.tick_time,
           s.sub_last_tick, s.sub_next_tick,
           current_timestamp - cur.tick_time,
           prev.tick_event_seq, cur.tick_event_seq
        into queue_name, consumer_name, batch_start, batch_end,
             prev_tick_id, tick_id, lag, seq_start, seq_end
        from pgque.subscription s, pgque.tick cur, pgque.tick prev,
             pgque.queue q, pgque.consumer c
        where s.sub_batch = x_batch_id
          and prev.tick_id = s.sub_last_tick
          and prev.tick_queue = s.sub_queue
          and cur.tick_id = s.sub_next_tick
          and cur.tick_queue = s.sub_queue
          and q.queue_id = s.sub_queue
          and c.co_id = s.sub_consumer;
    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ======================================================================
-- Section 3: PL/pgSQL event insertion (derived from PgQ)
-- Origin: pgq/lowlevel_pl/insert_event.sql
-- PgQue transformations: schema rename, txid→pg_* renames, search_path
-- ======================================================================


-- ----------------------------------------------------------------------
-- Function: pgque.insert_event_raw(11)
--
--      Actual event insertion.  Used also by retry queue maintenance.
--
-- Parameters:
--      queue_name      - Name of the queue
--      ev_id           - Event ID.  If NULL, will be taken from seq.
--      ev_time         - Event creation time.
--      ev_owner        - Subscription ID when retry event. If NULL, the event is for everybody.
--      ev_retry        - Retry count. NULL for first-time events.
--      ev_type         - user data
--      ev_data         - user data
--      ev_extra1       - user data
--      ev_extra2       - user data
--      ev_extra3       - user data
--      ev_extra4       - user data
--
-- Returns:
--      Event ID.
-- ----------------------------------------------------------------------
create or replace function pgque.insert_event_raw(
    queue_name text, ev_id bigint, ev_time timestamptz,
    ev_owner integer, ev_retry integer, ev_type text, ev_data text,
    ev_extra1 text, ev_extra2 text, ev_extra3 text, ev_extra4 text)
returns int8 as $$
declare
    qstate record;
    _qname text;
begin
    _qname := queue_name;
    select q.queue_id,
        pgque.quote_fqname(q.queue_data_pfx || '_' || q.queue_cur_table::text) as cur_table_name,
        nextval(q.queue_event_seq) as next_ev_id,
        q.queue_disable_insert
    from pgque.queue q where q.queue_name = _qname into qstate;

    if ev_id is null then
        ev_id := qstate.next_ev_id;
    end if;

    if qstate.queue_disable_insert then
        if current_setting('session_replication_role') <> 'replica' then
            raise exception 'Insert into queue disallowed';
        end if;
    end if;

    execute 'insert into ' || qstate.cur_table_name
        || ' (ev_id, ev_time, ev_owner, ev_retry,'
        || ' ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4)'
        || 'values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)'
        using ev_id, ev_time, ev_owner, ev_retry,
              ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4;

    return ev_id;
end;
$$ language plpgsql;

-- ======================================================================
-- Section 4: Trigger functions (derived from PgQ)
-- Origin: pgq/lowlevel_pl/{jsontriga,logutriga,sqltriga}.sql
-- PgQue transformations: schema rename, search_path hardening
-- ======================================================================

create or replace function pgque.jsontriga() returns trigger as $$
-- ----------------------------------------------------------------------
-- Function: pgque.jsontriga()
--
--      Trigger function that puts row data in JSON-encoded form into queue.
--
-- Purpose:
--      Convert row data into easily parseable form.
--
-- Trigger parameters:
--      arg1 - queue name
--      argX - any number of optional arg, in any order
--
-- Optional arguments:
--      SKIP                - The actual operation should be skipped (BEFORE trigger)
--      ignore=col1[,col2]  - don't look at the specified arguments
--      pkey=col1[,col2]    - Set pkey fields for the table, autodetection will be skipped
--      backup              - Put urlencoded contents of old row to ev_extra2
--      colname=EXPR        - Override field value with SQL expression.  Can reference table
--                            columns.  colname can be: ev_type, ev_data, ev_extra1 .. ev_extra4
--      when=EXPR           - If EXPR returns false, don't insert event.
--
-- Queue event fields:
--      ev_type      - I/U/D ':' pkey_column_list
--      ev_data      - column values urlencoded
--      ev_extra1    - table name
--      ev_extra2    - optional urlencoded backup
--
-- Regular listen trigger example:
-- >   CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgque.jsontriga('qname');
--
-- Redirect trigger example:
-- >   CREATE TRIGGER triga_nimi BEFORE INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgque.jsontriga('qname', 'SKIP');
-- ----------------------------------------------------------------------
declare
    qname text;
    ev_type text;
    ev_data text;
    ev_extra1 text;
    ev_extra2 text;
    ev_extra3 text;
    ev_extra4 text;
    do_skip boolean := false;
    do_backup boolean := false;
    do_insert boolean := true;
    do_deny boolean := false;
    extra_ignore_list text[];
    full_ignore_list text[];
    ignore_list text[] := '{}';
    pkey_list text[];
    pkey_str text;
    field_sql_sfx text;
    field_sql text[] := '{}';
    data_sql text;
    ignore_col_changes int4 := 0;
begin
    if TG_NARGS < 1 then
        raise exception 'Trigger needs queue name';
    end if;
    qname := TG_ARGV[0];

    -- standard output
    ev_extra1 := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;

    -- prepare to handle magic fields
    field_sql_sfx := ')::text as val from (select $1.*) r';
    extra_ignore_list := array['_pgq_ev_type', '_pgq_ev_extra1', '_pgq_ev_extra2',
                               '_pgq_ev_extra3', '_pgq_ev_extra4']::text[];

    -- parse trigger args
    declare
        got boolean;
        argpair text[];
        i integer;
    begin
        for i in 1 .. TG_NARGS-1 loop
            if TG_ARGV[i] in ('skip', 'SKIP') then
                do_skip := true;
            elsif TG_ARGV[i] = 'backup' then
                do_backup := true;
            elsif TG_ARGV[i] = 'deny' then
                do_deny := true;
            else
                got := false;
                for argpair in select regexp_matches(TG_ARGV[i], '^([^=]+)=(.*)') loop
                    got := true;
                    if argpair[1] = 'pkey' then
                        pkey_str := argpair[2];
                        pkey_list := string_to_array(pkey_str, ',');
                    elsif argpair[1] = 'ignore' then
                        ignore_list := string_to_array(argpair[2], ',');
                    elsif argpair[1] ~ '^ev_(type|extra[1-4])$' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (' || argpair[2] || field_sql_sfx);
                    elsif argpair[1] = 'when' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (case when (' || argpair[2]
                                                  || ')::boolean then ''proceed'' else null end' || field_sql_sfx);
                    else
                        got := false;
                    end if;
                end loop;
                if not got then
                    raise exception 'bad argument: %', TG_ARGV[i];
                end if;
            end if;
        end loop;
    end;

    full_ignore_list := ignore_list || extra_ignore_list;

    if pkey_str is null then
        select array_agg(pk.attname)
            from (select k.attname from pg_index i, pg_attribute k
                    where i.indrelid = TG_RELID
                        and k.attrelid = i.indexrelid and i.indisprimary
                        and k.attnum > 0 and not k.attisdropped
                    order by k.attnum) pk
            into pkey_list;
        if pkey_list is null then
            pkey_list := '{}';
            pkey_str := '';
        else
            pkey_str := array_to_string(pkey_list, ',');
        end if;
    end if;
    if pkey_str = '' and TG_OP in ('UPDATE', 'DELETE') then
        raise exception 'Update/Delete on table without pkey';
    end if;

    if TG_OP not in ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE') then
        raise exception 'TG_OP not supported: %', TG_OP;
    end if;

    -- fill ev_type
    select to_json(t.*)::text
        from (select TG_OP as op, array[TG_TABLE_SCHEMA,TG_TABLE_NAME] as "table", pkey_list as "pkey") t
        into ev_type;

    -- early exit?
    if current_setting('session_replication_role') = 'local' then
        if TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
            return null;
        elsif TG_OP = 'DELETE' then
            return OLD;
        else
            return NEW;
        end if;
    elsif do_deny then
        raise exception 'Table ''%.%'' to queue ''%'': change not allowed (%)',
                    TG_TABLE_SCHEMA, TG_TABLE_NAME, qname, TG_OP;
    elsif TG_OP = 'TRUNCATE' then
        perform pgque.insert_event(qname, ev_type, '{}', ev_extra1, ev_extra2, ev_extra3, ev_extra4);
        return null;
    end if;

    -- process table columns
    declare
        attr record;
        pkey_sql_buf text[];
        qcol text;
        data_sql_buf text[];
        ignore_sql text;
        ignore_sql_buf text[];
        pkey_change_sql text;
        pkey_col_changes int4 := 0;
        valexp text;
    begin
        for attr in
            select k.attnum, k.attname, k.atttypid
                from pg_attribute k
                where k.attrelid = TG_RELID and k.attnum > 0 and not k.attisdropped
                order by k.attnum
        loop
            qcol := quote_ident(attr.attname);
            if attr.attname = any (ignore_list) then
                ignore_sql_buf := array_append(ignore_sql_buf,
                    'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
                continue;
            elsif attr.attname = any (extra_ignore_list) then
                field_sql := array_prepend('select ' || quote_literal(substring(attr.attname from 6))
                                           || '::text as key, (r.' || qcol || field_sql_sfx, field_sql);
                continue;
            end if;

            -- force cast to text or not
            if attr.atttypid in ('timestamptz'::regtype::oid, 'timestamp'::regtype::oid,
                    'int8'::regtype::oid, 'int4'::regtype::oid, 'int2'::regtype::oid,
                    'date'::regtype::oid, 'boolean'::regtype::oid) then
                valexp := 'to_json(r.' || qcol || ')::text';
            else
                valexp := 'to_json(r.' || qcol || '::text)::text';
            end if;

            if attr.attname = any (pkey_list) then
                pkey_sql_buf := array_append(pkey_sql_buf,
                        'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
            end if;

            data_sql_buf := array_append(data_sql_buf,
                    'select ' || quote_literal(to_json(attr.attname) || ':')
                    || ' || coalesce(' || valexp || ', ''null'') as jpair from (select $1.*) r');
        end loop;

        -- SQL to see if pkey columns have changed
        if TG_OP = 'UPDATE' then
            pkey_change_sql := 'select count(1) from (' || array_to_string(pkey_sql_buf, ' union all ')
                            || ') cols where cols.is_changed';
            execute pkey_change_sql using OLD, NEW into pkey_col_changes;
            if pkey_col_changes > 0 then
                raise exception 'primary key update not allowed';
            end if;
        end if;

        -- SQL to see if ignored columns have changed
        if TG_OP = 'UPDATE' and array_length(ignore_list, 1) is not null then
            ignore_sql := 'select count(1) from (' || array_to_string(ignore_sql_buf, ' union all ')
                || ') cols where cols.is_changed';
            execute ignore_sql using OLD, NEW into ignore_col_changes;
        end if;

        -- SQL to load data
        data_sql := 'select ''{'' || array_to_string(array_agg(cols.jpair), '','') || ''}'' from ('
                 || array_to_string(data_sql_buf, ' union all ') || ') cols';
    end;

    -- render data
    declare
        old_data text;
    begin
        if TG_OP = 'INSERT' then
            execute data_sql using NEW into ev_data;
        elsif TG_OP = 'UPDATE' then

            -- render NEW
            execute data_sql using NEW into ev_data;

            -- render OLD when needed
            if do_backup or array_length(ignore_list, 1) is not null then
                execute data_sql using OLD into old_data;
            end if;

            -- only change was to ignored columns?
            if old_data = ev_data and ignore_col_changes > 0 then
                do_insert := false;
            end if;

            -- is backup needed?
            if do_backup then
                ev_extra2 := old_data;
            end if;
        elsif TG_OP = 'DELETE' then
            execute data_sql using OLD into ev_data;
        end if;
    end;

    -- apply magic args and columns
    declare
        col text;
        val text;
        rmain record;
        sql text;
    begin
        if do_insert and array_length(field_sql, 1) is not null then
            if TG_OP = 'DELETE' then
                rmain := OLD;
            else
                rmain := NEW;
            end if;

            sql := array_to_string(field_sql, ' union all ');
            for col, val in
                execute sql using rmain
            loop
                if col = 'ev_type' then
                    ev_type := val;
                elsif col = 'ev_extra1' then
                    ev_extra1 := val;
                elsif col = 'ev_extra2' then
                    ev_extra2 := val;
                elsif col = 'ev_extra3' then
                    ev_extra3 := val;
                elsif col = 'ev_extra4' then
                    ev_extra4 := val;
                elsif col = 'when' then
                    if val is null then
                        do_insert := false;
                    end if;
                end if;
            end loop;
        end if;
    end;

    -- insert final values
    if do_insert then
        perform pgque.insert_event(qname, ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
    end if;

    if do_skip or TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
        return null;
    elsif TG_OP = 'DELETE' then
        return OLD;
    else
        return NEW;
    end if;
end;
$$ language plpgsql;

create or replace function pgque.logutriga() returns trigger as $$
-- ----------------------------------------------------------------------
-- Function: pgque.logutriga()
--
--      Trigger function that puts row data in urlencoded form into queue.
--
-- Purpose:
--	Used as producer for several PgQ standard consumers (cube_dispatcher, 
--      queue_mover, table_dispatcher).  Basically for cases where the
--      consumer wants to parse the event and look at the actual column values.
--
-- Trigger parameters:
--      arg1 - queue name
--      argX - any number of optional arg, in any order
--
-- Optional arguments:
--      SKIP                - The actual operation should be skipped (BEFORE trigger)
--      ignore=col1[,col2]  - don't look at the specified arguments
--      pkey=col1[,col2]    - Set pkey fields for the table, autodetection will be skipped
--      backup              - Put urlencoded contents of old row to ev_extra2
--      colname=EXPR        - Override field value with SQL expression.  Can reference table
--                            columns.  colname can be: ev_type, ev_data, ev_extra1 .. ev_extra4
--      when=EXPR           - If EXPR returns false, don't insert event.
--
-- Queue event fields:
--      ev_type      - I/U/D ':' pkey_column_list
--      ev_data      - column values urlencoded
--      ev_extra1    - table name
--      ev_extra2    - optional urlencoded backup
--
-- Regular listen trigger example:
-- >   CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgque.logutriga('qname');
--
-- Redirect trigger example:
-- >   CREATE TRIGGER triga_nimi BEFORE INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgque.logutriga('qname', 'SKIP');
-- ----------------------------------------------------------------------
declare
    qname text;
    ev_type text;
    ev_data text;
    ev_extra1 text;
    ev_extra2 text;
    ev_extra3 text;
    ev_extra4 text;
    do_skip boolean := false;
    do_backup boolean := false;
    do_insert boolean := true;
    do_deny boolean := false;
    extra_ignore_list text[];
    full_ignore_list text[];
    ignore_list text[] := '{}';
    pkey_list text[];
    pkey_str text;
    field_sql_sfx text;
    field_sql text[] := '{}';
    data_sql text;
    ignore_col_changes int4 := 0;
begin
    if TG_NARGS < 1 then
        raise exception 'Trigger needs queue name';
    end if;
    qname := TG_ARGV[0];

    -- standard output
    ev_extra1 := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;

    -- prepare to handle magic fields
    field_sql_sfx := ')::text as val from (select $1.*) r';
    extra_ignore_list := array['_pgq_ev_type', '_pgq_ev_extra1', '_pgq_ev_extra2',
                               '_pgq_ev_extra3', '_pgq_ev_extra4']::text[];

    -- parse trigger args
    declare
        got boolean;
        argpair text[];
        i integer;
    begin
        for i in 1 .. TG_NARGS-1 loop
            if TG_ARGV[i] in ('skip', 'SKIP') then
                do_skip := true;
            elsif TG_ARGV[i] = 'backup' then
                do_backup := true;
            elsif TG_ARGV[i] = 'deny' then
                do_deny := true;
            else
                got := false;
                for argpair in select regexp_matches(TG_ARGV[i], '^([^=]+)=(.*)') loop
                    got := true;
                    if argpair[1] = 'pkey' then
                        pkey_str := argpair[2];
                        pkey_list := string_to_array(pkey_str, ',');
                    elsif argpair[1] = 'ignore' then
                        ignore_list := string_to_array(argpair[2], ',');
                    elsif argpair[1] ~ '^ev_(type|extra[1-4])$' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (' || argpair[2] || field_sql_sfx);
                    elsif argpair[1] = 'when' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (case when (' || argpair[2]
                                                  || ')::boolean then ''proceed'' else null end' || field_sql_sfx);
                    else
                        got := false;
                    end if;
                end loop;
                if not got then
                    raise exception 'bad argument: %', TG_ARGV[i];
                end if;
            end if;
        end loop;
    end;

    full_ignore_list := ignore_list || extra_ignore_list;

    if pkey_str is null then
        select array_agg(pk.attname)
            from (select k.attname from pg_index i, pg_attribute k
                    where i.indrelid = TG_RELID
                        and k.attrelid = i.indexrelid and i.indisprimary
                        and k.attnum > 0 and not k.attisdropped
                    order by k.attnum) pk
            into pkey_list;
        if pkey_list is null then
            pkey_list := '{}';
            pkey_str := '';
        else
            pkey_str := array_to_string(pkey_list, ',');
        end if;
    end if;
    if pkey_str = '' and TG_OP in ('UPDATE', 'DELETE') then
        raise exception 'Update/Delete on table without pkey';
    end if;

    if TG_OP = 'INSERT' then
        ev_type := 'I:' || pkey_str;
    elsif TG_OP = 'UPDATE' then
        ev_type := 'U:' || pkey_str;
    elsif TG_OP = 'DELETE' then
        ev_type := 'D:' || pkey_str;
    elsif TG_OP = 'TRUNCATE' then
        ev_type := 'R';
    else
        raise exception 'TG_OP not supported: %', TG_OP;
    end if;

    if current_setting('session_replication_role') = 'local' then
        if TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
            return null;
        elsif TG_OP = 'DELETE' then
            return OLD;
        else
            return NEW;
        end if;
    elsif do_deny then
        raise exception 'Table ''%.%'' to queue ''%'': change not allowed (%)',
                    TG_TABLE_SCHEMA, TG_TABLE_NAME, qname, TG_OP;
    elsif TG_OP = 'TRUNCATE' then
        perform pgque.insert_event(qname, ev_type, '', ev_extra1, ev_extra2, ev_extra3, ev_extra4);
        return null;
    end if;

    -- process table columns
    declare
        attr record;
        pkey_sql_buf text[];
        qcol text;
        data_sql_buf text[];
        ignore_sql text;
        ignore_sql_buf text[];
        pkey_change_sql text;
        pkey_col_changes int4 := 0;
        valexp text;
    begin
        for attr in
            select k.attnum, k.attname, k.atttypid
                from pg_attribute k
                where k.attrelid = TG_RELID and k.attnum > 0 and not k.attisdropped
                order by k.attnum
        loop
            qcol := quote_ident(attr.attname);
            if attr.attname = any (ignore_list) then
                ignore_sql_buf := array_append(ignore_sql_buf,
                    'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
                continue;
            elsif attr.attname = any (extra_ignore_list) then
                field_sql := array_prepend('select ' || quote_literal(substring(attr.attname from 6))
                                           || '::text as key, (r.' || qcol || field_sql_sfx, field_sql);
                continue;
            end if;

            if attr.atttypid = 'boolean'::regtype::oid then
                valexp := 'case r.' || qcol || ' when true then ''t'' when false then ''f'' else null end';
            else
                valexp := 'r.' || qcol || '::text';
            end if;

            if attr.attname = any (pkey_list) then
                pkey_sql_buf := array_append(pkey_sql_buf,
                        'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
            end if;

            data_sql_buf := array_append(data_sql_buf,
                    'select pgque._urlencode(' || quote_literal(attr.attname)
                    || ') || coalesce(''='' || pgque._urlencode(' || valexp
                    || '), '''') as upair from (select $1.*) r');
        end loop;

        -- SQL to see if pkey columns have changed
        if TG_OP = 'UPDATE' then
            pkey_change_sql := 'select count(1) from (' || array_to_string(pkey_sql_buf, ' union all ')
                            || ') cols where cols.is_changed';
            execute pkey_change_sql using OLD, NEW into pkey_col_changes;
            if pkey_col_changes > 0 then
                raise exception 'primary key update not allowed';
            end if;
        end if;

        -- SQL to see if ignored columns have changed
        if TG_OP = 'UPDATE' and array_length(ignore_list, 1) is not null then
            ignore_sql := 'select count(1) from (' || array_to_string(ignore_sql_buf, ' union all ')
                || ') cols where cols.is_changed';
            execute ignore_sql using OLD, NEW into ignore_col_changes;
        end if;

        -- SQL to load data
        data_sql := 'select array_to_string(array_agg(cols.upair), ''&'') from ('
                 || array_to_string(data_sql_buf, ' union all ') || ') cols';
    end;

    -- render data
    declare
        old_data text;
    begin
        if TG_OP = 'INSERT' then
            execute data_sql using NEW into ev_data;
        elsif TG_OP = 'UPDATE' then

            -- render NEW
            execute data_sql using NEW into ev_data;

            -- render OLD when needed
            if do_backup or array_length(ignore_list, 1) is not null then
                execute data_sql using OLD into old_data;
            end if;

            -- only change was to ignored columns?
            if old_data = ev_data and ignore_col_changes > 0 then
                do_insert := false;
            end if;

            -- is backup needed?
            if do_backup then
                ev_extra2 := old_data;
            end if;
        elsif TG_OP = 'DELETE' then
            execute data_sql using OLD into ev_data;
        end if;
    end;

    -- apply magic args and columns
    declare
        col text;
        val text;
        rmain record;
        sql text;
    begin
        if do_insert and array_length(field_sql, 1) is not null then
            if TG_OP = 'DELETE' then
                rmain := OLD;
            else
                rmain := NEW;
            end if;

            sql := array_to_string(field_sql, ' union all ');
            for col, val in
                execute sql using rmain
            loop
                if col = 'ev_type' then
                    ev_type := val;
                elsif col = 'ev_extra1' then
                    ev_extra1 := val;
                elsif col = 'ev_extra2' then
                    ev_extra2 := val;
                elsif col = 'ev_extra3' then
                    ev_extra3 := val;
                elsif col = 'ev_extra4' then
                    ev_extra4 := val;
                elsif col = 'when' then
                    if val is null then
                        do_insert := false;
                    end if;
                end if;
            end loop;
        end if;
    end;

    -- insert final values
    if do_insert then
        perform pgque.insert_event(qname, ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
    end if;

    if do_skip or TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
        return null;
    elsif TG_OP = 'DELETE' then
        return OLD;
    else
        return NEW;
    end if;
end;
$$ language plpgsql;

create or replace function pgque._urlencode(val text)
returns text as $$
    select replace(string_agg(pair[1] || regexp_replace(encode(convert_to(pair[2], 'utf8'), 'hex'), '..', E'%\\&', 'g'), ''), '%20', '+')
        from regexp_matches($1, '([-_.a-zA-Z0-9]*)([^-_.a-zA-Z0-9]*)', 'g') pair
$$ language sql strict immutable;

create or replace function pgque.sqltriga() returns trigger as $$
-- ----------------------------------------------------------------------
-- Function: pgque.sqltriga()
--
--      Trigger function that puts row data in SQL-fragment form into queue.
--
-- Purpose:
--      Anciant way to implement replication.
--
-- Trigger parameters:
--      arg1 - queue name
--      argX - any number of optional arg, in any order
--
-- Optional arguments:
--      SKIP                - The actual operation should be skipped (BEFORE trigger)
--      ignore=col1[,col2]  - don't look at the specified arguments
--      pkey=col1[,col2]    - Set pkey fields for the table, autodetection will be skipped
--      backup              - Put urlencoded contents of old row to ev_extra2
--      colname=EXPR        - Override field value with SQL expression.  Can reference table
--                            columns.  colname can be: ev_type, ev_data, ev_extra1 .. ev_extra4
--      when=EXPR           - If EXPR returns false, don't insert event.
--
-- Queue event fields:
--      ev_type      - I/U/D ':' pkey_column_list
--      ev_data      - column values urlencoded
--      ev_extra1    - table name
--      ev_extra2    - optional urlencoded backup
--
-- Regular listen trigger example:
-- >   CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgque.logutriga('qname');
--
-- Redirect trigger example:
-- >   CREATE TRIGGER triga_nimi BEFORE INSERT OR UPDATE ON customer
-- >   FOR EACH ROW EXECUTE PROCEDURE pgque.logutriga('qname', 'SKIP');
-- ----------------------------------------------------------------------
declare
    qname text;
    ev_type text;
    ev_data text;
    ev_extra1 text;
    ev_extra2 text;
    ev_extra3 text;
    ev_extra4 text;
    do_skip boolean := false;
    do_backup boolean := false;
    do_insert boolean := true;
    do_deny boolean := false;
    extra_ignore_list text[];
    full_ignore_list text[];
    ignore_list text[] := '{}';
    pkey_list text[];
    pkey_str text;
    field_sql_sfx text;
    field_sql text[] := '{}';
    data_sql text;
    ignore_col_changes int4 := 0;
begin
    if TG_NARGS < 1 then
        raise exception 'Trigger needs queue name';
    end if;
    qname := TG_ARGV[0];

    -- standard output
    ev_extra1 := TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME;

    -- prepare to handle magic fields
    field_sql_sfx := ')::text as val from (select $1.*) r';
    extra_ignore_list := array['_pgq_ev_type', '_pgq_ev_extra1', '_pgq_ev_extra2',
                               '_pgq_ev_extra3', '_pgq_ev_extra4']::text[];

    -- parse trigger args
    declare
        got boolean;
        argpair text[];
        i integer;
    begin
        for i in 1 .. TG_NARGS-1 loop
            if TG_ARGV[i] in ('skip', 'SKIP') then
                do_skip := true;
            elsif TG_ARGV[i] = 'backup' then
                do_backup := true;
            elsif TG_ARGV[i] = 'deny' then
                do_deny := true;
            else
                got := false;
                for argpair in select regexp_matches(TG_ARGV[i], '^([^=]+)=(.*)') loop
                    got := true;
                    if argpair[1] = 'pkey' then
                        pkey_str := argpair[2];
                        pkey_list := string_to_array(pkey_str, ',');
                    elsif argpair[1] = 'ignore' then
                        ignore_list := string_to_array(argpair[2], ',');
                    elsif argpair[1] ~ '^ev_(type|extra[1-4])$' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (' || argpair[2] || field_sql_sfx);
                    elsif argpair[1] = 'when' then
                        field_sql := array_append(field_sql, 'select ' || quote_literal(argpair[1])
                                                  || '::text as key, (case when (' || argpair[2]
                                                  || ')::boolean then ''proceed'' else null end' || field_sql_sfx);
                    else
                        got := false;
                    end if;
                end loop;
                if not got then
                    raise exception 'bad argument: %', TG_ARGV[i];
                end if;
            end if;
        end loop;
    end;

    full_ignore_list := ignore_list || extra_ignore_list;

    if pkey_str is null then
        select array_agg(pk.attname)
            from (select k.attname from pg_index i, pg_attribute k
                    where i.indrelid = TG_RELID
                        and k.attrelid = i.indexrelid and i.indisprimary
                        and k.attnum > 0 and not k.attisdropped
                    order by k.attnum) pk
            into pkey_list;
        if pkey_list is null then
            pkey_list := '{}';
            pkey_str := '';
        else
            pkey_str := array_to_string(pkey_list, ',');
        end if;
    end if;
    if pkey_str = '' and TG_OP in ('UPDATE', 'DELETE') then
        raise exception 'Update/Delete on table without pkey';
    end if;

    if TG_OP = 'INSERT' then
        ev_type := 'I';
    elsif TG_OP = 'UPDATE' then
        ev_type := 'U';
    elsif TG_OP = 'DELETE' then
        ev_type := 'D';
    elsif TG_OP = 'TRUNCATE' then
        ev_type := 'R';
    else
        raise exception 'TG_OP not supported: %', TG_OP;
    end if;

    if current_setting('session_replication_role') = 'local' then
        if TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
            return null;
        elsif TG_OP = 'DELETE' then
            return OLD;
        else
            return NEW;
        end if;
    elsif do_deny then
        raise exception 'Table ''%.%'' to queue ''%'': change not allowed (%)',
                    TG_TABLE_SCHEMA, TG_TABLE_NAME, qname, TG_OP;
    elsif TG_OP = 'TRUNCATE' then
        perform pgque.insert_event(qname, ev_type, '', ev_extra1, ev_extra2, ev_extra3, ev_extra4);
        return null;
    end if;

    -- process table columns
    declare
        attr record;
        pkey_sql_buf text[];
        qcol text;
        data_sql_buf text[];
        ignore_sql text;
        ignore_sql_buf text[];
        pkey_change_sql text;
        pkey_col_changes int4 := 0;
        valexp text;
        sql1_buf text[] := '{}'; -- I:cols, U:vals, D:-
        sql2_buf text[] := '{}'; -- I:vals, U:pks, D:pks
        sql1_buf_fallback text[] := '{}';
        val_sql text;
        has_changed boolean;
    begin
        for attr in
            select k.attnum, k.attname, k.atttypid
                from pg_attribute k
                where k.attrelid = TG_RELID and k.attnum > 0 and not k.attisdropped
                order by k.attnum
        loop
            qcol := quote_ident(attr.attname);
            if attr.attname = any (ignore_list) then
                ignore_sql_buf := array_append(ignore_sql_buf,
                    'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
                continue;
            elsif attr.attname = any (extra_ignore_list) then
                field_sql := array_prepend('select ' || quote_literal(substring(attr.attname from 6))
                                           || '::text as key, (r.' || qcol || field_sql_sfx, field_sql);
                continue;
            end if;

            if attr.atttypid = 'boolean'::regtype::oid then
                valexp := 'case r.' || qcol || ' when true then ''t'' when false then ''f'' else null end';
            else
                valexp := 'r.' || qcol || '::text';
            end if;

            if attr.attname = any (pkey_list) then
                pkey_sql_buf := array_append(pkey_sql_buf,
                        'select case when rold.' || qcol || ' is null and rnew.' || qcol || ' is null then false'
                        || ' when rold.' || qcol || ' is null or rnew.' || qcol || ' is null then true'
                        || ' else rold.' || qcol || ' <> rnew.' || qcol
                        || ' end as is_changed '
                        || 'from (select $1.*) rold, (select $2.*) rnew');
                if TG_OP in ('UPDATE', 'DELETE') then
                    sql2_buf := array_append(sql2_buf, 'select ' || quote_literal(qcol)
                        || ' || coalesce(''='' || quote_literal(' || valexp|| '), '' is null'') as val'
                        || ' from (select $1.*) r');
                    if array_length(sql1_buf_fallback, 1) is null then
                        sql1_buf_fallback := array_append(sql1_buf_fallback, 'select ' || quote_literal(qcol || '=')
                            || ' || quote_nullable(' || valexp || ') as val'
                            || ' from (select $1.*) r');
                    end if;
                    continue;
                end if;
            end if;

            if TG_OP = 'INSERT' then
                sql1_buf := array_append(sql1_buf, qcol);
                sql2_buf := array_append(sql2_buf, 'select coalesce(quote_literal(' || valexp || '), ''null'') as val'
                    || ' from (select $1.*) r');
            elsif TG_OP = 'UPDATE' then
                execute 'select quote_nullable(rold.' || qcol || ') <> quote_nullable(rnew.' || qcol || ') as has_changed'
                    || ' from (select $1.*) rold, (select $2.*) rnew'
                    using OLD, NEW into has_changed;
                if has_changed then
                    sql1_buf := array_append(sql1_buf, 'select ' || quote_literal(qcol || '=')
                        || ' || quote_nullable(' || valexp || ') as val'
                        || ' from (select $1.*) r');
                end if;
            end if;
        end loop;

        -- SQL to see if pkey columns have changed
        if TG_OP = 'UPDATE' then
            pkey_change_sql := 'select count(1) from (' || array_to_string(pkey_sql_buf, ' union all ')
                            || ') cols where cols.is_changed';
            execute pkey_change_sql using OLD, NEW into pkey_col_changes;
            if pkey_col_changes > 0 then
                raise exception 'primary key update not allowed';
            end if;
        end if;

        -- SQL to see if ignored columns have changed
        if TG_OP = 'UPDATE' and array_length(ignore_list, 1) is not null then
            ignore_sql := 'select count(1) from (' || array_to_string(ignore_sql_buf, ' union all ')
                || ') cols where cols.is_changed';
            execute ignore_sql using OLD, NEW into ignore_col_changes;
        end if;

        -- SQL to load data
        if TG_OP = 'INSERT' then
            data_sql := 'select array_to_string(array[''('', '
                || quote_literal(array_to_string(sql1_buf, ','))
                || ', '') values ('','
                || '(select array_to_string(array_agg(s.val), '','') from (' || array_to_string(sql2_buf, ' union all ') || ') s)'
                || ', '')'''
                || '], '''')';
        elsif TG_OP = 'UPDATE' then
            if array_length(sql1_buf, 1) is null then
                sql1_buf := sql1_buf_fallback;
            end if;
            data_sql := 'select array_to_string(array['
                || '(select array_to_string(array_agg(s.val), '','') from (' || array_to_string(sql1_buf, ' union all ') || ') s)'
                || ', '' where '','
                || '(select array_to_string(array_agg(s.val), '' and '') from (' || array_to_string(sql2_buf, ' union all ') || ') s)'
                || '], '''')';
        else
            data_sql := 'select array_to_string(array['
                || '(select array_to_string(array_agg(s.val), '' and '') from (' || array_to_string(sql2_buf, ' union all ') || ') s)'
                || '], '''')';
        end if;
    end;

    -- render data
    declare
        old_data text;
    begin
        if TG_OP = 'INSERT' then
            execute data_sql using NEW into ev_data;
        elsif TG_OP = 'UPDATE' then

            -- render NEW
            execute data_sql using NEW into ev_data;

            -- render OLD when needed
            if do_backup or array_length(ignore_list, 1) is not null then
                execute data_sql using OLD into old_data;
            end if;

            -- only change was to ignored columns?
            if old_data = ev_data and ignore_col_changes > 0 then
                do_insert := false;
            end if;

            -- is backup needed?
            if do_backup then
                ev_extra2 := old_data;
            end if;
        elsif TG_OP = 'DELETE' then
            execute data_sql using OLD into ev_data;
        end if;
    end;

    -- apply magic args and columns
    declare
        col text;
        val text;
        rmain record;
        sql text;
    begin
        if do_insert and array_length(field_sql, 1) is not null then
            if TG_OP = 'DELETE' then
                rmain := OLD;
            else
                rmain := NEW;
            end if;

            sql := array_to_string(field_sql, ' union all ');
            for col, val in
                execute sql using rmain
            loop
                if col = 'ev_type' then
                    ev_type := val;
                elsif col = 'ev_extra1' then
                    ev_extra1 := val;
                elsif col = 'ev_extra2' then
                    ev_extra2 := val;
                elsif col = 'ev_extra3' then
                    ev_extra3 := val;
                elsif col = 'ev_extra4' then
                    ev_extra4 := val;
                elsif col = 'when' then
                    if val is null then
                        do_insert := false;
                    end if;
                end if;
            end loop;
        end if;
    end;

    -- insert final values
    if do_insert then
        perform pgque.insert_event(qname, ev_type, ev_data, ev_extra1, ev_extra2, ev_extra3, ev_extra4);
    end if;

    if do_skip or TG_WHEN = 'AFTER' or TG_OP = 'TRUNCATE' then
        return null;
    elsif TG_OP = 'DELETE' then
        return OLD;
    else
        return NEW;
    end if;
end;
$$ language plpgsql;

-- ======================================================================
-- Section 5: Default grants (derived from PgQ)
-- Origin: pgq/structure/grants.sql
-- PgQue transformations: pgq_reader/writer/admin → pgque_reader/writer/admin
-- ======================================================================



grant usage on schema pgque to public;

-- old default grants
grant select on table pgque.consumer to public;
grant select on table pgque.queue to public;
grant select on table pgque.tick to public;
grant select on table pgque.queue to public;
grant select on table pgque.subscription to public;
grant select on table pgque.event_template to public;
grant select on table pgque.retry_queue to public;

-- ======================================================================
-- Section 6: pgque additions (NEW — not derived from PgQ)
-- ======================================================================

-- pgque-additions/config.sql
-- pgque.config — singleton configuration table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create table if not exists pgque.config (
    singleton       bool primary key default true check (singleton),
    ticker_job_id   bigint,
    maint_job_id    bigint,
    installed_at    timestamptz not null default clock_timestamp()
);

-- Idempotent insert
insert into pgque.config (singleton) values (true)
on conflict (singleton) do nothing;

-- pgque-additions/queue_max_retries.sql
-- Add queue_max_retries column to pgque.queue
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- The queue table is defined in PgQ's tables.sql.  After the transformed
-- PgQ schema is installed, we add this pgque-specific column.

do $$
begin
    if not exists (
        select 1 from information_schema.columns
        where table_schema = 'pgque' and table_name = 'queue'
        and column_name = 'queue_max_retries'
    ) then
        alter table pgque.queue add column queue_max_retries int4;
    end if;
end $$;

-- Override set_queue_config to also accept queue_max_retries
create or replace function pgque.set_queue_config(
    x_queue_name    text,
    x_param_name    text,
    x_param_value   text)
returns integer as $$
declare
    v_param_name    text;
begin
    -- discard NULL input
    if x_queue_name is null or x_param_name is null then
        raise exception 'Invalid NULL value';
    end if;

    -- check if queue exists
    perform 1 from pgque.queue where queue_name = x_queue_name;
    if not found then
        raise exception 'No such event queue';
    end if;

    -- check if valid parameter name
    v_param_name := 'queue_' || x_param_name;
    if v_param_name not in (
        'queue_ticker_max_count',
        'queue_ticker_max_lag',
        'queue_ticker_idle_period',
        'queue_ticker_paused',
        'queue_rotation_period',
        'queue_external_ticker',
        'queue_max_retries')
    then
        raise exception 'cannot change parameter "%s"', x_param_name;
    end if;

    execute 'update pgque.queue set '
        || v_param_name || ' = ' || quote_literal(x_param_value)
        || ' where queue_name = ' || quote_literal(x_queue_name);

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque-additions/lifecycle.sql
-- pgque lifecycle functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create or replace function pgque.start()
returns void as $$
declare
    v_ticker_id bigint;
    v_maint_id bigint;
    v_step2_id bigint;
    v_dbname text;
begin
    -- pg_cron is optional; start() specifically requires it because it schedules jobs.
    if not exists (select 1 from pg_extension where extname = 'pg_cron') then
        raise exception 'pg_cron extension is not installed. '
            'PgQue itself works without pg_cron, but pgque.start() schedules cron jobs. '
            'Install pg_cron first, or run pgque.ticker() and pgque.maint() manually.';
    end if;

    -- Idempotent: stop existing jobs first
    perform pgque.stop();

    v_dbname := current_database();

    -- Ticker: every 2 seconds (pg_cron >= 1.5 for sub-minute scheduling)
    select cron.schedule_in_database(
        'pgque_ticker',
        '2 seconds',
        $sql$SET statement_timeout = '1500ms'; SELECT pgque.ticker()$sql$,
        v_dbname
    ) into v_ticker_id;

    -- Maintenance: every 30 seconds (rotation step1, retry, vacuum)
    select cron.schedule_in_database(
        'pgque_maint',
        '30 seconds',
        $sql$SET statement_timeout = '25s'; SELECT pgque.maint()$sql$,
        v_dbname
    ) into v_maint_id;

    -- Rotation step2: every 10 seconds, SEPARATE transaction from step1.
    -- PgQ requires step1 and step2 in different transactions so that
    -- step2's txid is guaranteed to be visible to all new transactions.
    select cron.schedule_in_database(
        'pgque_rotate_step2',
        '10 seconds',
        $sql$SELECT pgque.maint_rotate_tables_step2()$sql$,
        v_dbname
    ) into v_step2_id;

    -- Store job IDs in config
    update pgque.config
    set ticker_job_id = v_ticker_id,
        maint_job_id = v_maint_id;

    raise notice 'pgque started: ticker job=%, maint job=%, rotate_step2 job=%',
        v_ticker_id, v_maint_id, v_step2_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.stop()
returns void as $$
declare
    v_ticker_id bigint;
    v_maint_id bigint;
    v_has_pgcron bool;
begin
    -- Read current job IDs
    select ticker_job_id, maint_job_id
    into v_ticker_id, v_maint_id
    from pgque.config;

    -- Check if pg_cron is available
    select exists (select 1 from pg_extension where extname = 'pg_cron')
    into v_has_pgcron;

    if v_has_pgcron then
        -- Unschedule ticker if it exists
        if v_ticker_id is not null then
            perform cron.unschedule(v_ticker_id);
        end if;

        -- Unschedule maint if it exists
        if v_maint_id is not null then
            perform cron.unschedule(v_maint_id);
        end if;

        -- Unschedule rotate_step2 by name (job ID not stored in config)
        -- Ignore if job doesn't exist (first run or already removed)
        begin
            perform cron.unschedule('pgque_rotate_step2');
        exception when others then
            raise notice 'pgque.stop: rotate_step2 job not found (OK on first install)';
        end;
    end if;

    -- Clear job IDs regardless (even if pg_cron is gone)
    update pgque.config
    set ticker_job_id = null,
        maint_job_id = null;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.uninstall()
returns void as $$
begin
    -- Stop pg_cron jobs before dropping the schema.
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        perform pgque.stop();
    end if;
    -- Drop everything
    drop schema pgque cascade;
    -- Note: roles are not dropped here (they may be in use by other databases)
    raise notice 'pgque uninstalled. Run DROP ROLE IF EXISTS pgque_reader, pgque_writer, pgque_admin; manually if needed.';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.version()
returns text as $$
begin
    return '1.0.0-dev';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

create or replace function pgque.status()
returns table (
    component text,
    status text,
    detail text
) as $$
begin
    -- PostgreSQL version
    return query select 'postgresql'::text, 'info'::text, pg_catalog.version()::text;

    -- pgque version
    return query select 'pgque'::text, 'info'::text, pgque.version();

    -- pg_cron status
    if exists (select 1 from pg_extension where extname = 'pg_cron') then
        return query
        select 'ticker'::text,
            case when c.ticker_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.ticker_job_id is not null
                then 'job_id=' || c.ticker_job_id::text
                else 'not scheduled'
            end
        from pgque.config c;

        return query
        select 'maintenance'::text,
            case when c.maint_job_id is not null then 'scheduled' else 'stopped' end,
            case when c.maint_job_id is not null
                then 'job_id=' || c.maint_job_id::text
                else 'not scheduled'
            end
        from pgque.config c;
    else
        return query select 'pg_cron'::text, 'unavailable'::text,
            'pg_cron not installed -- call pgque.ticker() and pgque.maint() manually'::text;
    end if;

    -- Queue count
    return query select 'queues'::text, 'info'::text,
        (select count(*)::text from pgque.queue) || ' queues configured';

    -- Consumer count
    return query select 'consumers'::text, 'info'::text,
        (select count(*)::text from pgque.subscription) || ' active subscriptions';
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque-additions/roles.sql
-- pgque security roles
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Create roles idempotently
do $$ begin create role pgque_reader; exception when duplicate_object then null; end $$;
do $$ begin create role pgque_writer; exception when duplicate_object then null; end $$;
do $$ begin create role pgque_admin;  exception when duplicate_object then null; end $$;

-- Inheritance: admin > writer > reader
-- Wrapped in exception handlers for PG14/15 compatibility (no IF NOT EXISTS
-- for role grants until PG16).
do $$ begin
    grant pgque_reader to pgque_writer;
exception when duplicate_object then null;
end $$;
do $$ begin
    grant pgque_writer to pgque_admin;
exception when duplicate_object then null;
end $$;

-- ---------------------------------------------------------------------------
-- Reader: read-only access to schema and information functions
-- ---------------------------------------------------------------------------
grant usage on schema pgque to pgque_reader;
grant select on all tables in schema pgque to pgque_reader;

-- get_queue_info — 0-arg (all queues) and 1-arg (single queue)
grant execute on function pgque.get_queue_info() to pgque_reader;
grant execute on function pgque.get_queue_info(text) to pgque_reader;

-- get_consumer_info — 0-arg, 1-arg, 2-arg overloads
grant execute on function pgque.get_consumer_info() to pgque_reader;
grant execute on function pgque.get_consumer_info(text) to pgque_reader;
grant execute on function pgque.get_consumer_info(text, text) to pgque_reader;

-- get_batch_info(bigint)
grant execute on function pgque.get_batch_info(bigint) to pgque_reader;

-- version
grant execute on function pgque.version() to pgque_reader;

-- ---------------------------------------------------------------------------
-- Writer: can produce events and manage consumer lifecycle
-- ---------------------------------------------------------------------------

-- insert_event — 3-arg and 7-arg overloads
grant execute on function pgque.insert_event(text, text, text) to pgque_writer;
grant execute on function pgque.insert_event(text, text, text, text, text, text, text) to pgque_writer;

-- consumer registration
grant execute on function pgque.register_consumer(text, text) to pgque_writer;
grant execute on function pgque.register_consumer_at(text, text, bigint) to pgque_writer;
grant execute on function pgque.unregister_consumer(text, text) to pgque_writer;

-- batch processing
grant execute on function pgque.next_batch(text, text) to pgque_writer;
grant execute on function pgque.next_batch_info(text, text) to pgque_writer;
grant execute on function pgque.next_batch_custom(text, text, interval, int4, interval) to pgque_writer;
grant execute on function pgque.get_batch_events(bigint) to pgque_writer;
grant execute on function pgque.finish_batch(bigint) to pgque_writer;

-- event retry — timestamptz and integer overloads
grant execute on function pgque.event_retry(bigint, bigint, timestamptz) to pgque_writer;
grant execute on function pgque.event_retry(bigint, bigint, integer) to pgque_writer;

-- Note: grants for the modern API wrappers (send*, subscribe, unsubscribe,
-- receive, ack, nack) live colocated with their definitions in
-- sql/pgque-api/*.sql. transform.sh appends pgque-additions/ before
-- pgque-api/, so API-layer grants cannot reference their functions from
-- this file.

-- ---------------------------------------------------------------------------
-- Admin: full access to everything in the pgque schema
-- ---------------------------------------------------------------------------
grant all on schema pgque to pgque_admin;
grant all on all tables in schema pgque to pgque_admin;
grant all on all sequences in schema pgque to pgque_admin;
grant execute on all functions in schema pgque to pgque_admin;

-- uninstall() drops the entire schema — only superuser / schema owner should run it
revoke execute on function pgque.uninstall() from pgque_admin;

-- pgque-additions/dlq.sql
-- pgque dead letter queue (DLQ) -- table + helper functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- PgQ has a retry queue but no dead letter queue. pgque adds one.
-- See SPECx.md section 4.5.

-- pgque.dead_letter table
--
-- FK behavior: both dl_queue_id and dl_consumer_id use `on delete cascade`.
-- Rationale:
--   - `pgque.drop_queue()` deletes the pgque.queue row unconditionally. DLQ
--     entries for a dropped queue are meaningless, so cascading them is
--     correct (users who want to preserve the audit trail should call
--     `pgque.dlq_purge` or copy the rows out before dropping the queue).
--   - `pgque.unregister_consumer()` deletes the pgque.consumer row only when
--     the consumer has no other subscriptions. That is an explicit,
--     user-initiated action (not routine maintenance), so cascading the
--     historical DLQ rows tied to that (now-removed) consumer id is the
--     least-surprising default. Same escape hatch: purge/copy first if the
--     audit trail matters.
create table if not exists pgque.dead_letter (
    dl_id           bigserial primary key,
    dl_queue_id     int4 not null references pgque.queue(queue_id)    on delete cascade,
    dl_consumer_id  int4 not null references pgque.consumer(co_id)    on delete cascade,
    dl_time         timestamptz not null default now(),
    dl_reason       text,

    -- Original event fields (copied from event at time of death)
    ev_id           bigint not null,
    ev_time         timestamptz not null,
    ev_txid         xid8,
    ev_retry        int4,
    ev_type         text,
    ev_data         text,
    ev_extra1       text,
    ev_extra2       text,
    ev_extra3       text,
    ev_extra4       text
);

create index if not exists dl_queue_time_idx
    on pgque.dead_letter (dl_queue_id, dl_time);

-- pgque.event_dead() -- move event to DLQ (called by nack() when max retries exceeded)
create or replace function pgque.event_dead(
    i_batch_id bigint,
    i_event_id bigint,
    i_reason text,
    i_ev_time timestamptz,
    i_ev_txid xid8,
    i_ev_retry int4,
    i_ev_type text,
    i_ev_data text,
    i_ev_extra1 text default null,
    i_ev_extra2 text default null,
    i_ev_extra3 text default null,
    i_ev_extra4 text default null)
returns integer as $$
declare
    v_sub record;
begin
    -- Look up subscription from batch
    select sub_queue, sub_consumer into v_sub
    from pgque.subscription where sub_batch = i_batch_id;
    if not found then
        raise exception 'batch not found: %', i_batch_id;
    end if;

    -- Insert into dead letter table (no re-query of batch events)
    insert into pgque.dead_letter (
        dl_queue_id, dl_consumer_id, dl_reason,
        ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    values (
        v_sub.sub_queue, v_sub.sub_consumer, i_reason,
        i_event_id, i_ev_time, i_ev_txid, i_ev_retry, i_ev_type, i_ev_data,
        i_ev_extra1, i_ev_extra2, i_ev_extra3, i_ev_extra4);

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.dlq_inspect() -- inspect DLQ entries for a queue
create or replace function pgque.dlq_inspect(
    i_queue_name text, i_limit_count int default 100)
returns setof pgque.dead_letter as $$
begin
    return query
    select dl.*
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = i_queue_name
    order by dl.dl_time desc
    limit i_limit_count;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.dlq_replay() -- replay a single dead letter event back into the queue
create or replace function pgque.dlq_replay(i_dead_letter_id bigint)
returns bigint as $$
declare
    v_dl record;
    v_queue_name text;
    v_new_eid bigint;
begin
    select dl.*, q.queue_name into v_dl
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where dl.dl_id = i_dead_letter_id;

    if not found then
        raise exception 'dead letter entry not found: %', i_dead_letter_id;
    end if;

    -- Re-insert into the queue
    v_new_eid := pgque.insert_event(v_dl.queue_name, v_dl.ev_type, v_dl.ev_data,
        v_dl.ev_extra1, v_dl.ev_extra2, v_dl.ev_extra3, v_dl.ev_extra4);

    -- Remove from DLQ
    delete from pgque.dead_letter where dl_id = i_dead_letter_id;

    return v_new_eid;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.dlq_replay_all() -- replay all DLQ events for a queue
create or replace function pgque.dlq_replay_all(i_queue_name text)
returns integer as $$
declare
    v_dl record;
    v_cnt integer := 0;
begin
    for v_dl in
        select dl.dl_id, dl.ev_type, dl.ev_data,
               dl.ev_extra1, dl.ev_extra2, dl.ev_extra3, dl.ev_extra4,
               q.queue_name
        from pgque.dead_letter dl
        join pgque.queue q on q.queue_id = dl.dl_queue_id
        where q.queue_name = i_queue_name
    loop
        perform pgque.insert_event(v_dl.queue_name, v_dl.ev_type, v_dl.ev_data,
            v_dl.ev_extra1, v_dl.ev_extra2, v_dl.ev_extra3, v_dl.ev_extra4);
        delete from pgque.dead_letter where dl_id = v_dl.dl_id;
        v_cnt := v_cnt + 1;
    end loop;

    return v_cnt;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.dlq_purge() -- purge old DLQ entries
create or replace function pgque.dlq_purge(
    i_queue_name text, i_older_than interval default '30 days')
returns integer as $$
declare
    v_cnt integer;
begin
    delete from pgque.dead_letter
    where dl_queue_id = (select queue_id from pgque.queue where queue_name = i_queue_name)
      and dl_time < now() - i_older_than;
    get diagnostics v_cnt = row_count;
    return v_cnt;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------
-- dlq.sql runs after roles.sql in transform.sh, so role names already exist.
-- However, roles.sql's blanket "grant select on all tables / execute on all
-- functions" passes ran *before* dlq.sql created these objects, so they do
-- NOT cover the DLQ table and functions. The explicit grants below are
-- therefore required (not redundant) for every role mentioned, including
-- pgque_admin.
--
-- dlq_inspect is read-only — available to pgque_reader and above.
-- dlq_replay / dlq_replay_all re-insert events — writer-level.
-- dlq_purge / event_dead: admin-level operations (purge = data loss,
-- event_dead = internal DLQ hook called from nack()). Granted to pgque_admin
-- explicitly for the reason above. Left on PUBLIC default EXECUTE for now;
-- consider revoke-from-public if the codebase adopts that convention more
-- broadly.

grant select on pgque.dead_letter                           to pgque_reader;
grant all    on pgque.dead_letter                           to pgque_admin;
grant all    on sequence pgque.dead_letter_dl_id_seq        to pgque_admin;

grant execute on function pgque.dlq_inspect(text, int)      to pgque_reader;
grant execute on function pgque.dlq_replay(bigint)          to pgque_writer;
grant execute on function pgque.dlq_replay_all(text)        to pgque_writer;
grant execute on function pgque.event_dead(
    bigint, bigint, text, timestamptz, xid8, int4,
    text, text, text, text, text, text)                     to pgque_admin;
grant execute on function pgque.dlq_purge(text, interval)   to pgque_admin;

-- ======================================================================
-- Section 7: pgque-api (NEW — not derived from PgQ)
-- ======================================================================

-- pgque-api/maint.sql
-- pgque maint() -- default maintenance runner for v0.1
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Runs PgQ maintenance operations (rotation, retry, vacuum).
-- Experimental addons may override this function to extend maintenance.

-- maint() runs rotation step1, retry, and vacuum.
-- IMPORTANT: rotation step2 is NOT included here — it MUST run in a separate
-- transaction from step1 (PgQ design requirement). pgque.start() schedules
-- step2 as its own pg_cron job.
create or replace function pgque.maint()
returns integer as $$
declare
    f record;
    sql text;
    r integer;
    total integer := 0;
begin
    for f in select func_name, func_arg from pgque.maint_operations()
    loop
        -- Skip step2: it needs a separate transaction (scheduled by pgque.start)
        if f.func_name = 'pgque.maint_rotate_tables_step2' then
            continue;
        elsif f.func_name = 'vacuum' then
            sql := 'vacuum ' || f.func_arg;
            execute sql;
            total := total + 1;
        elsif f.func_arg is not null then
            execute 'select ' || f.func_name || '(' || quote_literal(f.func_arg) || ')' into r;
            total := total + r;
        else
            execute 'select ' || f.func_name || '()' into r;
            total := total + r;
        end if;
    end loop;

    return total;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque-api/receive.sql
-- pgque.receive(), pgque.ack(), pgque.nack() -- modern consume API
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- These functions wrap PgQ primitives (next_batch, get_batch_events,
-- finish_batch, event_retry) into a simpler receive/ack/nack interface.
-- See SPECx.md sections 4.2 and 4.3.

-- pgque.message type (idempotent creation)
do $$ begin
    create type pgque.message as (
        msg_id      bigint,
        batch_id    bigint,
        type        text,
        payload     text,
        retry_count int4,
        created_at  timestamptz,
        extra1      text,
        extra2      text,
        extra3      text,
        extra4      text
    );
exception when duplicate_object then null;
end $$;

-- pgque.receive() -- wraps next_batch + get_batch_events
create or replace function pgque.receive(
    i_queue text, i_consumer text, i_max_return int default 100)
returns setof pgque.message as $$
declare
    v_batch_id bigint;
    ev record;
    cnt int := 0;
begin
    -- Get next batch (may return NULL if no events)
    v_batch_id := pgque.next_batch(i_queue, i_consumer);
    if v_batch_id is null then
        return;
    end if;

    -- Yield messages from the batch
    for ev in
        select ev_id, ev_type, ev_data, ev_retry, ev_time,
               ev_extra1, ev_extra2, ev_extra3, ev_extra4
        from pgque.get_batch_events(v_batch_id)
    loop
        return next row(
            ev.ev_id, v_batch_id, ev.ev_type, ev.ev_data,
            ev.ev_retry, ev.ev_time,
            ev.ev_extra1, ev.ev_extra2, ev.ev_extra3, ev.ev_extra4
        )::pgque.message;
        cnt := cnt + 1;
        exit when cnt >= i_max_return;
    end loop;
    return;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.ack() -- finishes the batch, advances consumer position
create or replace function pgque.ack(i_batch_id bigint)
returns integer as $$
begin
    return pgque.finish_batch(i_batch_id);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.nack() -- retry or route to DLQ based on retry_count vs max_retries
create or replace function pgque.nack(
    i_batch_id bigint,
    i_msg pgque.message,
    i_retry_after interval default '60 seconds',
    i_reason text default null)
returns integer as $$
declare
    v_max_retries int4;
begin
    -- Single lookup: subscription -> queue config
    select coalesce(q.queue_max_retries, 5) into v_max_retries
    from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    where s.sub_batch = i_batch_id;

    if not found then
        raise exception 'batch not found: %', i_batch_id;
    end if;

    if coalesce(i_msg.retry_count, 0) >= v_max_retries then
        -- Move to dead letter queue (pass event fields, no re-query)
        perform pgque.event_dead(i_batch_id, i_msg.msg_id,
            coalesce(i_reason, 'max retries exceeded'),
            i_msg.created_at, null::xid8, i_msg.retry_count,
            i_msg.type, i_msg.payload,
            i_msg.extra1, i_msg.extra2, i_msg.extra3, i_msg.extra4);
    else
        -- Retry after delay
        perform pgque.event_retry(i_batch_id, i_msg.msg_id,
            extract(epoch from i_retry_after)::integer);
    end if;
    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------
-- Colocated here (not in pgque-additions/roles.sql) because roles.sql is
-- assembled before pgque-api/, so these functions do not yet exist when
-- roles.sql runs. Same convention as sql/pgque-api/send.sql.

grant execute on function pgque.receive(text, text, int)                      to pgque_writer;
grant execute on function pgque.ack(bigint)                                   to pgque_writer;
grant execute on function pgque.nack(bigint, pgque.message, interval, text)   to pgque_writer;

-- pgque-api/send.sql
-- pgque-api/send.sql -- Modern send/subscribe API layer
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- Implements default v0.1 API surface:
--   pgque.message type
--   pgque.send(queue, payload)                -- text + jsonb overloads
--   pgque.send(queue, type, payload)          -- text + jsonb overloads
--   pgque.send_batch(queue, type, payloads[]) -- text[] + jsonb[] overloads
--   pgque.subscribe(queue, consumer)
--   pgque.unsubscribe(queue, consumer)
--
-- Overload resolution note: PostgreSQL resolves untyped string literals
-- (type `unknown`) to the `text` overload because `unknown -> text` needs
-- no implicit cast, while `unknown -> jsonb` does. Consequently:
--
--   select pgque.send('orders', '{"k":1}');           -- picks send(text, text)
--   select pgque.send('orders', '{"k":1}'::jsonb);    -- picks send(text, jsonb)
--
-- The `text` overloads are the default for untyped literals: bytes flow
-- through verbatim (no parse, no canonicalization, key order preserved)
-- for *textual* payloads -- JSON, XML, CSV, or binary that has already
-- been base64/hex-encoded. PostgreSQL `text` cannot store NUL (\x00),
-- so true binary payloads (raw protobuf, msgpack, Avro, bytea dumps)
-- must be caller-encoded before `send()` -- otherwise PG rejects the
-- insert with `invalid byte sequence`.
-- The `jsonb` overloads are opt-in via explicit `::jsonb` cast: PG
-- validates JSON at parse time and stores the canonical form.
-- Storage (ev_data TEXT) is identical in both paths.

-- pgque.message type (idempotent creation)
do $$ begin
    create type pgque.message as (
        msg_id      bigint,       -- ev_id
        batch_id    bigint,       -- batch containing this message
        type        text,         -- ev_type
        payload     text,         -- ev_data (caller casts to jsonb if needed)
        retry_count int4,         -- ev_retry (NULL for first delivery)
        created_at  timestamptz,  -- ev_time
        extra1      text,         -- ev_extra1
        extra2      text,         -- ev_extra2
        extra3      text,         -- ev_extra3
        extra4      text          -- ev_extra4
    );
exception when duplicate_object then null;
end $$;

-- pgque.send(queue, payload jsonb) -- send with default type, JSON payload
create or replace function pgque.send(i_queue text, i_payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, 'default', i_payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send(queue, payload text) -- fast path, opaque textual payload
-- Skips the jsonb parse + canonical reserialize round-trip. Use this when
-- the payload is text (JSON, XML, CSV, base64/hex-encoded binary) or when
-- the caller has already validated the payload. Raw binary with NUL bytes
-- (protobuf, msgpack, Avro wire format) is not accepted by PG `text` --
-- encode first.
create or replace function pgque.send(i_queue text, i_payload text)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, 'default', i_payload);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send(queue, type, payload jsonb) -- send with explicit type, JSON payload
create or replace function pgque.send(i_queue text, i_type text, i_payload jsonb)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload::text);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send(queue, type, payload text) -- fast path with explicit type
create or replace function pgque.send(i_queue text, i_type text, i_payload text)
returns bigint as $$
begin
    return pgque.insert_event(i_queue, i_type, i_payload);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send_batch(queue, type, payloads jsonb[]) -- batch send, JSON payloads
create or replace function pgque.send_batch(
    i_queue text, i_type text, i_payloads jsonb[])
returns bigint[] as $$
declare
    ids bigint[] := '{}';
    p jsonb;
begin
    foreach p in array i_payloads loop
        ids := array_append(ids,
            pgque.insert_event(i_queue, i_type, p::text));
    end loop;
    return ids;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.send_batch(queue, type, payloads text[]) -- fast-path batch send
create or replace function pgque.send_batch(
    i_queue text, i_type text, i_payloads text[])
returns bigint[] as $$
declare
    ids bigint[] := '{}';
    p text;
begin
    foreach p in array i_payloads loop
        ids := array_append(ids,
            pgque.insert_event(i_queue, i_type, p));
    end loop;
    return ids;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.subscribe(queue, consumer) -- wrapper for register_consumer
create or replace function pgque.subscribe(i_queue text, i_consumer text)
returns integer as $$
begin
    return pgque.register_consumer(i_queue, i_consumer);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.unsubscribe(queue, consumer) -- wrapper for unregister_consumer
create or replace function pgque.unsubscribe(i_queue text, i_consumer text)
returns integer as $$
begin
    return pgque.unregister_consumer(i_queue, i_consumer);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- Explicit writer grants for the send* + subscribe/unsubscribe family.
-- Colocated here (not in pgque-additions/roles.sql) because roles.sql
-- runs before the pgque-api section during build, so API-layer grants
-- must live alongside the function definitions they apply to.
grant execute on function pgque.send(text, jsonb)               to pgque_writer;
grant execute on function pgque.send(text, text)                to pgque_writer;
grant execute on function pgque.send(text, text, jsonb)         to pgque_writer;
grant execute on function pgque.send(text, text, text)          to pgque_writer;
grant execute on function pgque.send_batch(text, text, jsonb[]) to pgque_writer;
grant execute on function pgque.send_batch(text, text, text[])  to pgque_writer;
grant execute on function pgque.subscribe(text, text)           to pgque_writer;
grant execute on function pgque.unsubscribe(text, text)         to pgque_writer;


