-- pgque delayed delivery: send_at() and maint_deliver_delayed()
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Events with a future delivery time go into a holding table.
-- A maintenance step moves them to the main event table when due.

-- delayed_events holding table
create table if not exists pgque.delayed_events (
    de_id           bigserial primary key,
    de_queue_name   text not null,
    de_deliver_at   timestamptz not null,
    de_type         text,
    de_data         text,
    de_extra1       text,
    de_extra2       text,
    de_extra3       text,
    de_extra4       text
);

create index if not exists de_deliver_idx
    on pgque.delayed_events (de_deliver_at);

-- send_at() -- delayed event delivery
--
-- If i_deliver_at <= now(), delivers immediately via insert_event()
-- and returns the queue event ID.
-- If i_deliver_at is in the future, inserts into delayed_events
-- and returns the scheduled-entry ID (NOT a queue event ID).
create or replace function pgque.send_at(
    i_queue text, i_type text, i_payload jsonb, i_deliver_at timestamptz)
returns bigint as $$
begin
    if i_deliver_at <= now() then
        -- Deliver immediately; returns queue event ID
        return pgque.insert_event(i_queue, i_type, i_payload::text);
    end if;

    insert into pgque.delayed_events
        (de_queue_name, de_deliver_at, de_type, de_data)
    values (i_queue, i_deliver_at, i_type, i_payload::text);

    -- Returns scheduled-entry ID (NOT a queue event ID)
    return currval('pgque.delayed_events_de_id_seq');
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- maint_deliver_delayed() -- move due delayed events into queues
--
-- Called by pgque.maint(). Deletes rows from delayed_events where
-- de_deliver_at <= now() and inserts them into the target queue
-- via insert_event().
create or replace function pgque.maint_deliver_delayed()
returns integer as $$
declare
    ev record;
    cnt integer := 0;
begin
    for ev in
        delete from pgque.delayed_events
        where de_deliver_at <= now()
        returning *
    loop
        perform pgque.insert_event(ev.de_queue_name, ev.de_type, ev.de_data,
            ev.de_extra1, ev.de_extra2, ev.de_extra3, ev.de_extra4);
        cnt := cnt + 1;
    end loop;
    return cnt;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- maint() -- top-level maintenance wrapper
--
-- Calls PgQ's maint_operations() (rotation, retry) and also
-- runs maint_deliver_delayed() for delayed event delivery.
--
-- IMPORTANT: VACUUM is intentionally skipped here. Running VACUUM inside
-- the same transaction as rotation is dangerous: if VACUUM hits the
-- statement_timeout (25s from pg_cron), the entire transaction rolls back,
-- including the rotation step1/step2 changes. This causes rotation to
-- never complete, leading to unbounded event table growth.
--
-- Event tables have autovacuum_enabled=off (set by create_queue) because
-- they use TRUNCATE-based rotation instead of DELETE. The pgque.tick and
-- pgque.subscription tables are small and handled by autovacuum normally.
create or replace function pgque.maint()
returns integer as $$
declare
    f record;
    sql text;
    r integer;
    total integer := 0;
begin
    -- Run PgQ maintenance operations (rotation, retry) — skip vacuum
    for f in select func_name, func_arg from pgque.maint_operations()
    loop
        if f.func_name = 'vacuum' then
            -- Skip: vacuum inside the same TX as rotation causes timeout
            -- rollback that prevents rotation from ever completing.
            -- autovacuum handles the small metadata tables; event tables
            -- use TRUNCATE rotation and don't need vacuum.
            continue;
        elsif f.func_arg is not null then
            execute 'select ' || f.func_name || '(' || quote_literal(f.func_arg) || ')' into r;
            total := total + r;
        else
            execute 'select ' || f.func_name || '()' into r;
            total := total + r;
        end if;
    end loop;

    -- Run delayed event delivery
    select pgque.maint_deliver_delayed() into r;
    total := total + r;

    return total;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
