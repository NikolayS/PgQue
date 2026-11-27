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
$$ language plpgsql security definer set search_path = pgque, pg_catalog, pg_temp;

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
$$ language plpgsql security definer set search_path = pgque, pg_catalog, pg_temp;

-- maint() -- top-level maintenance wrapper
--
-- Calls PgQ's maint_operations() (rotation, retry) and also
-- runs maint_deliver_delayed() for delayed event delivery.
--
-- IMPORTANT: rotation step2 and VACUUM are skipped here.
--
-- Step2 MUST run in a separate transaction from step1 (PgQ design
-- requirement). pgque.start() schedules step2 as its own pg_cron job.
--
-- VACUUM is skipped because autovacuum handles the small metadata
-- tables, and event tables use TRUNCATE rotation (no dead tuples).
create or replace function pgque.maint()
returns integer as $$
declare
    f record;
    r integer;
    total integer := 0;
    v_maint_owner name;
    v_func_owner  name;
    v_func_oid    oid;
    v_pass        integer;
begin
    /* Resolve install-owner name once per call (pg_get_userbyid avoids pg_authid). */
    select pg_catalog.pg_get_userbyid(p.proowner) into v_maint_owner
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
    where n.nspname = 'pgque'
      and p.proname = 'maint'
      and pg_catalog.pg_get_function_arguments(p.oid) = '';

    for f in select func_name, func_arg from pgque.maint_operations()
    loop
        if f.func_name = 'pgque.maint_rotate_tables_step2' then
            continue;
        elsif f.func_name = 'vacuum' then
            continue;
        elsif f.func_arg is not null then
            begin
                execute format('select %L::regprocedure', f.func_name || '(text)')
                into v_func_oid;
            exception when others then
                raise warning 'pgque.maint: skipping % — invalid regprocedure: %', f.func_name, sqlerrm;
                continue;
            end;

            select pg_catalog.pg_get_userbyid(p.proowner) into v_func_owner
            from pg_proc p
            where p.oid = v_func_oid;

            if v_func_owner is distinct from v_maint_owner then
                raise warning 'pgque.maint: skipping % — owner % is not maint() owner %', f.func_name, v_func_owner, v_maint_owner;
                continue;
            end if;

            v_pass := 0;
            loop
                execute 'select ' || f.func_name || '(' || quote_literal(f.func_arg) || ')' into r;
                total := total + coalesce(r, 0);
                v_pass := v_pass + 1;
                exit when f.func_name = 'pgque.maint_rotate_tables_step1'
                    or r is distinct from 1
                    or v_pass >= 50;
            end loop;
        else
            execute 'select ' || f.func_name || '()' into r;
            total := total + r;
        end if;
    end loop;

    select pgque.maint_deliver_delayed() into r;
    total := total + r;

    return total;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog, pg_temp;

grant execute on function pgque.maint() to pgque_admin;
