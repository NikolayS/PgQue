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
