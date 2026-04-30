-- pgque maint() -- default maintenance runner for v0.1
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Runs PgQ maintenance operations (rotation, retry, extra hooks).
-- Experimental addons may override this function to extend maintenance.

-- maint() runs rotation step1, retry, and queue_extra_maint hooks.
-- IMPORTANT: rotation step2 is NOT included here — it MUST run in a separate
-- transaction from step1 (PgQ design requirement). pgque.start() schedules
-- step2 as its own pg_cron job.
--
-- VACUUM note (issue #110): PostgreSQL forbids VACUUM inside any function or
-- transaction block.  The 'vacuum' rows emitted by maint_operations() are
-- skipped here with a NOTICE logged.  When autovacuum is enabled (the default)
-- it handles pgque metadata tables automatically.  Installations with
-- autovacuum disabled should schedule a separate pg_cron job, e.g.:
--   select cron.schedule('pgque-vacuum', '0 * * * *',
--     'vacuum pgque.queue, pgque.tick, pgque.subscription, pgque.consumer');
--
-- NOTE: any user-supplied queue_extra_maint function that itself issues VACUUM
-- is also subject to the no-VACUUM-in-PL/pgSQL rule and will fail at runtime.
create or replace function pgque.maint()
returns integer as $$
declare
    f record;
    r integer;
    total integer := 0;
begin
    for f in select func_name, func_arg from pgque.maint_operations()
    loop
        -- Skip step2: it needs a separate transaction (scheduled by pgque.start)
        if f.func_name = 'pgque.maint_rotate_tables_step2' then
            continue;
        elsif f.func_name = 'vacuum' then
            -- VACUUM cannot be executed from a function (PostgreSQL restriction).
            -- Emit a notice so operators can diagnose why VACUUM did not run.
            -- Autovacuum handles pgque metadata tables by default; see issue #110.
            raise notice 'pgque.maint: skipping VACUUM (%) — schedule via pg_cron if autovacuum is off', f.func_arg;
            continue;
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
