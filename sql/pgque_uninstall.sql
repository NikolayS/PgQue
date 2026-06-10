-- pgque_uninstall.sql -- Remove pgque from database
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Stops scheduler jobs (pg_cron / pg_timetable) before dropping the schema:
-- scheduler jobs are catalog rows, not dependent objects, so dropping the
-- schema alone would leave them behind, failing forever afterwards. A real
-- stop() failure therefore aborts the uninstall; only "pgque is not
-- installed" errors are tolerated, keeping the script idempotent.

do $$
begin
    begin
        perform pgque.stop();
    exception
        when undefined_function or invalid_schema_name then
            -- pgque is not installed (or has no stop()); nothing to stop.
            null;
    end;
    drop schema if exists pgque cascade;
end $$;

-- Roles are database-global and may be shared across databases.
-- Do not drop them automatically here.
