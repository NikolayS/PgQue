-- pgque_uninstall.sql -- Remove pgque from database
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/*
 * Stops scheduler jobs (pg_cron / pg_timetable) before dropping the schema:
 * they are catalog rows, not dependent objects, so a schema drop alone would
 * orphan them. A real stop() failure aborts; only "not installed" is
 * tolerated, keeping the script idempotent.
 */

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
