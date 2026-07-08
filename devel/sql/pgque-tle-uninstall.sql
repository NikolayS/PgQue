-- pgque-tle-uninstall.sql -- Remove PgQue from pg_tle.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/*
 * Stops scheduler jobs, drops the pgque extension from this database (if
 * installed), and unregisters pgque from pg_tle. Roles are NOT dropped -- they
 * may still be referenced by other databases on the cluster. Idempotent.
 *
 * Usage: psql -d mydb -f sql/pgque-tle-uninstall.sql
 */

\set ON_ERROR_STOP on

/* Stop scheduler jobs before dropping the extension: they are catalog rows,
   not dependent objects, so drop extension alone would orphan them. A real
   stop() failure aborts; only "not installed" is tolerated. */
do $$
begin
    begin
        perform pgque.stop();
    exception
        when undefined_function or invalid_schema_name then
            -- pgque is not installed (or has no stop()); nothing to stop.
            null;
    end;
    drop extension if exists pgque cascade;
end $$;

do $$
begin
    if not exists (select 1 from pg_catalog.pg_extension where extname = 'pg_tle') then
        raise notice 'pg_tle is not available; nothing to unregister.';
        return;
    end if;
    if not exists (select 1 from pgtle.available_extensions() where name = 'pgque') then
        raise notice 'pgque is not registered with pg_tle; nothing to unregister.';
        return;
    end if;
    perform pgtle.uninstall_extension('pgque');
    raise notice 'pgque unregistered from pg_tle.';
end $$;

\echo ''
\echo 'PgQue uninstalled from pg_tle.'
\echo 'Drop the pgque_reader / pgque_writer / pgque_admin roles manually if no'
\echo 'other database on this cluster still uses them.'
