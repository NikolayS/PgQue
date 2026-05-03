-- pgque-pg_tle-uninstall.sql -- Remove PgQue from pg_tle.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Drops the pgque extension from this database (if installed) and unregisters
-- every installed version from pg_tle's catalog. Roles are NOT dropped because
-- they may still be referenced by other databases on the cluster.
--
-- Usage:
--   psql -d mydb -f sql/pgque-pg_tle-uninstall.sql

\set ON_ERROR_STOP on

drop extension if exists pgque cascade;

do $$
begin
    if to_regprocedure('pgtle.uninstall_extension(text)') is null then
        raise notice 'pg_tle is not available; nothing to unregister.';
        return;
    end if;
    perform pgtle.uninstall_extension('pgque');
    raise notice 'pgque unregistered from pg_tle.';
end $$;

\echo ''
\echo 'PgQue uninstalled from pg_tle.'
\echo 'Drop the pgque_reader / pgque_writer / pgque_admin roles manually if no'
\echo 'other database on this cluster still uses them.'
