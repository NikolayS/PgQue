-- test_pg_tle_install.sql -- End-to-end pg_tle install path.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Pre-conditions for the caller:
--   - pg_tle binary is loaded (shared_preload_libraries=pg_tle)
--   - the database is fresh (no pgque schema, no pgque extension installed)
--
-- Steps exercised:
--   1. create extension pg_tle
--   2. \i sql/pgque-pg_tle.sql              -- registers pgque with pg_tle
--   3. create extension pgque                -- materialises the schema
--   4. assert extension membership / role grants are wired
--   5. drop extension pgque cascade          -- clean uninstall
--   6. \i sql/pgque-pg_tle-uninstall.sql     -- unregister from pg_tle
--
-- Run from the repo root:
--   psql -d pgque_pgtle_test -v ON_ERROR_STOP=1 -f tests/test_pg_tle_install.sql

\set ON_ERROR_STOP on

\echo '=== test_pg_tle_install (e2e against real pg_tle) ==='

create extension if not exists pg_tle;

\i sql/pgque-pg_tle.sql

-- pgque must show up in the pg_tle catalog before we materialise the schema.
do $$
declare
    v text;
begin
    select default_version into v
    from pgtle.available_extensions()
    where name = 'pgque';
    assert v is not null, 'pgque must appear in pgtle.available_extensions()';
    assert v ~ '^[0-9]+\.[0-9]+\.[0-9]+',
        format('pgque version looks malformed: %s', v);
    raise notice 'PASS: pgque registered with pg_tle as version %', v;
end $$;

create extension pgque;

-- Extension membership: pgque is now visible in pg_extension and the schema /
-- core tables / public version() function are reachable.
do $$
begin
    assert exists (select 1 from pg_catalog.pg_extension where extname = 'pgque'),
        'pgque must be listed in pg_extension';
    assert exists (select 1 from pg_catalog.pg_namespace where nspname = 'pgque'),
        'pgque schema must exist';
    assert exists (
        select 1 from pg_catalog.pg_class c
        join pg_catalog.pg_namespace n on n.oid = c.relnamespace
        where n.nspname = 'pgque' and c.relname = 'queue'
    ), 'pgque.queue must exist';
    assert pgque.version() ~ '^[0-9]+\.[0-9]+\.[0-9]+',
        'pgque.version() must return a version string';
    raise notice 'PASS: pgque is registered as an extension and schema is reachable';
end $$;

-- Functional behaviour (produce / tick / receive / ack) is exercised by the
-- regression and acceptance suites running against the pg_tle install path
-- in CI; nothing extra to assert here.

-- drop extension cascade removes the schema and the extension membership.
drop extension pgque cascade;

do $$
begin
    assert not exists (select 1 from pg_catalog.pg_extension where extname = 'pgque'),
        'pgque extension must be gone after drop';
    assert not exists (select 1 from pg_catalog.pg_namespace where nspname = 'pgque'),
        'pgque schema must be gone after drop extension cascade';
    raise notice 'PASS: drop extension pgque cascade removes schema and extension';
end $$;

-- Uninstall script unregisters the version from pg_tle.
\i sql/pgque-pg_tle-uninstall.sql

do $$
begin
    assert not exists (
        select 1 from pgtle.available_extensions() where name = 'pgque'
    ), 'pgque must be unregistered from pg_tle after uninstall script';
    raise notice 'PASS: pg_tle no longer lists pgque after uninstall';
end $$;

-- Re-running the uninstall script must be a no-op (idempotent).
\i sql/pgque-pg_tle-uninstall.sql

do $$
begin
    assert not exists (
        select 1 from pgtle.available_extensions() where name = 'pgque'
    ), 'second uninstall run must remain a no-op';
    raise notice 'PASS: pg_tle uninstall script is idempotent';
end $$;

\echo '=== test_pg_tle_install: ALL PASSED ==='
