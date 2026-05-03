-- test_tle_install.sql -- End-to-end pg_tle install path.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Pre-conditions for the caller:
--   - pg_tle binary is loaded (shared_preload_libraries=pg_tle)
--   - the database is fresh (no pgque schema, no pgque extension installed)
--   - the running role is a member of pgtle_admin and has CREATEROLE
--
-- Steps exercised:
--   1. create extension pg_tle
--   2. \i sql/pgque-tle.sql              -- registers pgque with pg_tle
--   3. \i sql/pgque-tle.sql              -- second run is a no-op (idempotent)
--   4. create extension pgque                -- materialises the schema
--   5. assert pg_extension membership and that pg_tle's catalog version
--      matches pgque.version()
--   6. drop extension pgque cascade          -- clean uninstall
--   7. \i sql/pgque-tle-uninstall.sql     -- unregister from pg_tle (twice
--      to confirm the uninstall script is also idempotent)
--
-- Run from the repo root:
--   psql -d pgque_tle_test -v ON_ERROR_STOP=1 -f tests/test_tle_install.sql

\set ON_ERROR_STOP on

\echo '=== test_tle_install (e2e against real pg_tle) ==='

create extension if not exists pg_tle;

\i sql/pgque-tle.sql

-- Re-running the wrapper must be a no-op so users can rerun a deployment
-- script without hitting "extension version already installed" from
-- pgtle.install_extension().
\i sql/pgque-tle.sql

-- pgque must show up in the pg_tle catalog before we materialise the schema,
-- and at exactly one version (no duplicate registration from the second run).
do $$
declare
    v text;
    n int;
begin
    select count(*), max(default_version) into n, v
    from pgtle.available_extensions()
    where name = 'pgque';
    assert n = 1, format('expected 1 pgque registration, found %s', n);
    assert v is not null, 'pgque must appear in pgtle.available_extensions()';
    assert v ~ '^[0-9]+\.[0-9]+\.[0-9]+',
        format('pgque version looks malformed: %s', v);
    raise notice 'PASS: pgque registered with pg_tle as version % (idempotent)', v;
end $$;

create extension pgque;

-- pgque is now visible in pg_extension; the schema / core tables / public
-- version() function are reachable; and the version registered with pg_tle
-- matches what pgque.version() returns at runtime (so the wrapper cannot
-- silently advertise an out-of-date version).
do $$
declare
    catalog_version text;
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

    select default_version into catalog_version
    from pgtle.available_extensions()
    where name = 'pgque';
    assert catalog_version = pgque.version(),
        format('pg_tle catalog version (%s) must match pgque.version() (%s)',
               catalog_version, pgque.version());
    raise notice 'PASS: pgque registered, schema reachable, catalog version matches pgque.version()';
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
\i sql/pgque-tle-uninstall.sql

do $$
begin
    assert not exists (
        select 1 from pgtle.available_extensions() where name = 'pgque'
    ), 'pgque must be unregistered from pg_tle after uninstall script';
    raise notice 'PASS: pg_tle no longer lists pgque after uninstall';
end $$;

-- Re-running the uninstall script must be a no-op (idempotent).
\i sql/pgque-tle-uninstall.sql

do $$
begin
    assert not exists (
        select 1 from pgtle.available_extensions() where name = 'pgque'
    ), 'second uninstall run must remain a no-op';
    raise notice 'PASS: pg_tle uninstall script is idempotent';
end $$;

-- Roles are deliberately not dropped by uninstall (they may be in use by
-- other databases on the cluster); confirm the contract.
do $$
begin
    assert exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_reader'),
        'pgque_reader must survive uninstall';
    assert exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_writer'),
        'pgque_writer must survive uninstall';
    assert exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_admin'),
        'pgque_admin must survive uninstall';
    raise notice 'PASS: pgque_* roles persist after uninstall (cluster-global by design)';
end $$;

\echo '=== test_tle_install: ALL PASSED ==='
