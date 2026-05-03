-- test_pg_tle_install.sql -- Validate sql/pgque-pg_tle.sql packaging.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Validates that build/transform.sh produced a working pg_tle install file.
-- Runs against a fresh database with a MOCK `pgtle.install_extension` so the
-- test does not require the real pg_tle extension to be installed in CI.
--
-- Run from the repo root, against a database where pgque is NOT installed:
--   psql -d pgque_pgtle_test -v ON_ERROR_STOP=1 -f tests/test_pg_tle_install.sql

\set ON_ERROR_STOP on

\echo '=== test_pg_tle_install ==='

-- Mock pg_tle: capture install_extension() calls into a table so the test
-- can assert on what was registered. The four-argument form matches the
-- real pgtle.install_extension(name, version, description, ext) signature.
drop schema if exists pgtle cascade;
create schema pgtle;

create table pgtle.captured_install (
    name        text,
    version     text,
    description text,
    body        text
);

create function pgtle.install_extension(
    name text, version text, description text, ext text
) returns boolean as $$
begin
    insert into pgtle.captured_install values (name, version, description, ext);
    return true;
end;
$$ language plpgsql;

-- The install script checks for pg_tle by probing pgtle.install_extension(),
-- not by looking at pg_extension, so the mock above is enough to satisfy it.

-- Run the install script.
\i sql/pgque-pg_tle.sql

\echo 'Asserting capture state...'

-- Test 1: install_extension was called exactly once.
do $$
declare
    n int;
begin
    select count(*) into n from pgtle.captured_install;
    assert n = 1, format('expected 1 captured install_extension call, got %s', n);
    raise notice 'PASS: install_extension called exactly once';
end $$;

-- Test 2: the registered extension name is 'pgque'.
do $$
declare
    nm text;
begin
    select name into nm from pgtle.captured_install;
    assert nm = 'pgque', format('expected name=pgque, got %s', nm);
    raise notice 'PASS: extension name is pgque';
end $$;

-- Test 3: the registered version is non-empty and looks like a version string.
do $$
declare
    v text;
begin
    select version into v from pgtle.captured_install;
    assert v is not null and length(v) > 0, 'version must be non-empty';
    assert v ~ '^[0-9]+\.[0-9]+\.[0-9]+', format('version looks malformed: %s', v);
    raise notice 'PASS: version is %', v;
end $$;

-- Test 4: the body contains the core PgQ schema setup.
do $$
declare
    b text;
begin
    select body into b from pgtle.captured_install;
    assert b ~ 'create schema if not exists pgque',
        'body must create the pgque schema';
    assert b ~ 'pgque\.queue', 'body must reference pgque.queue';
    assert b ~ 'pgque\.tick', 'body must reference pgque.tick';
    assert b ~ 'pgque\.consumer', 'body must reference pgque.consumer';
    raise notice 'PASS: body contains core PgQ tables';
end $$;

-- Test 5: the body contains the modern API surface.
do $$
declare
    b text;
begin
    select body into b from pgtle.captured_install;
    assert b ~ 'function pgque\.send',     'body must define pgque.send';
    assert b ~ 'function pgque\.receive',  'body must define pgque.receive';
    assert b ~ 'function pgque\.ack',      'body must define pgque.ack';
    assert b ~ 'function pgque\.subscribe','body must define pgque.subscribe';
    raise notice 'PASS: body contains modern API';
end $$;

-- Test 6: the wrapper script pre-created the pgque_* roles so the body's
-- idempotent role creation is a no-op (CREATE ROLE inside a TLE body is
-- typically blocked when the executing role lacks CREATEROLE).
do $$
begin
    assert exists (select 1 from pg_roles where rolname = 'pgque_reader'),
        'pgque_reader must be pre-created by the wrapper';
    assert exists (select 1 from pg_roles where rolname = 'pgque_writer'),
        'pgque_writer must be pre-created by the wrapper';
    assert exists (select 1 from pg_roles where rolname = 'pgque_admin'),
        'pgque_admin must be pre-created by the wrapper';
    raise notice 'PASS: pgque_* roles pre-created by wrapper';
end $$;

\echo '=== test_pg_tle_install: ALL PASSED ==='
