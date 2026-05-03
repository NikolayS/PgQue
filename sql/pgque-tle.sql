-- pgque-tle.sql -- Install PgQue as a pg_tle (Trusted Language Extension).
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
--
-- pg_tle (Trusted Language Extensions, https://github.com/aws/pg_tle) lets
-- PgQue install via create extension without a C extension binary on disk.
--
-- This wrapper reads sql/pgque.sql at runtime via psql's `\set name `cat …``
-- form, so the install body is not duplicated in the repo. RUN FROM THE REPO
-- ROOT (cwd-relative paths):
--
--   psql -d mydb -f sql/pgque-tle.sql
--
-- Then in the target database:
--   create extension pgque;
--
-- Prerequisites:
--   1. pg_tle is loaded (shared_preload_libraries=pg_tle) and
--      create extension pg_tle has been run in this database.
--   2. The current role is a member of pgtle_admin and has CREATEROLE.
--
-- Uninstall: \i sql/pgque-tle-uninstall.sql
--
-- Programmatic install (no psql): read sql/pgque.sql into a string in your
-- language of choice and call:
--   select pgtle.install_extension('pgque', '<version>', '<description>', $body$ … $body$);

\set ON_ERROR_STOP on

-- Step 1: confirm pg_tle is loaded.
do $$
begin
    if not exists (select 1 from pg_catalog.pg_extension where extname = 'pg_tle') then
        raise exception 'pg_tle is not available in this database. '
            'Add pg_tle to shared_preload_libraries (managed providers: '
            'parameter group + reboot; self-hosted: alter system + restart), '
            'then run: create extension pg_tle; '
            'and grant pgtle_admin to the current role.';
    end if;
end $$;

-- Step 2: pre-create the pgque_* roles. Roles are cluster-global and cannot
-- be created from inside a TLE install body, so the body's idempotent role
-- creation only succeeds when the roles already exist.
do $$
begin
    if not exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_reader') then
        create role pgque_reader;
    end if;
    if not exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_writer') then
        create role pgque_writer;
    end if;
    if not exists (select 1 from pg_catalog.pg_roles where rolname = 'pgque_admin') then
        create role pgque_admin;
    end if;
end $$;

-- Step 3: extract the version pgque.version() will return at runtime, so
-- pgtle.install_extension and pgque.version() advertise the same string.
-- The awk anchors to the pgque.version() function so unrelated `return '…'`
-- lines added later cannot shadow it.
\set pgque_version `awk '/create or replace function pgque\.version/ { in_fn=1; next } in_fn && match($0, /return '\''[^'\'']+'\''/) { s=substr($0, RSTART+8, RLENGTH-9); print s; exit }' sql/pgque-additions/lifecycle.sql`

-- Step 4: idempotency guard. install_extension errors if the same version is
-- already registered; we want re-running to be a no-op. psql does not
-- substitute `:'name'` inside dollar-quoted blocks, so the check has to be
-- bare SQL gated by \if/\else outside any do-block.
select case
    when exists (
        select 1 from pgtle.available_extensions()
        where name = 'pgque' and default_version = :'pgque_version'
    ) then 'true' else 'false'
end as pgque_already_registered \gset

\if :pgque_already_registered
    \echo 'pgque ' :pgque_version ' already registered with pg_tle; skipping install_extension().'
\else
    -- Step 5: read the install body from sql/pgque.sql (same file used by the
    -- default \i install path; no duplicated copy in the repo).
    \set pgque_body `cat sql/pgque.sql`

    -- Step 6: register with pg_tle. :'pgque_body' is psql interpolation that
    -- emits a single-quoted SQL literal (with internal quotes doubled), so
    -- the body is delivered safely whatever it contains.
    select pgtle.install_extension(
        'pgque',
        :'pgque_version',
        'PgQue — PgQ Universal Edition (zero-bloat Postgres queue)',
        :'pgque_body'
    );
\endif

\echo ''
\echo 'PgQue ' :pgque_version ' registered with pg_tle.'
\echo 'Run create extension pgque; to materialise the schema in this database.'
