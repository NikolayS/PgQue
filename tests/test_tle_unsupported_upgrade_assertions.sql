\set ON_ERROR_STOP on

-- A refused pg_tle update must preserve the installed version and its data.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  assert (
    select extversion = '0.1.0'
    from pg_catalog.pg_extension
    where extname = 'pgque'
  ), 'refused update must preserve the installed extension version';
  assert (
    select default_version = '0.1.0'
    from pgtle.available_extensions()
    where name = 'pgque'
  ), 'refused update must preserve the registered default version';
  assert (
    select value = 'preserved'
    from public.tle_unsupported_state
  ), 'refused update must preserve extension-owned data';

  raise notice 'PASS: unsupported pg_tle update failed without changing state';
end $$;
