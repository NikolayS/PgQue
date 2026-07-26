\set ON_ERROR_STOP on

-- Register a synthetic older pg_tle version with durable state. The current
-- wrapper must reject it because only the 0.2.0 update path is tested.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

select pgtle.install_extension(
  'pgque',
  '0.1.0',
  'unsupported-upgrade fixture',
  $extension_body$
    create table public.tle_unsupported_state (
      value text primary key
    );
    insert into public.tle_unsupported_state values ('preserved');
  $extension_body$
);

create extension pgque;
