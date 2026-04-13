-- test_pgque_roles.sql -- Verify pgque roles exist
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  assert exists (select 1 from pg_roles where rolname = 'pgque_reader'),
    'pgque_reader should exist';
  assert exists (select 1 from pg_roles where rolname = 'pgque_writer'),
    'pgque_writer should exist';
  assert exists (select 1 from pg_roles where rolname = 'pgque_admin'),
    'pgque_admin should exist';

  raise notice 'PASS: pgque_roles';
end $$;
