-- test_security_public_execute.sql -- Regression: PUBLIC EXECUTE revoked from all pgque functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Issue #96: default PUBLIC EXECUTE lets unprivileged roles call writer/admin APIs.
-- This test asserts the deny-by-default posture: an ungranted role must NOT be
-- able to execute any mutating pgque function.
--
-- Red until fix: add "revoke execute on all functions in schema pgque from public;"
-- to devel/sql/pgque-additions/roles.sql (before the explicit role grants).

do $$
begin
  -- Ensure the sentinel role exists and has NO pgque grants.
  if not exists (select 1 from pg_roles where rolname = 'pgque_none_role') then
    execute 'create role pgque_none_role login';
  end if;
end $$;

do $$
declare
  v_violations int;
  v_names text;
begin
  select count(*),
         string_agg(p.proname || '(' || pg_catalog.pg_get_function_arguments(p.oid) || ')', ', ')
    into v_violations, v_names
    from pg_proc p
    join pg_namespace n on n.oid = p.pronamespace
   where n.nspname = 'pgque'
     and has_function_privilege('pgque_none_role', p.oid, 'EXECUTE');

  assert v_violations = 0,
    'PUBLIC EXECUTE not revoked from ' || v_violations::text || ' pgque function(s): ' || v_names;

  raise notice 'PASS: security_public_execute - PUBLIC EXECUTE revoked from all pgque functions';
end $$;
