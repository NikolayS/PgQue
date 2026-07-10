\set ON_ERROR_STOP on

-- Stable reinstalls must preserve OIDs, dependencies, ownership, and grants.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_function record;
  v_new_oid oid;
  v_new_owner name;
begin
  for v_function in
    select *
    from public.named_v03_stable_functions
    order by signature
  loop
    v_new_oid := to_regprocedure(v_function.signature)::oid;
    select pg_get_userbyid(p.proowner) into v_new_owner
    from pg_proc as p
    where p.oid = v_new_oid;

    assert v_new_oid = v_function.function_oid,
      format('%s OID changed on stable reinstall: %s -> %s',
        v_function.signature, v_function.function_oid, v_new_oid);
    assert v_new_owner = v_function.owner_name,
      format('%s owner changed on stable reinstall', v_function.signature);
    assert has_function_privilege(
      'pgque_reader', v_new_oid, 'execute') = v_function.reader_execute,
      format('%s pgque_reader grant changed on stable reinstall', v_function.signature);
    assert has_function_privilege(
      'pgque_writer', v_new_oid, 'execute') = v_function.writer_execute,
      format('%s pgque_writer grant changed on stable reinstall', v_function.signature);
    assert has_function_privilege(
      'pgque_admin', v_new_oid, 'execute') = v_function.admin_execute,
      format('%s pgque_admin grant changed on stable reinstall', v_function.signature);
  end loop;

  assert (
    select result = 0
    from public.named_v03_function_dependency
  ), 'dependent view must survive and remain callable after stable reinstall';

  drop view public.named_v03_function_dependency;
  drop table public.named_v03_stable_functions;
  raise notice 'PASS: stable reinstall preserved named API identities and dependency';
end $$;
