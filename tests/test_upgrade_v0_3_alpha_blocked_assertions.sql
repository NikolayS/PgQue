\set ON_ERROR_STOP on

-- A blocked alpha rename must be atomic and leave user dependencies usable.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_event_id bigint;
  v_old_oid oid;
begin
  select function_oid into strict v_old_oid
  from public.upgrade_v03_named_functions
  where signature = 'pgque.send(text,text,jsonb,text)';

  assert to_regclass('public.upgrade_v03_named_dependency') is not null,
    'blocked upgrade must preserve the dependent alpha view';
  assert to_regprocedure('pgque.send(text,text,jsonb,text)')::oid = v_old_oid,
    'blocked upgrade must preserve the alpha function identity';
  assert (
    select p.proargnames[1] = 'i_queue'
    from pg_proc as p
    where p.oid = v_old_oid
  ), 'blocked upgrade must leave the alpha input names unchanged';
  assert exists (
    select 1
    from public.upgrade_v03_named_state
  ), 'blocked upgrade must preserve alpha queue state';

  select event_id into strict v_event_id
  from public.upgrade_v03_named_dependency;
  assert v_event_id is not null,
    'dependent alpha view must remain callable after the blocked upgrade';

  drop view public.upgrade_v03_named_dependency;
  raise notice 'PASS: blocked alpha rename preserved function, dependency, and state';
end $$;
