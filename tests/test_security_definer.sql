-- test_security_definer.sql -- All SECURITY DEFINER functions have search_path
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
declare
  v_bad record;
  v_count int := 0;
begin
  for v_bad in
    select p.proname
    from pg_proc p
    join pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'pgque'
    and p.prosecdef = true
    and (p.proconfig is null
         or not exists (
           select 1 from unnest(p.proconfig) c
           where c like 'search_path=%'
         ))
  loop
    raise warning 'SECURITY DEFINER without search_path: %', v_bad.proname;
    v_count := v_count + 1;
  end loop;

  assert v_count = 0,
    v_count || ' SECURITY DEFINER function(s) missing search_path';

  raise notice 'PASS: security_definer - all functions have search_path';
end $$;
