-- Test LISTEN/NOTIFY integration in ticker
-- Verify pg_notify call exists in the ticker function source
do $$
declare
  v_src text;
  v_found bool := false;
begin
  -- Check that at least one ticker overload contains pg_notify.
  -- The 1-arg ticker(text) handles individual queues and should emit
  -- pg_notify; the zero-arg dispatcher just loops over queues.
  for v_src in
    select prosrc
    from pg_proc p
    join pg_namespace n on p.pronamespace = n.oid
    where n.nspname = 'pgque' and p.proname = 'ticker'
  loop
    if v_src like '%pg_notify%' and v_src like '%pgque_%' then
      v_found := true;
    end if;
  end loop;

  assert v_found, 'at least one ticker overload should contain pg_notify on pgque_ channel';

  raise notice 'PASS: ticker contains pg_notify for LISTEN/NOTIFY';
end $$;
