-- Test LISTEN/NOTIFY integration in ticker
-- Verify pg_notify call exists in the ticker function source
do $$
declare
  v_src text;
begin
  -- Check that the ticker function contains pg_notify
  select prosrc into v_src
  from pg_proc p
  join pg_namespace n on p.pronamespace = n.oid
  where n.nspname = 'pgque' and p.proname = 'ticker'
  limit 1;

  assert v_src like '%pg_notify%', 'ticker function should contain pg_notify call';
  assert v_src like '%pgque_%', 'ticker should notify on pgque_ channel prefix';

  raise notice 'PASS: ticker contains pg_notify for LISTEN/NOTIFY';
end $$;
