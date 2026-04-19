-- Test pg_current.status() diagnostic dashboard
do $$
declare
  v_row record;
  v_found_pg_current bool := false;
  v_found_pg bool := false;
begin
  for v_row in select * from pg_current.status()
  loop
    if v_row.component = 'pg_current' then
      v_found_pg_current := true;
      assert v_row.detail = pg_current.version(), 'pg_current version should be ' || pg_current.version() || ', got ' || v_row.detail;
    end if;
    if v_row.component = 'postgresql' then
      v_found_pg := true;
    end if;
  end loop;

  assert v_found_pg_current, 'should have pg_current component';
  assert v_found_pg, 'should have postgresql component';

  raise notice 'PASS: status() returns diagnostic info';
end $$;
