-- Test logres.status() diagnostic dashboard
do $$
declare
  v_row record;
  v_found_logres bool := false;
  v_found_pg bool := false;
begin
  for v_row in select * from logres.status()
  loop
    if v_row.component = 'logres' then
      v_found_logres := true;
      assert v_row.detail = logres.version(), 'logres version should be ' || logres.version() || ', got ' || v_row.detail;
    end if;
    if v_row.component = 'postgresql' then
      v_found_pg := true;
    end if;
  end loop;

  assert v_found_logres, 'should have logres component';
  assert v_found_pg, 'should have postgresql component';

  raise notice 'PASS: status() returns diagnostic info';
end $$;
