-- Test pgque.status() diagnostic dashboard
do $$
declare
  v_row record;
  v_found_pgque bool := false;
  v_found_pg bool := false;
begin
  for v_row in select * from pgque.status()
  loop
    if v_row.component = 'pgque' then
      v_found_pgque := true;
      assert v_row.detail = '1.0.0-dev', 'pgque version should be 1.0.0-dev, got ' || v_row.detail;
    end if;
    if v_row.component = 'postgresql' then
      v_found_pg := true;
    end if;
  end loop;

  assert v_found_pgque, 'should have pgque component';
  assert v_found_pg, 'should have postgresql component';

  raise notice 'PASS: status() returns diagnostic info';
end $$;
