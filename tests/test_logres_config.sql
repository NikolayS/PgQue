-- test_logres_config.sql -- Verify logres.config table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  -- Config should have exactly 1 row
  assert (select count(*) from logres.config) = 1, 'config should have 1 row';

  -- Singleton constraint works
  begin
    insert into logres.config (singleton) values (true);
    assert false, 'should not allow second row';
  exception when unique_violation then
    null; -- expected
  end;

  -- Version function works
  assert logres.version() = '1.0.0-dev',
    'version should be 1.0.0-dev, got ' || logres.version();

  raise notice 'PASS: logres_config';
end $$;
