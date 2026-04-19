-- test_pg_current_config.sql -- Verify pg_current.config table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  -- Config should have exactly 1 row
  assert (select count(*) from pg_current.config) = 1, 'config should have 1 row';

  -- Singleton constraint works
  begin
    insert into pg_current.config (singleton) values (true);
    assert false, 'should not allow second row';
  exception when unique_violation then
    null; -- expected
  end;

  -- Version function works
  assert pg_current.version() = '1.0.0-dev',
    'version should be 1.0.0-dev, got ' || pg_current.version();

  raise notice 'PASS: pg_current_config';
end $$;
