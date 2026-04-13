-- test_pgque_config.sql -- Verify pgque.config table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  -- Config should have exactly 1 row
  assert (select count(*) from pgque.config) = 1, 'config should have 1 row';

  -- Singleton constraint works
  begin
    insert into pgque.config (singleton) values (true);
    assert false, 'should not allow second row';
  exception when unique_violation then
    null; -- expected
  end;

  -- Version function works
  assert pgque.version() = '1.0.0-dev',
    'version should be 1.0.0-dev, got ' || pgque.version();

  raise notice 'PASS: pgque_config';
end $$;
