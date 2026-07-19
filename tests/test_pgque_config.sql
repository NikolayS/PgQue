-- test_pgque_config.sql -- Verify pgque.config table
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\if :{?expected_pgque_version}
\else
\set expected_pgque_version 0.3.0-devel
\endif

select pg_catalog.set_config(
  'pgque.test_expected_version', :'expected_pgque_version', false
);

do $$
declare
  v_expected_version text := current_setting('pgque.test_expected_version');
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
  assert pgque.version() = v_expected_version,
    format('version should be %s, got %s', v_expected_version, pgque.version());

  raise notice 'PASS: pgque_config';
end $$;

reset pgque.test_expected_version;
