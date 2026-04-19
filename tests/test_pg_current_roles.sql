-- test_pg_current_roles.sql -- Verify pg_current roles exist and modern API grants are present
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  assert exists (select 1 from pg_roles where rolname = 'pg_current_reader'),
    'pg_current_reader should exist';
  assert exists (select 1 from pg_roles where rolname = 'pg_current_writer'),
    'pg_current_writer should exist';
  assert exists (select 1 from pg_roles where rolname = 'pg_current_admin'),
    'pg_current_admin should exist';

  -- send() overloads: jsonb + text at both arities
  assert has_function_privilege('pg_current_writer', 'pg_current.send(text, jsonb)', 'EXECUTE'),
    'pg_current_writer should have execute on send(text, jsonb)';
  assert has_function_privilege('pg_current_writer', 'pg_current.send(text, text)', 'EXECUTE'),
    'pg_current_writer should have execute on send(text, text)';
  assert has_function_privilege('pg_current_writer', 'pg_current.send(text, text, jsonb)', 'EXECUTE'),
    'pg_current_writer should have execute on send(text, text, jsonb)';
  assert has_function_privilege('pg_current_writer', 'pg_current.send(text, text, text)', 'EXECUTE'),
    'pg_current_writer should have execute on send(text, text, text)';

  -- send_batch() overloads: jsonb[] + text[]
  assert has_function_privilege('pg_current_writer', 'pg_current.send_batch(text, text, jsonb[])', 'EXECUTE'),
    'pg_current_writer should have execute on send_batch(text, text, jsonb[])';
  assert has_function_privilege('pg_current_writer', 'pg_current.send_batch(text, text, text[])', 'EXECUTE'),
    'pg_current_writer should have execute on send_batch(text, text, text[])';

  -- subscribe/unsubscribe wrappers
  assert has_function_privilege('pg_current_writer', 'pg_current.subscribe(text, text)', 'EXECUTE'),
    'pg_current_writer should have execute on subscribe(text, text)';
  assert has_function_privilege('pg_current_writer', 'pg_current.unsubscribe(text, text)', 'EXECUTE'),
    'pg_current_writer should have execute on unsubscribe(text, text)';

  -- receive/ack/nack — explicit grants colocated with the function
  -- definitions in sql/pg_current-api/receive.sql (same convention as send.sql).
  assert has_function_privilege('pg_current_writer', 'pg_current.receive(text, text, integer)', 'EXECUTE'),
    'pg_current_writer should have execute on receive(text, text, integer)';
  assert has_function_privilege('pg_current_writer', 'pg_current.ack(bigint)', 'EXECUTE'),
    'pg_current_writer should have execute on ack(bigint)';
  assert has_function_privilege('pg_current_writer', 'pg_current.nack(bigint, pg_current.message, interval, text)', 'EXECUTE'),
    'pg_current_writer should have execute on nack(bigint, pg_current.message, interval, text)';

  -- uninstall() must be superuser-only: execute is revoked from both
  -- pg_current_admin and PUBLIC. Any non-superuser role (including pg_current_admin,
  -- pg_current_writer, pg_current_reader) should NOT be able to execute it.
  assert not has_function_privilege('pg_current_admin',  'pg_current.uninstall()', 'EXECUTE'),
    'pg_current_admin should NOT have execute on uninstall() (revoked in roles.sql)';
  assert not has_function_privilege('pg_current_writer', 'pg_current.uninstall()', 'EXECUTE'),
    'pg_current_writer should NOT have execute on uninstall() (inherits PUBLIC revoke)';
  assert not has_function_privilege('pg_current_reader', 'pg_current.uninstall()', 'EXECUTE'),
    'pg_current_reader should NOT have execute on uninstall() (inherits PUBLIC revoke)';

  raise notice 'PASS: pg_current_roles';
end $$;
