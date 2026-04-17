-- test_pgque_roles.sql -- Verify pgque roles exist and modern API grants are present
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  assert exists (select 1 from pg_roles where rolname = 'pgque_reader'),
    'pgque_reader should exist';
  assert exists (select 1 from pg_roles where rolname = 'pgque_writer'),
    'pgque_writer should exist';
  assert exists (select 1 from pg_roles where rolname = 'pgque_admin'),
    'pgque_admin should exist';

  -- send() overloads: jsonb + text at both arities
  assert has_function_privilege('pgque_writer', 'pgque.send(text, jsonb)', 'EXECUTE'),
    'pgque_writer should have execute on send(text, jsonb)';
  assert has_function_privilege('pgque_writer', 'pgque.send(text, text)', 'EXECUTE'),
    'pgque_writer should have execute on send(text, text)';
  assert has_function_privilege('pgque_writer', 'pgque.send(text, text, jsonb)', 'EXECUTE'),
    'pgque_writer should have execute on send(text, text, jsonb)';
  assert has_function_privilege('pgque_writer', 'pgque.send(text, text, text)', 'EXECUTE'),
    'pgque_writer should have execute on send(text, text, text)';

  -- send_batch() overloads: jsonb[] + text[]
  assert has_function_privilege('pgque_writer', 'pgque.send_batch(text, text, jsonb[])', 'EXECUTE'),
    'pgque_writer should have execute on send_batch(text, text, jsonb[])';
  assert has_function_privilege('pgque_writer', 'pgque.send_batch(text, text, text[])', 'EXECUTE'),
    'pgque_writer should have execute on send_batch(text, text, text[])';

  -- subscribe/unsubscribe wrappers
  assert has_function_privilege('pgque_writer', 'pgque.subscribe(text, text)', 'EXECUTE'),
    'pgque_writer should have execute on subscribe(text, text)';
  assert has_function_privilege('pgque_writer', 'pgque.unsubscribe(text, text)', 'EXECUTE'),
    'pgque_writer should have execute on unsubscribe(text, text)';

  -- receive/ack/nack — explicit grants colocated with the function
  -- definitions in sql/pgque-api/receive.sql (same convention as send.sql).
  assert has_function_privilege('pgque_writer', 'pgque.receive(text, text, integer)', 'EXECUTE'),
    'pgque_writer should have execute on receive(text, text, integer)';
  assert has_function_privilege('pgque_writer', 'pgque.ack(bigint)', 'EXECUTE'),
    'pgque_writer should have execute on ack(bigint)';
  assert has_function_privilege('pgque_writer', 'pgque.nack(bigint, pgque.message, interval, text)', 'EXECUTE'),
    'pgque_writer should have execute on nack(bigint, pgque.message, interval, text)';

  -- uninstall() must be superuser-only: execute is revoked from both
  -- pgque_admin and PUBLIC. Any non-superuser role (including pgque_admin,
  -- pgque_writer, pgque_reader) should NOT be able to execute it.
  assert not has_function_privilege('pgque_admin',  'pgque.uninstall()', 'EXECUTE'),
    'pgque_admin should NOT have execute on uninstall() (revoked in roles.sql)';
  assert not has_function_privilege('pgque_writer', 'pgque.uninstall()', 'EXECUTE'),
    'pgque_writer should NOT have execute on uninstall() (inherits PUBLIC revoke)';
  assert not has_function_privilege('pgque_reader', 'pgque.uninstall()', 'EXECUTE'),
    'pgque_reader should NOT have execute on uninstall() (inherits PUBLIC revoke)';

  raise notice 'PASS: pgque_roles';
end $$;
