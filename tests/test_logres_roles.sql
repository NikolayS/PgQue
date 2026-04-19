-- test_logres_roles.sql -- Verify logres roles exist and modern API grants are present
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  assert exists (select 1 from pg_roles where rolname = 'logres_reader'),
    'logres_reader should exist';
  assert exists (select 1 from pg_roles where rolname = 'logres_writer'),
    'logres_writer should exist';
  assert exists (select 1 from pg_roles where rolname = 'logres_admin'),
    'logres_admin should exist';

  -- send() overloads: jsonb + text at both arities
  assert has_function_privilege('logres_writer', 'logres.send(text, jsonb)', 'EXECUTE'),
    'logres_writer should have execute on send(text, jsonb)';
  assert has_function_privilege('logres_writer', 'logres.send(text, text)', 'EXECUTE'),
    'logres_writer should have execute on send(text, text)';
  assert has_function_privilege('logres_writer', 'logres.send(text, text, jsonb)', 'EXECUTE'),
    'logres_writer should have execute on send(text, text, jsonb)';
  assert has_function_privilege('logres_writer', 'logres.send(text, text, text)', 'EXECUTE'),
    'logres_writer should have execute on send(text, text, text)';

  -- send_batch() overloads: jsonb[] + text[]
  assert has_function_privilege('logres_writer', 'logres.send_batch(text, text, jsonb[])', 'EXECUTE'),
    'logres_writer should have execute on send_batch(text, text, jsonb[])';
  assert has_function_privilege('logres_writer', 'logres.send_batch(text, text, text[])', 'EXECUTE'),
    'logres_writer should have execute on send_batch(text, text, text[])';

  -- subscribe/unsubscribe wrappers
  assert has_function_privilege('logres_writer', 'logres.subscribe(text, text)', 'EXECUTE'),
    'logres_writer should have execute on subscribe(text, text)';
  assert has_function_privilege('logres_writer', 'logres.unsubscribe(text, text)', 'EXECUTE'),
    'logres_writer should have execute on unsubscribe(text, text)';

  -- receive/ack/nack — explicit grants colocated with the function
  -- definitions in sql/logres-api/receive.sql (same convention as send.sql).
  assert has_function_privilege('logres_writer', 'logres.receive(text, text, integer)', 'EXECUTE'),
    'logres_writer should have execute on receive(text, text, integer)';
  assert has_function_privilege('logres_writer', 'logres.ack(bigint)', 'EXECUTE'),
    'logres_writer should have execute on ack(bigint)';
  assert has_function_privilege('logres_writer', 'logres.nack(bigint, logres.message, interval, text)', 'EXECUTE'),
    'logres_writer should have execute on nack(bigint, logres.message, interval, text)';

  -- uninstall() must be superuser-only: execute is revoked from both
  -- logres_admin and PUBLIC. Any non-superuser role (including logres_admin,
  -- logres_writer, logres_reader) should NOT be able to execute it.
  assert not has_function_privilege('logres_admin',  'logres.uninstall()', 'EXECUTE'),
    'logres_admin should NOT have execute on uninstall() (revoked in roles.sql)';
  assert not has_function_privilege('logres_writer', 'logres.uninstall()', 'EXECUTE'),
    'logres_writer should NOT have execute on uninstall() (inherits PUBLIC revoke)';
  assert not has_function_privilege('logres_reader', 'logres.uninstall()', 'EXECUTE'),
    'logres_reader should NOT have execute on uninstall() (inherits PUBLIC revoke)';

  raise notice 'PASS: logres_roles';
end $$;
