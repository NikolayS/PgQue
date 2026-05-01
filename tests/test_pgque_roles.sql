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

  -- subscribe/unsubscribe are consumer-side -> pgque_reader.
  assert has_function_privilege('pgque_reader', 'pgque.subscribe(text, text)', 'EXECUTE'),
    'pgque_reader should have execute on subscribe(text, text)';
  assert has_function_privilege('pgque_reader', 'pgque.unsubscribe(text, text)', 'EXECUTE'),
    'pgque_reader should have execute on unsubscribe(text, text)';

  -- receive/ack/nack are consumer-side -> pgque_reader (mirrors PgQ's split).
  assert has_function_privilege('pgque_reader', 'pgque.receive(text, text, integer)', 'EXECUTE'),
    'pgque_reader should have execute on receive(text, text, integer)';
  assert has_function_privilege('pgque_reader', 'pgque.ack(bigint)', 'EXECUTE'),
    'pgque_reader should have execute on ack(bigint)';
  assert has_function_privilege('pgque_reader', 'pgque.nack(bigint, pgque.message, interval, text)', 'EXECUTE'),
    'pgque_reader should have execute on nack(bigint, pgque.message, interval, text)';

  -- Producer/consumer split (#102, #106): pgque_writer must NOT inherit
  -- consumer-side primitives. Apps that both produce and consume hold both
  -- roles; pure producers cannot ack/finish/inspect another consumer's
  -- batch by id.
  assert not has_function_privilege('pgque_writer', 'pgque.ack(bigint)', 'EXECUTE'),
    'pgque_writer must NOT have execute on ack(bigint) (#102)';
  assert not has_function_privilege('pgque_writer', 'pgque.nack(bigint, pgque.message, interval, text)', 'EXECUTE'),
    'pgque_writer must NOT have execute on nack(...) (#102)';
  assert not has_function_privilege('pgque_writer', 'pgque.receive(text, text, integer)', 'EXECUTE'),
    'pgque_writer must NOT have execute on receive(...) (#106)';
  assert not has_function_privilege('pgque_writer', 'pgque.finish_batch(bigint)', 'EXECUTE'),
    'pgque_writer must NOT have execute on finish_batch(bigint) (#102)';
  assert not has_function_privilege('pgque_writer', 'pgque.next_batch(text, text)', 'EXECUTE'),
    'pgque_writer must NOT have execute on next_batch(text, text) (#106)';
  assert not has_function_privilege('pgque_writer', 'pgque.get_batch_events(bigint)', 'EXECUTE'),
    'pgque_writer must NOT have execute on get_batch_events(bigint) (#106)';
  assert not has_function_privilege('pgque_writer', 'pgque.register_consumer_at(text, text, bigint)', 'EXECUTE'),
    'pgque_writer must NOT have execute on register_consumer_at(...) (#106)';
  assert not has_function_privilege('pgque_writer', 'pgque.event_retry(bigint, bigint, integer)', 'EXECUTE'),
    'pgque_writer must NOT have execute on event_retry(...) (#106)';
  assert not has_function_privilege('pgque_writer', 'pgque.subscribe(text, text)', 'EXECUTE'),
    'pgque_writer must NOT have execute on subscribe(text, text) (#106)';

  -- pgque_admin still inherits both: should retain access to consumer-side
  -- functions via membership in pgque_reader.
  assert has_function_privilege('pgque_admin', 'pgque.ack(bigint)', 'EXECUTE'),
    'pgque_admin should have execute on ack(bigint) via pgque_reader';
  assert has_function_privilege('pgque_admin', 'pgque.send(text, jsonb)', 'EXECUTE'),
    'pgque_admin should have execute on send(text, jsonb) via pgque_writer';

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
