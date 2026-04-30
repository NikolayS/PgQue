-- test_security_public_execute.sql -- Regression: PUBLIC EXECUTE revoked from all pgque functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Issue #96: default PUBLIC EXECUTE lets unprivileged roles call writer/admin APIs.
-- This test asserts the deny-by-default posture: an ungranted role must NOT be
-- able to execute any mutating pgque function.
--
-- Red until fix: add "revoke execute on all functions in schema pgque from public;"
-- to sql/pgque-additions/roles.sql (before the explicit role grants).

do $$
begin
  -- Ensure the sentinel role exists and has NO pgque grants.
  if not exists (select 1 from pg_roles where rolname = 'pgque_none_role') then
    execute 'create role pgque_none_role login';
  end if;
end $$;

do $$
declare
  v_func text;
  v_has_execute bool;
  v_violations text[] := '{}';
  -- Functions that must NOT be executable by PUBLIC / ungranted roles.
  -- This list covers all mutating and admin-level APIs.
  funcs text[] := ARRAY[
    'pgque.create_queue(text)',
    'pgque.drop_queue(text)',
    'pgque.drop_queue(text, boolean)',
    'pgque.set_queue_config(text, text, text)',
    'pgque.insert_event(text, text, text)',
    'pgque.insert_event(text, text, text, text, text, text, text)',
    'pgque.send(text, text)',
    'pgque.send(text, jsonb)',
    'pgque.send(text, text, text)',
    'pgque.send(text, text, jsonb)',
    'pgque.send_batch(text, text, text[])',
    'pgque.send_batch(text, text, jsonb[])',
    'pgque.receive(text, text, integer)',
    'pgque.ack(bigint)',
    'pgque.nack(bigint, pgque.message, interval, text)',
    'pgque.subscribe(text, text)',
    'pgque.unsubscribe(text, text)',
    'pgque.register_consumer(text, text)',
    'pgque.unregister_consumer(text, text)',
    'pgque.next_batch(text, text)',
    'pgque.finish_batch(bigint)',
    'pgque.dlq_replay(bigint)',
    'pgque.dlq_replay_all(text)',
    'pgque.dlq_purge(text, interval)',
    'pgque.start()',
    'pgque.stop()',
    'pgque.maint()',
    'pgque.ticker()',
    'pgque.force_tick(text)'
  ];
begin
  foreach v_func in array funcs loop
    select has_function_privilege('pgque_none_role', v_func, 'EXECUTE')
    into v_has_execute;

    if v_has_execute then
      v_violations := array_append(v_violations, v_func);
    end if;
  end loop;

  assert array_length(v_violations, 1) is null,
    'PUBLIC EXECUTE not revoked from ' || array_length(v_violations, 1)::text
    || ' function(s): ' || array_to_string(v_violations, ', ');

  raise notice 'PASS: security_public_execute - PUBLIC EXECUTE revoked from all mutating functions';
end $$;
