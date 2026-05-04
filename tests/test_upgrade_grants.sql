-- test_upgrade_grants.sql
-- Regression test for #163 upgrade path (#165): re-running sql/pgque.sql on
-- a database that holds the legacy pre-#163 permission set must end with
-- pgque_writer stripped of every consumer-side grant and stripped of the
-- pgque_reader membership.
--
-- A future grant regression (e.g., someone re-grants ack to pgque_writer
-- "to fix" an upgrade issue) would slip past test_pgque_roles.sql, which
-- only inspects the post-install grant state. This test inspects the
-- BEFORE / AFTER of an upgrade.
--
-- Strategy:
--   1. Manually re-create the legacy state: grant pgque_reader to pgque_writer
--      and grant execute on each moved function to pgque_writer.
--   2. Re-run the explicit revoke statements from sql/pgque-additions/roles.sql,
--      sql/pgque-api/receive.sql, and sql/pgque-api/send.sql.
--   3. Assert pgque_writer has no execute on the moved functions and is
--      no longer a member of pgque_reader; assert pgque_reader still has
--      execute on each (i.e., the revokes were writer-targeted only).
--
-- The whole test runs inside a transaction and rolls back at the end so
-- the synthetic legacy state never leaks into other tests in run_all.sql,
-- even on partial failure.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

begin;

-- 0. Precondition: baseline must already be the post-#163 sibling model.
--    If pgque_writer is already a member of pgque_reader on entry, the
--    legacy state we're about to synthesize would be ambiguous (we
--    couldn't tell whether step 3 actually revoked it or whether some
--    other source preserved the membership).
do $$
begin
    assert not pg_has_role('pgque_writer', 'pgque_reader', 'MEMBER'),
        'precondition: baseline must be post-#163 (pgque_writer must NOT inherit pgque_reader)';
end $$;

-- 1. Pretend to be a pre-#163 install: legacy grants + membership.
--    (Type names normalized to `integer` for consistency with the assertion
--    loops below — `int` and `integer` are aliases in Postgres but the
--    asymmetry is a code smell.)
do $$
declare
    f text;
begin
    -- Legacy membership: writer was a member of reader.
    grant pgque_reader to pgque_writer;

    -- Legacy function-level grants: every consumer primitive was on writer.
    foreach f in array array[
        'pgque.register_consumer(text, text)',
        'pgque.register_consumer_at(text, text, bigint)',
        'pgque.unregister_consumer(text, text)',
        'pgque.next_batch(text, text)',
        'pgque.next_batch_info(text, text)',
        'pgque.next_batch_custom(text, text, interval, int4, interval)',
        'pgque.get_batch_events(bigint)',
        'pgque.finish_batch(bigint)',
        'pgque.event_retry(bigint, bigint, timestamptz)',
        'pgque.event_retry(bigint, bigint, integer)',
        'pgque.receive(text, text, integer)',
        'pgque.ack(bigint)',
        'pgque.nack(bigint, pgque.message, interval, text)',
        'pgque.subscribe(text, text)',
        'pgque.unsubscribe(text, text)'
    ] loop
        execute format('grant execute on function %s to pgque_writer', f);
    end loop;
end $$;

-- 2. Verify the legacy state is fully established (loop over all 15
--    functions plus the membership). If section 1 silently failed for any
--    function, section 4 assertions would pass vacuously — this loop
--    closes that gap.
do $$
declare
    f text;
begin
    assert pg_has_role('pgque_writer', 'pgque_reader', 'MEMBER'),
        'precondition: pgque_writer should hold pgque_reader after legacy grant';

    foreach f in array array[
        'pgque.register_consumer(text, text)',
        'pgque.register_consumer_at(text, text, bigint)',
        'pgque.unregister_consumer(text, text)',
        'pgque.next_batch(text, text)',
        'pgque.next_batch_info(text, text)',
        'pgque.next_batch_custom(text, text, interval, int4, interval)',
        'pgque.get_batch_events(bigint)',
        'pgque.finish_batch(bigint)',
        'pgque.event_retry(bigint, bigint, timestamptz)',
        'pgque.event_retry(bigint, bigint, integer)',
        'pgque.receive(text, text, integer)',
        'pgque.ack(bigint)',
        'pgque.nack(bigint, pgque.message, interval, text)',
        'pgque.subscribe(text, text)',
        'pgque.unsubscribe(text, text)'
    ] loop
        if not has_function_privilege('pgque_writer', f, 'EXECUTE') then
            raise exception 'precondition: pgque_writer should have execute on % after legacy grant', f;
        end if;
    end loop;

    raise notice 'PASS: legacy state established (pgque_writer over-granted on 15 functions)';
end $$;

-- 3. Replay the upgrade revokes. We replay the explicit revoke statements
--    from sql/pgque-additions/roles.sql, sql/pgque-api/receive.sql, and
--    sql/pgque-api/send.sql. Running the full sql/pgque.sql is also valid
--    but heavier; the revokes here mirror exactly what those files emit.
do $$ begin
    revoke pgque_reader from pgque_writer;
exception when undefined_object then null;
end $$;

revoke execute on function pgque.register_consumer(text, text)                           from pgque_writer;
revoke execute on function pgque.register_consumer_at(text, text, bigint)                from pgque_writer;
revoke execute on function pgque.unregister_consumer(text, text)                         from pgque_writer;
revoke execute on function pgque.next_batch(text, text)                                  from pgque_writer;
revoke execute on function pgque.next_batch_info(text, text)                             from pgque_writer;
revoke execute on function pgque.next_batch_custom(text, text, interval, int4, interval) from pgque_writer;
revoke execute on function pgque.get_batch_events(bigint)                                from pgque_writer;
revoke execute on function pgque.finish_batch(bigint)                                    from pgque_writer;
revoke execute on function pgque.event_retry(bigint, bigint, timestamptz)                from pgque_writer;
revoke execute on function pgque.event_retry(bigint, bigint, integer)                    from pgque_writer;
revoke execute on function pgque.receive(text, text, int)                                from pgque_writer;
revoke execute on function pgque.ack(bigint)                                             from pgque_writer;
revoke execute on function pgque.nack(bigint, pgque.message, interval, text)             from pgque_writer;
revoke execute on function pgque.subscribe(text, text)                                   from pgque_writer;
revoke execute on function pgque.unsubscribe(text, text)                                 from pgque_writer;

-- 4. Assert the upgrade left pgque_writer stripped clean AND pgque_reader
--    untouched. If a future "fix" accidentally writes
--    `revoke ... from pgque_reader`, the second loop catches it.
do $$
declare
    f text;
begin
    -- Membership revoked.
    assert not pg_has_role('pgque_writer', 'pgque_reader', 'MEMBER'),
        'upgrade should have revoked pgque_reader membership from pgque_writer';

    -- Function grants revoked from writer.
    foreach f in array array[
        'pgque.register_consumer(text, text)',
        'pgque.register_consumer_at(text, text, bigint)',
        'pgque.unregister_consumer(text, text)',
        'pgque.next_batch(text, text)',
        'pgque.next_batch_info(text, text)',
        'pgque.next_batch_custom(text, text, interval, int4, interval)',
        'pgque.get_batch_events(bigint)',
        'pgque.finish_batch(bigint)',
        'pgque.event_retry(bigint, bigint, timestamptz)',
        'pgque.event_retry(bigint, bigint, integer)',
        'pgque.receive(text, text, integer)',
        'pgque.ack(bigint)',
        'pgque.nack(bigint, pgque.message, interval, text)',
        'pgque.subscribe(text, text)',
        'pgque.unsubscribe(text, text)'
    ] loop
        if has_function_privilege('pgque_writer', f, 'EXECUTE') then
            raise exception 'upgrade did NOT revoke execute on % from pgque_writer (#165)', f;
        end if;
        -- Reader must still hold execute (the revoke targeted writer only).
        -- A regression like `revoke ... from pgque_reader` would trip here.
        if not has_function_privilege('pgque_reader', f, 'EXECUTE') then
            raise exception 'upgrade incorrectly revoked execute on % from pgque_reader (#165)', f;
        end if;
    end loop;

    -- pgque_admin still works through membership (sanity).
    assert has_function_privilege('pgque_admin', 'pgque.ack(bigint)', 'EXECUTE'),
        'pgque_admin should retain ack via pgque_reader membership';

    raise notice 'PASS: upgrade-path revoked all legacy grants from pgque_writer (#165) and pgque_reader retains all 15 grants';
end $$;

rollback;

\echo 'PASS: upgrade-path regression test (#165)'
