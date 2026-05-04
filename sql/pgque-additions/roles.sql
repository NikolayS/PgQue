-- pgque security roles
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Create roles idempotently.
do $$
begin
    if not exists (select from pg_roles where rolname = 'pgque_reader') then
        create role pgque_reader;
    end if;
    if not exists (select from pg_roles where rolname = 'pgque_writer') then
        create role pgque_writer;
    end if;
    if not exists (select from pg_roles where rolname = 'pgque_admin') then
        create role pgque_admin;
    end if;
end $$;

-- Role hierarchy: pgque_admin inherits both pgque_reader and pgque_writer.
-- pgque_reader and pgque_writer are SIBLINGS, not parent/child — this matches
-- upstream PgQ's `create role pgq_admin in role pgq_reader, pgq_writer;`
-- model.
--
-- Why siblings, not writer-inherits-reader: a producer-only role MUST NOT be
-- able to call consumer-side primitives like finish_batch / ack / next_batch.
-- Otherwise any role that can pgque.send() can also ack any consumer's batch
-- by id (issue #102) and read/mutate other consumers' active batches
-- (issue #106). Apps that both produce and consume must be granted BOTH
-- pgque_reader and pgque_writer explicitly.
--
-- Upgrade path (CRITICAL): pre-#163 installs granted pgque_reader to
-- pgque_writer. Postgres does NOT revoke prior role grants on re-install,
-- so we must do it explicitly. Without this, in-place upgrades silently
-- retain the vulnerable inheritance and the security fix is a no-op.
do $$
begin
    if pg_has_role('pgque_writer', 'pgque_reader', 'member') then
        revoke pgque_reader from pgque_writer;
    end if;
end $$;

-- Grant role hierarchy idempotently. Use explicit membership checks instead
-- of GRANT IF NOT EXISTS so this stays compatible with PG14/15.
do $$
begin
    if not pg_has_role('pgque_admin', 'pgque_reader', 'member') then
        grant pgque_reader to pgque_admin;
    end if;
    if not pg_has_role('pgque_admin', 'pgque_writer', 'member') then
        grant pgque_writer to pgque_admin;
    end if;
end $$;

-- ---------------------------------------------------------------------------
-- Reader: consume events. Includes batch processing primitives — registering
-- consumers, opening/closing batches, retrying events. Mirrors PgQ's
-- pgq_reader role.
-- ---------------------------------------------------------------------------
grant usage on schema pgque to pgque_reader;
grant select on all tables in schema pgque to pgque_reader;

-- get_queue_info — 0-arg (all queues) and 1-arg (single queue)
grant execute on function pgque.get_queue_info() to pgque_reader;
grant execute on function pgque.get_queue_info(text) to pgque_reader;

-- get_consumer_info — 0-arg, 1-arg, 2-arg overloads
grant execute on function pgque.get_consumer_info() to pgque_reader;
grant execute on function pgque.get_consumer_info(text) to pgque_reader;
grant execute on function pgque.get_consumer_info(text, text) to pgque_reader;

-- get_batch_info(bigint)
grant execute on function pgque.get_batch_info(bigint) to pgque_reader;

-- version
grant execute on function pgque.version() to pgque_reader;

-- Upgrade path (CRITICAL): the consumer-side primitives below moved from
-- pgque_writer to pgque_reader in #163. Postgres preserves function-level
-- grants across `create or replace function`, so a re-install on a pre-#163
-- database silently keeps the old pgque_writer grants. Explicitly revoke
-- before re-granting. Each revoke is idempotent (no-op if the grant doesn't
-- exist).
revoke execute on function pgque.register_consumer(text, text) from pgque_writer;
revoke execute on function pgque.register_consumer_at(text, text, bigint) from pgque_writer;
revoke execute on function pgque.unregister_consumer(text, text) from pgque_writer;
revoke execute on function pgque.next_batch(text, text) from pgque_writer;
revoke execute on function pgque.next_batch_info(text, text) from pgque_writer;
revoke execute on function pgque.next_batch_custom(text, text, interval, int4, interval) from pgque_writer;
revoke execute on function pgque.get_batch_events(bigint) from pgque_writer;
revoke execute on function pgque.finish_batch(bigint) from pgque_writer;
revoke execute on function pgque.event_retry(bigint, bigint, timestamptz) from pgque_writer;
revoke execute on function pgque.event_retry(bigint, bigint, integer) from pgque_writer;

-- consumer registration (consumer side: create/move/drop a subscription cursor)
grant execute on function pgque.register_consumer(text, text) to pgque_reader;
grant execute on function pgque.unregister_consumer(text, text) to pgque_reader;

-- batch processing
grant execute on function pgque.next_batch(text, text) to pgque_reader;
grant execute on function pgque.next_batch_info(text, text) to pgque_reader;
grant execute on function pgque.next_batch_custom(text, text, interval, int4, interval) to pgque_reader;
grant execute on function pgque.finish_batch(bigint) to pgque_reader;

-- ---------------------------------------------------------------------------
-- Trusted-operator-only primitives (#106)
--
-- The PgQ-compatible primitives below operate on (queue, consumer) name pairs
-- or on raw batch ids and do NOT validate caller context. That means any role
-- holding the grant can reach into another consumer's active batch / cursor:
--
--   - register_consumer_at(queue, victim, tick) repositions any consumer's
--     cursor (clears sub_batch, rewrites sub_last_tick).
--   - event_retry(batch_id, ev_id, n) pushes any consumer's events into
--     the retry queue.
--   - get_batch_events(batch_id) leaks any consumer's active payloads.
--
-- v0.2.0 mitigation: keep these as pgque_admin only. The high-level API
-- (pgque.receive, pgque.ack, pgque.nack) is SECURITY DEFINER and reaches
-- the primitives via the function owner, so application code that uses
-- the modern API is unaffected. Apps that genuinely need the PgQ-compatible
-- primitive layer must run as pgque_admin (or a role granted pgque_admin).
--
-- Per-queue / per-consumer ACLs would close the consumer-vs-consumer
-- boundary in a finer-grained way; that is a multi-week design exercise
-- and is tracked separately. For v0.2.0 the smallest-blast-radius fix is
-- the trusted-operator gate plus an explicit comment on each function.
-- ---------------------------------------------------------------------------

-- Upgrade path: pre-#106-fix installs granted these to pgque_reader. Postgres
-- preserves function-level grants across `create or replace function`, so a
-- re-install silently retains the old grants. Revoke from pgque_reader
-- explicitly. The earlier revoke from pgque_writer above is also still
-- needed for installs that pre-date #163.
revoke execute on function pgque.register_consumer_at(text, text, bigint) from pgque_reader;
revoke execute on function pgque.get_batch_events(bigint)                 from pgque_reader;
revoke execute on function pgque.event_retry(bigint, bigint, timestamptz) from pgque_reader;
revoke execute on function pgque.event_retry(bigint, bigint, integer)     from pgque_reader;

-- Document the trust contract on the functions themselves so anyone
-- inspecting via \df+ sees it without having to read docs/reference.md.
comment on function pgque.register_consumer_at(text, text, bigint) is
    'Trusted-operator only (pgque_admin). Repositions a consumer cursor and '
    'does not validate caller ownership of the consumer. Apps should use '
    'pgque.subscribe()/pgque.receive() instead.';
comment on function pgque.get_batch_events(bigint) is
    'Trusted-operator only (pgque_admin). Returns the events of any active '
    'batch by id and does not validate caller ownership of the batch. Apps '
    'should use pgque.receive() instead.';
comment on function pgque.event_retry(bigint, bigint, timestamptz) is
    'Trusted-operator only (pgque_admin). Re-queues an event of any active '
    'batch by id and does not validate caller ownership. Apps should use '
    'pgque.nack() instead.';
comment on function pgque.event_retry(bigint, bigint, integer) is
    'Trusted-operator only (pgque_admin). Re-queues an event of any active '
    'batch by id and does not validate caller ownership. Apps should use '
    'pgque.nack() instead.';

-- ---------------------------------------------------------------------------
-- Writer: produce events. Strictly producer-side primitives.
-- ---------------------------------------------------------------------------

-- insert_event — 3-arg and 7-arg overloads
grant execute on function pgque.insert_event(text, text, text) to pgque_writer;
grant execute on function pgque.insert_event(text, text, text, text, text, text, text) to pgque_writer;

-- Note: grants for the modern API wrappers (send*, subscribe, unsubscribe,
-- receive, ack, nack) live colocated with their definitions in
-- sql/pgque-api/*.sql. transform.sh appends pgque-additions/ before
-- pgque-api/, so API-layer grants cannot reference their functions from
-- this file. send* go to pgque_writer; subscribe/unsubscribe/receive/ack/nack
-- go to pgque_reader.

-- Deny-by-default: revoke PUBLIC EXECUTE so role grants below are authoritative.
revoke execute on all functions in schema pgque from public;

-- ---------------------------------------------------------------------------
-- Admin: full access to everything in the pgque schema
-- ---------------------------------------------------------------------------
grant all on schema pgque to pgque_admin;
grant all on all tables in schema pgque to pgque_admin;
grant all on all sequences in schema pgque to pgque_admin;
grant execute on all functions in schema pgque to pgque_admin;

-- uninstall() drops the entire schema — only superuser / schema owner should run it.
-- Revoke from pgque_admin (the "all functions" grant above would otherwise include it).
revoke execute on function pgque.uninstall() from pgque_admin;

-- insert_event_bulk() is an internal primitive for SECURITY DEFINER send_batch()
-- wrappers. It is defined later during a full install, so revoke here only
-- when roles.sql is run after pgque-api/send.sql has already been loaded.
do $$
begin
    if to_regprocedure('pgque.insert_event_bulk(text, text, text[])') is not null then
        revoke execute on function pgque.insert_event_bulk(text, text, text[])
            from public, pgque_reader, pgque_writer, pgque_admin;
    end if;
end $$;


-- get_batch_cursor is an advanced PgQ-compatible primitive.
-- Keep both overloads admin-only; application roles should use pgque.receive().
revoke execute on function pgque.get_batch_cursor(bigint, text, int4)        from public, pgque_reader, pgque_writer;
revoke execute on function pgque.get_batch_cursor(bigint, text, int4, text)  from public, pgque_reader, pgque_writer;
