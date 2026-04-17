-- pgque security roles
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Create roles idempotently
do $$ begin create role pgque_reader; exception when duplicate_object then null; end $$;
do $$ begin create role pgque_writer; exception when duplicate_object then null; end $$;
do $$ begin create role pgque_admin;  exception when duplicate_object then null; end $$;

-- Inheritance: admin > writer > reader
-- Wrapped in exception handlers for PG14/15 compatibility (no IF NOT EXISTS
-- for role grants until PG16).
do $$ begin
    grant pgque_reader to pgque_writer;
exception when duplicate_object then null;
end $$;
do $$ begin
    grant pgque_writer to pgque_admin;
exception when duplicate_object then null;
end $$;

-- ---------------------------------------------------------------------------
-- Reader: read-only access to schema and information functions
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

-- ---------------------------------------------------------------------------
-- Writer: can produce events and manage consumer lifecycle
-- ---------------------------------------------------------------------------

-- insert_event — 3-arg and 7-arg overloads
grant execute on function pgque.insert_event(text, text, text) to pgque_writer;
grant execute on function pgque.insert_event(text, text, text, text, text, text, text) to pgque_writer;

-- consumer registration
grant execute on function pgque.register_consumer(text, text) to pgque_writer;
grant execute on function pgque.register_consumer_at(text, text, bigint) to pgque_writer;
grant execute on function pgque.unregister_consumer(text, text) to pgque_writer;

-- batch processing
grant execute on function pgque.next_batch(text, text) to pgque_writer;
grant execute on function pgque.next_batch_info(text, text) to pgque_writer;
grant execute on function pgque.next_batch_custom(text, text, interval, int4, interval) to pgque_writer;
grant execute on function pgque.get_batch_events(bigint) to pgque_writer;
grant execute on function pgque.finish_batch(bigint) to pgque_writer;

-- event retry — timestamptz and integer overloads
grant execute on function pgque.event_retry(bigint, bigint, timestamptz) to pgque_writer;
grant execute on function pgque.event_retry(bigint, bigint, integer) to pgque_writer;

-- Note: grants for the modern API wrappers (send*, subscribe, unsubscribe,
-- receive, ack, nack) live colocated with their definitions in
-- sql/pgque-api/*.sql. transform.sh appends pgque-additions/ before
-- pgque-api/, so API-layer grants cannot reference their functions from
-- this file.

-- ---------------------------------------------------------------------------
-- Admin: full access to everything in the pgque schema
-- ---------------------------------------------------------------------------
grant all on schema pgque to pgque_admin;
grant all on all tables in schema pgque to pgque_admin;
grant all on all sequences in schema pgque to pgque_admin;
grant execute on all functions in schema pgque to pgque_admin;

-- uninstall() drops the entire schema — only superuser / schema owner should run it
revoke execute on function pgque.uninstall() from pgque_admin;
