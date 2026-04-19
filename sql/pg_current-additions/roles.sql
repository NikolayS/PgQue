-- pg_current security roles
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Create roles idempotently
do $$ begin create role pg_current_reader; exception when duplicate_object then null; end $$;
do $$ begin create role pg_current_writer; exception when duplicate_object then null; end $$;
do $$ begin create role pg_current_admin;  exception when duplicate_object then null; end $$;

-- Inheritance: admin > writer > reader
-- Wrapped in exception handlers for PG14/15 compatibility (no IF NOT EXISTS
-- for role grants until PG16).
do $$ begin
    grant pg_current_reader to pg_current_writer;
exception when duplicate_object then null;
end $$;
do $$ begin
    grant pg_current_writer to pg_current_admin;
exception when duplicate_object then null;
end $$;

-- ---------------------------------------------------------------------------
-- Reader: read-only access to schema and information functions
-- ---------------------------------------------------------------------------
grant usage on schema pg_current to pg_current_reader;
grant select on all tables in schema pg_current to pg_current_reader;

-- get_queue_info — 0-arg (all queues) and 1-arg (single queue)
grant execute on function pg_current.get_queue_info() to pg_current_reader;
grant execute on function pg_current.get_queue_info(text) to pg_current_reader;

-- get_consumer_info — 0-arg, 1-arg, 2-arg overloads
grant execute on function pg_current.get_consumer_info() to pg_current_reader;
grant execute on function pg_current.get_consumer_info(text) to pg_current_reader;
grant execute on function pg_current.get_consumer_info(text, text) to pg_current_reader;

-- get_batch_info(bigint)
grant execute on function pg_current.get_batch_info(bigint) to pg_current_reader;

-- version
grant execute on function pg_current.version() to pg_current_reader;

-- ---------------------------------------------------------------------------
-- Writer: can produce events and manage consumer lifecycle
-- ---------------------------------------------------------------------------

-- insert_event — 3-arg and 7-arg overloads
grant execute on function pg_current.insert_event(text, text, text) to pg_current_writer;
grant execute on function pg_current.insert_event(text, text, text, text, text, text, text) to pg_current_writer;

-- consumer registration
grant execute on function pg_current.register_consumer(text, text) to pg_current_writer;
grant execute on function pg_current.register_consumer_at(text, text, bigint) to pg_current_writer;
grant execute on function pg_current.unregister_consumer(text, text) to pg_current_writer;

-- batch processing
grant execute on function pg_current.next_batch(text, text) to pg_current_writer;
grant execute on function pg_current.next_batch_info(text, text) to pg_current_writer;
grant execute on function pg_current.next_batch_custom(text, text, interval, int4, interval) to pg_current_writer;
grant execute on function pg_current.get_batch_events(bigint) to pg_current_writer;
grant execute on function pg_current.finish_batch(bigint) to pg_current_writer;

-- event retry — timestamptz and integer overloads
grant execute on function pg_current.event_retry(bigint, bigint, timestamptz) to pg_current_writer;
grant execute on function pg_current.event_retry(bigint, bigint, integer) to pg_current_writer;

-- Note: grants for the modern API wrappers (send*, subscribe, unsubscribe,
-- receive, ack, nack) live colocated with their definitions in
-- sql/pg_current-api/*.sql. transform.sh appends pg_current-additions/ before
-- pg_current-api/, so API-layer grants cannot reference their functions from
-- this file.

-- ---------------------------------------------------------------------------
-- Admin: full access to everything in the pg_current schema
-- ---------------------------------------------------------------------------
grant all on schema pg_current to pg_current_admin;
grant all on all tables in schema pg_current to pg_current_admin;
grant all on all sequences in schema pg_current to pg_current_admin;
grant execute on all functions in schema pg_current to pg_current_admin;

-- uninstall() drops the entire schema — only superuser / schema owner should run it.
-- SECURITY DEFINER functions default to PUBLIC execute; revoke both PUBLIC and
-- pg_current_admin so the function really is superuser-only.
revoke execute on function pg_current.uninstall() from public;
revoke execute on function pg_current.uninstall() from pg_current_admin;
