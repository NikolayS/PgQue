-- logres security roles
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Create roles idempotently
do $$ begin create role logres_reader; exception when duplicate_object then null; end $$;
do $$ begin create role logres_writer; exception when duplicate_object then null; end $$;
do $$ begin create role logres_admin;  exception when duplicate_object then null; end $$;

-- Inheritance: admin > writer > reader
-- Wrapped in exception handlers for PG14/15 compatibility (no IF NOT EXISTS
-- for role grants until PG16).
do $$ begin
    grant logres_reader to logres_writer;
exception when duplicate_object then null;
end $$;
do $$ begin
    grant logres_writer to logres_admin;
exception when duplicate_object then null;
end $$;

-- ---------------------------------------------------------------------------
-- Reader: read-only access to schema and information functions
-- ---------------------------------------------------------------------------
grant usage on schema logres to logres_reader;
grant select on all tables in schema logres to logres_reader;

-- get_queue_info — 0-arg (all queues) and 1-arg (single queue)
grant execute on function logres.get_queue_info() to logres_reader;
grant execute on function logres.get_queue_info(text) to logres_reader;

-- get_consumer_info — 0-arg, 1-arg, 2-arg overloads
grant execute on function logres.get_consumer_info() to logres_reader;
grant execute on function logres.get_consumer_info(text) to logres_reader;
grant execute on function logres.get_consumer_info(text, text) to logres_reader;

-- get_batch_info(bigint)
grant execute on function logres.get_batch_info(bigint) to logres_reader;

-- version
grant execute on function logres.version() to logres_reader;

-- ---------------------------------------------------------------------------
-- Writer: can produce events and manage consumer lifecycle
-- ---------------------------------------------------------------------------

-- insert_event — 3-arg and 7-arg overloads
grant execute on function logres.insert_event(text, text, text) to logres_writer;
grant execute on function logres.insert_event(text, text, text, text, text, text, text) to logres_writer;

-- consumer registration
grant execute on function logres.register_consumer(text, text) to logres_writer;
grant execute on function logres.register_consumer_at(text, text, bigint) to logres_writer;
grant execute on function logres.unregister_consumer(text, text) to logres_writer;

-- batch processing
grant execute on function logres.next_batch(text, text) to logres_writer;
grant execute on function logres.next_batch_info(text, text) to logres_writer;
grant execute on function logres.next_batch_custom(text, text, interval, int4, interval) to logres_writer;
grant execute on function logres.get_batch_events(bigint) to logres_writer;
grant execute on function logres.finish_batch(bigint) to logres_writer;

-- event retry — timestamptz and integer overloads
grant execute on function logres.event_retry(bigint, bigint, timestamptz) to logres_writer;
grant execute on function logres.event_retry(bigint, bigint, integer) to logres_writer;

-- Note: grants for the modern API wrappers (send*, subscribe, unsubscribe,
-- receive, ack, nack) live colocated with their definitions in
-- sql/logres-api/*.sql. transform.sh appends logres-additions/ before
-- logres-api/, so API-layer grants cannot reference their functions from
-- this file.

-- ---------------------------------------------------------------------------
-- Admin: full access to everything in the logres schema
-- ---------------------------------------------------------------------------
grant all on schema logres to logres_admin;
grant all on all tables in schema logres to logres_admin;
grant all on all sequences in schema logres to logres_admin;
grant execute on all functions in schema logres to logres_admin;

-- uninstall() drops the entire schema — only superuser / schema owner should run it.
-- SECURITY DEFINER functions default to PUBLIC execute; revoke both PUBLIC and
-- logres_admin so the function really is superuser-only.
revoke execute on function logres.uninstall() from public;
revoke execute on function logres.uninstall() from logres_admin;
