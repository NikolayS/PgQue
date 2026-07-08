-- pgque dead letter queue (DLQ) -- table + helper functions
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- PgQ has a retry queue but no dead letter queue. pgque adds one.
-- See SPECx.md section 4.5.

-- pgque.dead_letter table
--
-- FK behavior: both dl_queue_id and dl_consumer_id use `on delete cascade`.
-- Rationale:
--   - `pgque.drop_queue()` deletes the pgque.queue row unconditionally. DLQ
--     entries for a dropped queue are meaningless, so cascading them is
--     correct (users who want to preserve the audit trail should call
--     `pgque.dlq_purge` or copy the rows out before dropping the queue).
--   - `pgque.unregister_consumer()` deletes the pgque.consumer row only when
--     the consumer has no other subscriptions. That is an explicit,
--     user-initiated action (not routine maintenance), so cascading the
--     historical DLQ rows tied to that (now-removed) consumer id is the
--     least-surprising default. Same escape hatch: purge/copy first if the
--     audit trail matters.
create table if not exists pgque.dead_letter (
    dl_id           bigserial primary key,
    dl_queue_id     int4 not null references pgque.queue(queue_id)    on delete cascade,
    dl_consumer_id  int4 not null references pgque.consumer(co_id)    on delete cascade,
    dl_time         timestamptz not null default now(),
    dl_reason       text,

    -- Original event fields (copied from event at time of death)
    ev_id           bigint not null,
    ev_time         timestamptz not null,
    ev_txid         xid8,
    ev_retry        int4,
    ev_type         text,
    ev_data         text,
    ev_extra1       text,
    ev_extra2       text,
    ev_extra3       text,
    ev_extra4       text
);

create index if not exists dl_queue_time_idx
    on pgque.dead_letter (dl_queue_id, dl_time);

-- Unique index: one DLQ row per (queue, consumer, original ev_id).
-- Required for idempotent insert in event_dead() (#104).
create unique index if not exists dl_queue_consumer_ev_idx
    on pgque.dead_letter (dl_queue_id, dl_consumer_id, ev_id);

-- pgque.event_dead() -- move event to DLQ (called by nack() when max retries exceeded)
-- The insert uses ON CONFLICT DO NOTHING so that repeated nack() calls for
-- the same terminal message are idempotent (fix for #104).
create or replace function pgque.event_dead(
    i_batch_id bigint,
    i_event_id bigint,
    i_reason text,
    i_ev_time timestamptz,
    i_ev_txid xid8,
    i_ev_retry int4,
    i_ev_type text,
    i_ev_data text,
    i_ev_extra1 text default null,
    i_ev_extra2 text default null,
    i_ev_extra3 text default null,
    i_ev_extra4 text default null)
returns integer as $$
declare
    v_sub record;
begin
    -- Look up subscription from batch. For cooperative subconsumers, route
    -- the DLQ row to the coop_main's co_id rather than the member's. Member
    -- consumer rows are ephemeral (workers come and go); the main is the
    -- persistent consumer-group identity. Anchoring DLQ rows to the member
    -- would let unregister_subconsumer (which deletes the orphan member
    -- consumer row) cascade-delete a freshly inserted DLQ row before commit.
    select
        s.sub_queue,
        coalesce(m.sub_consumer, s.sub_consumer) as sub_consumer
    into v_sub
    from pgque.subscription s
    left join pgque.subscription m
        on s.sub_role = 'coop_member'
        and m.sub_id = s.sub_id
        and m.sub_queue = s.sub_queue
        and m.sub_role = 'coop_main'
    where s.sub_batch = i_batch_id;
    if not found then
        raise exception 'batch not found: %', i_batch_id;
    end if;

    -- Idempotent insert: if the same (queue, consumer, ev_id) tuple already
    -- exists (repeated nack() before ack()), silently skip the duplicate.
    insert into pgque.dead_letter (
        dl_queue_id, dl_consumer_id, dl_reason,
        ev_id, ev_time, ev_txid, ev_retry, ev_type, ev_data,
        ev_extra1, ev_extra2, ev_extra3, ev_extra4)
    values (
        v_sub.sub_queue, v_sub.sub_consumer, i_reason,
        i_event_id, i_ev_time, i_ev_txid, i_ev_retry, i_ev_type, i_ev_data,
        i_ev_extra1, i_ev_extra2, i_ev_extra3, i_ev_extra4)
    on conflict (dl_queue_id, dl_consumer_id, ev_id) do nothing;

    return 1;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.dlq_inspect() -- inspect DLQ entries for a queue
create or replace function pgque.dlq_inspect(
    i_queue_name text, i_limit_count int default 100)
returns setof pgque.dead_letter as $$
begin
    return query
    select dl.*
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where q.queue_name = i_queue_name
    order by dl.dl_time desc
    limit i_limit_count;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.dlq_replay() -- replay a single dead letter event back into the queue
--
-- The initial select locks the dead_letter row (`for update of dl`) so that
-- two concurrent dlq_replay() calls for the same dl_id cannot both pass the
-- existence check and re-enqueue the event twice. The second caller blocks on
-- the row lock; after the first commits its delete, the second's select
-- re-evaluates, finds no row, and raises 'dead letter entry not found'.
create or replace function pgque.dlq_replay(i_dead_letter_id bigint)
returns bigint as $$
declare
    v_dl record;
    v_queue_name text;
    v_new_eid bigint;
begin
    select dl.*, q.queue_name into v_dl
    from pgque.dead_letter dl
    join pgque.queue q on q.queue_id = dl.dl_queue_id
    where dl.dl_id = i_dead_letter_id
    for update of dl;

    if not found then
        raise exception 'dead letter entry not found: %', i_dead_letter_id;
    end if;

    -- Re-insert into the queue
    v_new_eid := pgque.insert_event(v_dl.queue_name, v_dl.ev_type, v_dl.ev_data,
        v_dl.ev_extra1, v_dl.ev_extra2, v_dl.ev_extra3, v_dl.ev_extra4);

    -- Remove from DLQ
    delete from pgque.dead_letter where dl_id = i_dead_letter_id;

    return v_new_eid;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.dlq_replay_all() -- replay all DLQ events for a queue.
--
-- Returns a single row (replayed, failed, first_error). Per-event failures are
-- caught so one bad event does not abort the rest, and surfaced via raise
-- warning (visible at the default log_min_messages = warning, unlike notice
-- which is hidden under many production configs). Callers can check
-- failed > 0 to detect partial success programmatically.
--
-- The loop's select locks each dead_letter row (`for update of dl skip
-- locked`) before replaying it. `skip locked` (rather than blocking) fits the
-- replay-everything semantics: a row locked by a concurrent dlq_replay() or
-- dlq_replay_all() is already being handled by that session, so this call
-- skips it instead of waiting only to replay it twice (the pre-lock race) or
-- to count a guaranteed failure.
--
-- Return-type change from v0.1's bare integer count to a record is a breaking
-- API change accepted at the v0.2 cut. Callers previously doing
--   select pgque.dlq_replay_all('q')          -- returned int
-- should switch to
--   select replayed from pgque.dlq_replay_all('q')
-- or destructure all three columns.
--
-- Drop first so upgrades from v0.1 do not hit "cannot change return type".
drop function if exists pgque.dlq_replay_all(text);
create or replace function pgque.dlq_replay_all(i_queue_name text,
    out replayed bigint, out failed bigint, out first_error text)
returns record as $$
declare
    v_dl record;
begin
    replayed := 0;
    failed := 0;
    first_error := null;

    for v_dl in
        select dl.dl_id, dl.ev_type, dl.ev_data,
               dl.ev_extra1, dl.ev_extra2, dl.ev_extra3, dl.ev_extra4,
               q.queue_name
        from pgque.dead_letter dl
        join pgque.queue q on q.queue_id = dl.dl_queue_id
        where q.queue_name = i_queue_name
        for update of dl skip locked
    loop
        begin
            perform pgque.insert_event(v_dl.queue_name, v_dl.ev_type, v_dl.ev_data,
                v_dl.ev_extra1, v_dl.ev_extra2, v_dl.ev_extra3, v_dl.ev_extra4);
            delete from pgque.dead_letter where dl_id = v_dl.dl_id;
            replayed := replayed + 1;
        exception when others then
            failed := failed + 1;
            if first_error is null then
                first_error := format('dl_id=%s: %s', v_dl.dl_id, sqlerrm);
            end if;
            raise warning 'dlq_replay_all: failed to replay dl_id=%, error: %',
                v_dl.dl_id, sqlerrm;
        end;
    end loop;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- pgque.dlq_purge() -- purge old DLQ entries
create or replace function pgque.dlq_purge(
    i_queue_name text, i_older_than interval default '30 days')
returns integer as $$
declare
    v_cnt integer;
begin
    delete from pgque.dead_letter
    where dl_queue_id = (select queue_id from pgque.queue where queue_name = i_queue_name)
      and dl_time < now() - i_older_than;
    get diagnostics v_cnt = row_count;
    return v_cnt;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- ---------------------------------------------------------------------------
-- Grants
-- ---------------------------------------------------------------------------
-- dlq.sql runs after roles.sql in transform.sh, so role names already exist.
-- However, roles.sql's blanket "grant select on all tables / execute on all
-- functions" passes ran *before* dlq.sql created these objects, so they do
-- NOT cover the DLQ table and functions. The explicit grants below are
-- therefore required (not redundant) for every role mentioned, including
-- pgque_admin.
--
-- dlq_inspect is read-only — available to pgque_reader and above.
-- dlq_replay / dlq_replay_all re-insert events into queues — writer-level
-- because they call insert_event(), the canonical produce primitive.
-- (Replaying a dead-letter is conceptually a produce action: the event ends
-- up back on the queue tail. A pure consumer with only pgque_reader cannot
-- replay; that is intentional.)
-- dlq_purge / event_dead: admin-level operations (purge = data loss,
-- event_dead = internal DLQ hook called from nack()). Granted to pgque_admin
-- explicitly for the reason above.
--
grant select on pgque.dead_letter                           to pgque_reader;
grant all    on pgque.dead_letter                           to pgque_admin;
grant all    on sequence pgque.dead_letter_dl_id_seq        to pgque_admin;

-- Grant to intended roles.
grant execute on function pgque.dlq_inspect(text, int)      to pgque_reader;
grant execute on function pgque.dlq_replay(bigint)          to pgque_writer;
grant execute on function pgque.dlq_replay_all(text)        to pgque_writer;
grant execute on function pgque.event_dead(
    bigint, bigint, text, timestamptz, xid8, int4,
    text, text, text, text, text, text)                     to pgque_admin;
grant execute on function pgque.dlq_purge(text, interval)   to pgque_admin;
