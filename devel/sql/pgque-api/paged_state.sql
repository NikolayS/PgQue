-- pgque-api/paged_state.sql -- Durable state for bounded batch pages
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).

/*
 * One row follows one concrete subscription. Active delivery fields may move
 * between cooperative members, but acknowledgement receipts stay with the
 * member that recorded them.
 */
create table if not exists pgque.page_state (
    queue_id               int4        not null,
    consumer_id            int4        not null,
    active_batch_id        int8        unique,
    prev_tick_id           int8,
    next_tick_id           int8,
    mode                   text        check (mode in ('normal', 'coop', 'partition')),
    acked_event_id         int8,
    acked_page_number      int8        not null default 0,
    pending_token          uuid        unique,
    pending_last_event_id  int8,
    pending_page_size      int4,
    pending_final          boolean,
    pending_worker         text,
    pending_lease_ttl      interval,
    pending_lease_until    timestamptz,
    partition_co_name      text,
    partition_slot         int4,
    partition_n            int4,
    partition_epoch        int8,
    last_ack_token         uuid        unique,
    last_ack_worker        text,
    last_ack_request       jsonb,
    last_ack_finished      boolean,

    primary key (queue_id, consumer_id),
    foreign key (queue_id, consumer_id)
        references pgque.subscription (sub_queue, sub_consumer)
        on delete cascade
);

revoke all on table pgque.page_state from public;
revoke all on table pgque.page_state
    from pgque_reader, pgque_writer, pgque_admin;

/*
 * Serialize behind the active subscription before checking the paging guard.
 * An unknown or already-finished batch remains an unguarded no-op so callers
 * can preserve their existing stale-batch behavior.
 */
create or replace function pgque._assert_unpaged(i_batch_id bigint)
returns void as $$
declare
    v_queue_id int4;
    v_consumer_id int4;
begin
    select
        s.sub_queue,
        s.sub_consumer
    into
        v_queue_id,
        v_consumer_id
    from pgque.subscription as s
    where s.sub_batch = i_batch_id
    for update;
    if not found then
        return;
    end if;

    perform 1
    from pgque.page_state as ps
    where
        ps.queue_id = v_queue_id
        and ps.consumer_id = v_consumer_id
        and ps.active_batch_id is not null
    for update;
    if found then
        raise exception 'batch % is managed by paged delivery', i_batch_id
            using errcode = '55000';
    end if;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

/* Clear delivery state without discarding the most recent ack receipt. */
create or replace function pgque._clear_paged_active(
    i_queue_id int4,
    i_consumer_id int4)
returns void as $$
begin
    update pgque.page_state
    set
        active_batch_id = null,
        prev_tick_id = null,
        next_tick_id = null,
        mode = null,
        acked_event_id = null,
        acked_page_number = 0,
        pending_token = null,
        pending_last_event_id = null,
        pending_page_size = null,
        pending_final = null,
        pending_worker = null,
        pending_lease_ttl = null,
        pending_lease_until = null,
        partition_co_name = null,
        partition_slot = null,
        partition_n = null,
        partition_epoch = null
    where
        queue_id = i_queue_id
        and consumer_id = i_consumer_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

/*
 * Move only active cooperative progress. The destination and victim receipts
 * remain on their original rows. Destination-first locking matches cooperative
 * allocation's current-member-before-victim order.
 */
create or replace function pgque._transfer_paged_active(
    i_queue_id int4,
    i_victim_consumer_id int4,
    i_destination_consumer_id int4,
    i_new_batch_id bigint)
returns void as $$
declare
    v_source pgque.page_state%rowtype;
begin
    insert into pgque.page_state (queue_id, consumer_id)
    values (i_queue_id, i_destination_consumer_id)
    on conflict (queue_id, consumer_id) do nothing;

    perform 1
    from pgque.page_state as ps
    where
        ps.queue_id = i_queue_id
        and ps.consumer_id = i_destination_consumer_id
    for update;

    select ps.*
    into v_source
    from pgque.page_state as ps
    where
        ps.queue_id = i_queue_id
        and ps.consumer_id = i_victim_consumer_id
        and ps.active_batch_id is not null
    for update;
    if not found then
        raise exception 'paged cooperative victim has no active state'
            using errcode = '55000';
    end if;

    update pgque.page_state
    set
        active_batch_id = i_new_batch_id,
        prev_tick_id = v_source.prev_tick_id,
        next_tick_id = v_source.next_tick_id,
        mode = v_source.mode,
        acked_event_id = v_source.acked_event_id,
        acked_page_number = v_source.acked_page_number,
        pending_token = null,
        pending_last_event_id = v_source.pending_last_event_id,
        pending_page_size = v_source.pending_page_size,
        pending_final = v_source.pending_final,
        pending_worker = null,
        pending_lease_ttl = v_source.pending_lease_ttl,
        pending_lease_until = null,
        partition_co_name = v_source.partition_co_name,
        partition_slot = v_source.partition_slot,
        partition_n = v_source.partition_n,
        partition_epoch = v_source.partition_epoch
    where
        queue_id = i_queue_id
        and consumer_id = i_destination_consumer_id;

    perform pgque._clear_paged_active(
        i_queue_id,
        i_victim_consumer_id
    );
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

revoke execute on function pgque._assert_unpaged(bigint)
    from public, pgque_reader, pgque_writer, pgque_admin;
revoke execute on function pgque._clear_paged_active(int4, int4)
    from public, pgque_reader, pgque_writer, pgque_admin;
revoke execute on function pgque._transfer_paged_active(int4, int4, int4, bigint)
    from public, pgque_reader, pgque_writer, pgque_admin;
