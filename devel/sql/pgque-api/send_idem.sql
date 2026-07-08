-- pgque-api/send_idem.sql -- Producer idempotency (TTL dedup window)
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).

/*
 * Producer-side dedup (blueprints/idempotency/SPEC.md, Phase 1B): a send
 * whose idem_key was already used for the same queue within the TTL window
 * appends nothing and returns the original event id with deduped = true.
 * Composes with partition keys via send_idem(..., partition_key); neither
 * feature requires the other.
 *
 * Key scope: dedup is an EXACT match on (queue, idem_key). The key must
 * encode the intended EFFECT, not just the entity -- keying on the entity
 * alone silently suppresses a later, different send that reuses the key
 * inside the window. The TTL collapses a burst of the SAME effect; it is
 * not a rate limiter for distinct effects.
 */

/*
 * Internal claim table: one row per live (queue, idem_key) window. App roles
 * never touch it -- access goes through the SECURITY DEFINER functions below.
 * Live size ~= produce_rate * ttl.
 */
create table if not exists pgque.idem (
    queue_id   int4        not null references pgque.queue (queue_id)
                           on delete cascade,
    idem_key   text        not null,
    event_id   int8,
    expires_at timestamptz not null,
    primary key (queue_id, idem_key)
);

-- GC scans by expiry; serves both the whole-table and per-queue sweeps.
create index if not exists idem_expires_at_idx on pgque.idem (expires_at);

/* pgque_reader holds a blanket select on pre-existing tables (roles.sql), so
   revoke explicitly regardless of install order. Revokes are idempotent. */
revoke all on table pgque.idem from public;
revoke all on table pgque.idem from pgque_reader, pgque_writer, pgque_admin;

/*
 * pgque.send_idem(queue, type, payload text, idem_key, ttl, partition_key)
 * Fast path, opaque textual payload (same conventions as pgque.send(text)).
 * Returns one row: deduped = false with a new event id on first use in the
 * window, deduped = true with the original id on a duplicate.
 *
 * Concurrent producers racing one key resolve to a single insert: the
 * (queue_id, idem_key) primary key plus one atomic upsert serialize them --
 * no advisory lock, no subtransaction. Claim and append share one
 * transaction, so a failed append rolls the claim back and the key stays
 * usable (a crash can never leave a claimed key with no event).
 */
create or replace function pgque.send_idem(
    i_queue text, i_type text, i_payload text, i_idem_key text,
    i_ttl interval default '1 hour', i_partition_key text default null)
returns table (event_id int8, deduped boolean) as $$
declare
    v_queue_id int4;
    v_extra_maint text[];
    v_claimed boolean;
    v_event_id int8;
begin
    if i_idem_key is null then
        raise exception 'idem_key must not be null';
    end if;
    if i_ttl is null or i_ttl <= interval '0' then
        raise exception 'ttl must be a positive interval';
    end if;

    select q.queue_id, q.queue_extra_maint
    into v_queue_id, v_extra_maint
    from pgque.queue q
    where q.queue_name = i_queue;
    if not found then
        raise exception 'queue not found: %', i_queue;
    end if;

    /*
     * Register pgque.maint_idem in this queue's queue_extra_maint so the
     * stock maint() runner reaps expired claims. The one-time update
     * re-checks under the row lock, so concurrent first-sends stay safe.
     */
    if not (coalesce(v_extra_maint, '{}'::text[]) @> array['pgque.maint_idem']) then
        update pgque.queue q
        set queue_extra_maint =
            coalesce(q.queue_extra_maint, '{}'::text[])
            || array['pgque.maint_idem']
        where q.queue_id = v_queue_id
          and not (coalesce(q.queue_extra_maint, '{}'::text[])
                   @> array['pgque.maint_idem']);
    end if;

    /*
     * Atomic claim: the unique key serializes concurrent producers; the
     * conditional do-update lets only an EXPIRED key be reclaimed.
     */
    insert into pgque.idem as k (queue_id, idem_key, event_id, expires_at)
    values (v_queue_id, i_idem_key, null, now() + i_ttl)
    on conflict (queue_id, idem_key) do update
        set event_id = excluded.event_id,
            expires_at = excluded.expires_at
        where k.expires_at <= now()
    returning true into v_claimed;

    if v_claimed then
        /* Append in the same transaction as the claim: if this fails, the
           claim rolls back with it and the key stays usable. */
        v_event_id := pgque.insert_event(
            i_queue, i_type, i_payload, i_partition_key, i_idem_key,
            null, null);

        -- Record the id for later dedup responses (contention-free: this
        -- transaction already holds the row lock, invisible to others until
        -- the claim+append pair commits).
        update pgque.idem k
        set event_id = v_event_id
        where k.queue_id = v_queue_id
          and k.idem_key = i_idem_key;

        event_id := v_event_id;
        deduped := false;
    else
        /* Dedup: report the original event id. It is null only in the corner
           where the claim expired and was GC'd between these two statements
           (sub-second TTLs); deduped still stays true. */
        select k.event_id into v_event_id
        from pgque.idem k
        where k.queue_id = v_queue_id
          and k.idem_key = i_idem_key;

        event_id := v_event_id;
        deduped := true;
    end if;
    return next;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function
    pgque.send_idem(text, text, text, text, interval, text) from public;

/*
 * pgque.send_idem(queue, type, payload jsonb, idem_key, ttl, partition_key)
 * JSON payload variant; opt in with an explicit ::jsonb cast (untyped
 * literals resolve to the text overload -- see send.sql).
 */
create or replace function pgque.send_idem(
    i_queue text, i_type text, i_payload jsonb, i_idem_key text,
    i_ttl interval default '1 hour', i_partition_key text default null)
returns table (event_id int8, deduped boolean) as $$
begin
    return query
    select s.event_id, s.deduped
    from pgque.send_idem(
        i_queue, i_type, i_payload::text, i_idem_key, i_ttl,
        i_partition_key) s;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function
    pgque.send_idem(text, text, jsonb, text, interval, text) from public;

/*
 * pgque.maint_idem(queue_name) -- reap expired claims for one queue, shaped
 * for the pgque.maint_operations() extra-maint hook. Deletes a bounded batch;
 * returns 1 to request another immediate pass when the batch came back full,
 * else 0. send_idem() registers it in queue_extra_maint automatically; it
 * shares the install owner with maint(), so maint()'s ownership check passes.
 */
create or replace function pgque.maint_idem(i_queue_name text)
returns integer as $$
declare
    v_queue_id int4;
    v_deleted integer;
begin
    select queue_id into v_queue_id
    from pgque.queue
    where queue_name = i_queue_name;
    if not found then
        return 0;
    end if;

    delete from pgque.idem k
    where (k.queue_id, k.idem_key) in (
        select d.queue_id, d.idem_key
        from pgque.idem d
        where d.queue_id = v_queue_id
          and d.expires_at < now()
        limit 10000);
    get diagnostics v_deleted = row_count;

    return case when v_deleted >= 10000 then 1 else 0 end;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.maint_idem(text) from public;

/*
 * pgque.maint_idem() -- reap expired claims across all queues, for operators
 * scheduling GC directly (pg_cron or external). Same bounded-batch contract:
 * returns 1 when a full batch was deleted (call again), 0 when drained.
 */
create or replace function pgque.maint_idem()
returns integer as $$
declare
    v_deleted integer;
begin
    delete from pgque.idem k
    where (k.queue_id, k.idem_key) in (
        select d.queue_id, d.idem_key
        from pgque.idem d
        where d.expires_at < now()
        limit 10000);
    get diagnostics v_deleted = row_count;

    return case when v_deleted >= 10000 then 1 else 0 end;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
revoke execute on function pgque.maint_idem() from public;

-- Grants: send_idem -> pgque_writer (producer); maint_idem -> pgque_admin.
grant execute on function
    pgque.send_idem(text, text, text, text, interval, text)  to pgque_writer;
grant execute on function
    pgque.send_idem(text, text, jsonb, text, interval, text) to pgque_writer;
grant execute on function pgque.maint_idem()     to pgque_admin;
grant execute on function pgque.maint_idem(text) to pgque_admin;
