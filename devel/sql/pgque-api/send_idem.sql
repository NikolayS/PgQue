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
) with (fillfactor = 80);

alter table pgque.idem set (fillfactor = 80);

-- GC scans by expiry; global index serves maint_idem(), composite serves maint_idem(queue_name).
create index if not exists idem_expires_at_idx on pgque.idem (expires_at);
create index if not exists idem_queue_expires_at_idx on pgque.idem (queue_id, expires_at);

/* pgque_reader holds a blanket select on pre-existing tables (roles.sql), so
   revoke explicitly regardless of install order. Revokes are idempotent. */
revoke all on table pgque.idem from public;
revoke all on table pgque.idem from pgque_reader, pgque_writer, pgque_admin;

/* PostgreSQL cannot rename function inputs in place. Recreate only exact
 * alpha contracts; modern reinstalls keep OIDs, grants and dependencies. */
do $$
declare
    v_actual_names text[];
    v_dependents text;
    v_function record;
    v_oid oid;
begin
    for v_function in
        select *
        from (values
            ('pgque.send_idem(text,text,text,text,interval,text)',
                array['i_queue', 'i_type', 'i_payload', 'i_idem_key', 'i_ttl', 'i_partition_key']),
            ('pgque.send_idem(text,text,jsonb,text,interval,text)',
                array['i_queue', 'i_type', 'i_payload', 'i_idem_key', 'i_ttl', 'i_partition_key']),
            ('pgque.maint_idem(text)',
                array['i_queue_name'])
        ) as alpha(signature, old_names)
    loop
        v_oid := to_regprocedure(v_function.signature)::oid;
        if v_oid is not null then
            select p.proargnames[1:p.pronargs] into v_actual_names
            from pg_proc as p
            where p.oid = v_oid;
            if v_actual_names = v_function.old_names then
                select string_agg(
                    distinct pg_catalog.pg_describe_object(
                        d.classid, d.objid, d.objsubid), ', ')
                into v_dependents
                from pg_catalog.pg_depend as d
                where d.refclassid = 'pg_catalog.pg_proc'::regclass
                  and d.refobjid = v_oid
                  and d.deptype in ('n', 'a');
                if v_dependents is not null then
                    raise exception 'cannot stabilize named arguments for % because dependent objects exist: %. Drop these dependents, run the transactional upgrade, then recreate them',
                        v_function.signature, v_dependents;
                end if;
                execute 'drop function ' || v_function.signature;
            end if;
        end if;
    end loop;
end $$;

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
    queue_name text, type_name text, payload text, idem_key text,
    ttl interval default '1 hour', partition_key text default null)
returns table (event_id int8, deduped boolean) as $$
#variable_conflict use_column
declare
    i_queue alias for $1;
    i_type alias for $2;
    i_payload alias for $3;
    i_idem_key alias for $4;
    i_ttl alias for $5;
    i_partition_key alias for $6;
    v_queue_id int4;
    v_extra_maint text[];
    v_event_seq text;
    v_data_pfx text;
    v_cur_table int4;
    v_disable_insert boolean;
    v_claimed boolean;
    v_inserted boolean;
    v_event_id int8;
    v_claim_ts timestamptz;
    v_post_claim_ts timestamptz;
    v_expires_at timestamptz;
    v_existing_expires_at timestamptz;
begin
    if i_idem_key is null then
        raise exception 'idem_key must not be null' using errcode = '22023';
    end if;
    if i_idem_key = '' then
        raise exception 'idem_key must not be empty' using errcode = '22023';
    end if;
    if i_ttl is null or not isfinite(i_ttl) or i_ttl <= interval '0' then
        raise exception 'ttl must be a positive finite interval' using errcode = '22023';
    end if;
    v_claim_ts := clock_timestamp();
    v_expires_at := pgque._lease_deadline(v_claim_ts, i_ttl);

    select q.queue_id, q.queue_extra_maint, q.queue_event_seq
    into v_queue_id, v_extra_maint, v_event_seq
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
     * Read-first fast path for live duplicates: FOR KEY SHARE is compatible
     * with concurrent duplicate readers (zero AB-BA live-duplicate deadlocks)
     * AND with FOR NO KEY UPDATE on expired-key takeovers (zero shared-to-
     * exclusive upgrade deadlocks if open transactions retry after expiry),
     * while still waiting on an uncommitted DELETE (FOR UPDATE). Recheck
     * expires_at after acquiring the lock so a waited-on row that expired
     * during the wait falls through to takeover.
     */
    select k.event_id, k.expires_at
    into v_event_id, v_existing_expires_at
    from pgque.idem k
    where k.queue_id = v_queue_id
      and k.idem_key = i_idem_key
      and k.expires_at > clock_timestamp()
    for key share;
    if found and v_existing_expires_at > clock_timestamp() then
        event_id := v_event_id;
        deduped := true;
        return next;
        return;
    end if;

    /*
     * Atomic single-write claim: pre-allocate event_id so uncontended fresh
     * sends write the final claim row in one INSERT (zero dead-on-birth UPDATE).
     * The conditional DO UPDATE only modifies non-key columns (FOR NO KEY UPDATE),
     * letting only an EXPIRED key be reclaimed.
     */
    v_claim_ts := clock_timestamp();
    v_expires_at := pgque._lease_deadline(v_claim_ts, i_ttl);
    v_event_id := nextval(v_event_seq);

    insert into pgque.idem as k (queue_id, idem_key, event_id, expires_at)
    values (v_queue_id, i_idem_key, v_event_id, v_expires_at)
    on conflict (queue_id, idem_key) do update
        set event_id = excluded.event_id,
            /* Evaluated after any conflicting row-lock wait, so a takeover
               always receives a fresh TTL window. */
            expires_at = pgque._lease_deadline(clock_timestamp(), i_ttl)
        where k.expires_at <= clock_timestamp()
    returning true, (xmax = 0) into v_claimed, v_inserted;
    v_post_claim_ts := clock_timestamp();

    if v_claimed then
        /* Re-read current table + disable flag after any lock wait, and append
           directly with the pre-allocated v_event_id (avoiding insert_event_raw's
           unconditional second nextval() that would skew ticker/stats counts). */
        select q.queue_data_pfx, q.queue_cur_table, q.queue_disable_insert
        into v_data_pfx, v_cur_table, v_disable_insert
        from pgque.queue q
        where q.queue_id = v_queue_id;
        if not found then
            raise exception 'queue not found: %', i_queue;
        end if;
        if v_disable_insert then
            if current_setting('session_replication_role') <> 'replica' then
                raise exception 'Insert into queue disallowed';
            end if;
        end if;

        execute 'insert into '
            || pgque.quote_fqname(v_data_pfx || '_' || v_cur_table::text)
            || ' (ev_id, ev_time, ev_owner, ev_retry, ev_type, ev_data, '
            || 'ev_extra1, ev_extra2, ev_extra3, ev_extra4) '
            || 'values ($1, $2, null, null, $3, $4, $5, $6, null, null)'
            using v_event_id, now(), i_type, i_payload, i_partition_key, i_idem_key;

        /* Refresh expires_at only if a fresh INSERT waited on a conflicting
           uncommitted row that disappeared (ON CONFLICT DO UPDATE already
           computes expires_at after any lock wait). Threshold scales with
           i_ttl so sub-10ms TTLs never lose their window after a short wait. */
        if v_inserted and (
            v_post_claim_ts - v_claim_ts > least(interval '10 milliseconds', i_ttl / 2)
        ) then
            update pgque.idem k
            set expires_at = pgque._lease_deadline(clock_timestamp(), i_ttl)
            where k.queue_id = v_queue_id
              and k.idem_key = i_idem_key;
        end if;

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
$$ language plpgsql security definer set search_path = pgque, pg_catalog, pg_temp;
revoke execute on function
    pgque.send_idem(text, text, text, text, interval, text) from public;

/*
 * pgque.send_idem(queue, type, payload jsonb, idem_key, ttl, partition_key)
 * JSON payload variant; opt in with an explicit ::jsonb cast (untyped
 * literals resolve to the text overload -- see send.sql).
 */
create or replace function pgque.send_idem(
    queue_name text, type_name text, payload jsonb, idem_key text,
    ttl interval default '1 hour', partition_key text default null)
returns table (event_id int8, deduped boolean) as $$
#variable_conflict use_column
declare
    i_queue alias for $1;
    i_type alias for $2;
    i_payload alias for $3;
    i_idem_key alias for $4;
    i_ttl alias for $5;
    i_partition_key alias for $6;
begin
    return query
    select s.event_id, s.deduped
    from pgque.send_idem(
        i_queue, i_type, i_payload::text, i_idem_key, i_ttl,
        i_partition_key) s;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog, pg_temp;
revoke execute on function
    pgque.send_idem(text, text, jsonb, text, interval, text) from public;

/*
 * pgque.maint_idem(queue_name) -- reap expired claims for one queue, shaped
 * for the pgque.maint_operations() extra-maint hook. Deletes a bounded batch;
 * returns 1 to request another immediate pass when the batch came back full,
 * else 0. send_idem() registers it in queue_extra_maint automatically; it
 * shares the install owner with maint(), so maint()'s ownership check passes.
 */
create or replace function pgque.maint_idem(queue_name text)
returns integer as $$
#variable_conflict use_column
declare
    i_queue_name alias for $1;
    v_queue_id int4;
    v_deleted integer;
begin
    select queue_id into v_queue_id
    from pgque.queue
    where queue_name = i_queue_name;
    if not found then
        return 0;
    end if;

    with doomed as (
        select d.ctid
        from pgque.idem d
        where d.queue_id = v_queue_id
          and d.expires_at < clock_timestamp()
        limit 10000
        for update skip locked
    )
    delete from pgque.idem k
    using doomed
    where k.ctid = doomed.ctid
      and k.expires_at < clock_timestamp();
    get diagnostics v_deleted = row_count;

    return case when v_deleted >= 10000 then 1 else 0 end;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog, pg_temp;
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
    with doomed as (
        select d.ctid
        from pgque.idem d
        where d.expires_at < clock_timestamp()
        limit 10000
        for update skip locked
    )
    delete from pgque.idem k
    using doomed
    where k.ctid = doomed.ctid
      and k.expires_at < clock_timestamp();
    get diagnostics v_deleted = row_count;

    return case when v_deleted >= 10000 then 1 else 0 end;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog, pg_temp;
revoke execute on function pgque.maint_idem() from public;

-- Grants: send_idem -> pgque_writer (producer); maint_idem -> pgque_admin.
grant execute on function
    pgque.send_idem(text, text, text, text, interval, text)  to pgque_writer;
grant execute on function
    pgque.send_idem(text, text, jsonb, text, interval, text) to pgque_writer;
grant execute on function pgque.maint_idem()     to pgque_admin;
grant execute on function pgque.maint_idem(text) to pgque_admin;
