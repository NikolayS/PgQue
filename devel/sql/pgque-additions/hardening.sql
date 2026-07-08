-- pgque hardening overrides for #100.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Two findings from the round-2 raw-SQL audit:
--
--   Finding 2: pgque.ticker(queue, tick_id, ts, event_seq) — the external
--   ticker push API — accepted any tick_id / event_seq the (queue, tick_id)
--   PK didn't reject. Non-monotonic input could create ticks that consumers
--   would never reach. The override validates that tick_id is strictly
--   greater than the queue's current max tick_id and event_seq is at least
--   the previous tick's event_seq.
--
--   Finding 3: pgque.force_tick(queue) returned NULL when the queue was
--   missing, paused, or marked external_ticker. Silent NULL is a footgun
--   in scripts and tests. The override raises clear errors instead.

-- Override the 4-arg external ticker with monotonicity checks.
create or replace function pgque.ticker(
    i_queue_name text,
    i_tick_id bigint,
    i_orig_timestamp timestamptz,
    i_event_seq bigint)
returns bigint as $$
declare
    v_queue_id    int4;
    v_paused      bool;
    v_external    bool;
    v_max_tick    bigint;
    v_max_seq     bigint;
begin
    -- Resolve queue and capture validation flags up front.
    select queue_id, queue_ticker_paused, queue_external_ticker
      into v_queue_id, v_paused, v_external
      from pgque.queue
     where queue_name = i_queue_name;
    if not found then
        raise exception 'queue not found: %', i_queue_name;
    end if;
    if v_paused then
        raise exception 'queue % is paused (queue_ticker_paused = true)',
            i_queue_name;
    end if;
    if not v_external then
        raise exception 'queue % is not configured for external ticker '
            '(queue_external_ticker = false); use pgque.ticker(queue) instead',
            i_queue_name;
    end if;

    -- Monotonicity: tick_id must be strictly greater than current max.
    select coalesce(max(tick_id), 0)
      into v_max_tick
      from pgque.tick
     where tick_queue = v_queue_id;
    if i_tick_id <= v_max_tick then
        raise exception 'external ticker tick_id must be strictly greater than current max (% <= %)',
            i_tick_id, v_max_tick;
    end if;

    -- Monotonicity: event_seq must be >= previous tick's event_seq.
    -- Equal is allowed (no new events between ticks); strictly less is a bug.
    select tick_event_seq
      into v_max_seq
      from pgque.tick
     where tick_queue = v_queue_id
     order by tick_id desc
     limit 1;
    if v_max_seq is not null and i_event_seq < v_max_seq then
        raise exception 'external ticker event_seq must be >= previous tick (% < %)',
            i_event_seq, v_max_seq;
    end if;

    -- All checks passed: insert the tick and update sequence state.
    insert into pgque.tick (tick_queue, tick_id, tick_time, tick_event_seq)
    values (v_queue_id, i_tick_id, i_orig_timestamp, i_event_seq);

    perform pgque.seq_setval(queue_tick_seq, i_tick_id),
            pgque.seq_setval(queue_event_seq, i_event_seq)
       from pgque.queue
      where queue_id = v_queue_id;

    perform pg_notify('pgque_' || i_queue_name, i_tick_id::text);
    return i_tick_id;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- Override force_tick: raise instead of silently returning NULL when the
-- target queue is missing, paused, or configured for external ticker.
create or replace function pgque.force_tick(i_queue_name text)
returns bigint as $$
declare
    v_queue_id    int4;
    v_paused      bool;
    v_external    bool;
    v_max_count   int4;
    v_max_tick    bigint;
begin
    select queue_id, queue_ticker_paused, queue_external_ticker, queue_ticker_max_count
      into v_queue_id, v_paused, v_external, v_max_count
      from pgque.queue
     where queue_name = i_queue_name;
    if not found then
        raise exception 'queue not found: %', i_queue_name;
    end if;
    if v_paused then
        raise exception 'queue % is paused (queue_ticker_paused = true)',
            i_queue_name;
    end if;
    if v_external then
        raise exception 'queue % is configured for external ticker; '
            'force_tick is meaningless — push ticks via pgque.ticker(queue, tick_id, ts, event_seq)',
            i_queue_name;
    end if;

    -- Bump event-seq past ticker_max_count so the next pgque.ticker() run ticks.
    perform setval(queue_event_seq, nextval(queue_event_seq) + v_max_count * 2 + 1000)
       from pgque.queue
      where queue_id = v_queue_id;

    -- Return the current last tick id (the one before force_tick took effect).
    -- If the queue has no ticks yet, returns NULL — same as the upstream
    -- behavior on a brand-new queue.
    select tick_id
      into v_max_tick
      from pgque.tick
     where tick_queue = v_queue_id
     order by tick_id desc
     limit 1;
    return v_max_tick;
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;
