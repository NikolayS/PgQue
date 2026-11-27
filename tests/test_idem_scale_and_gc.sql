-- test_idem_scale_and_gc.sql -- Idempotency single-write, validation, and looped GC
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

/* 1. fillfactor=80, (queue_id, expires_at) index, and single-write fresh send_idem. */
do $$
declare
    v_opts text[];
    v_eid bigint;
    v_dedup boolean;
    v_idem_cmin int;
    v_ev_cmin int;
    v_table text;
begin
    select c.reloptions into v_opts
    from pg_catalog.pg_class as c
    join pg_catalog.pg_namespace as n on n.oid = c.relnamespace
    where n.nspname = 'pgque' and c.relname = 'idem';

    assert v_opts is not null and 'fillfactor=80' = any(v_opts),
        'pgque.idem must have fillfactor=80, got: ' || coalesce(v_opts::text, 'NULL');

    assert exists (
        select 1
        from pg_catalog.pg_index as ix
        join pg_catalog.pg_class as tbl on tbl.oid = ix.indrelid
        join pg_catalog.pg_namespace as n on n.oid = tbl.relnamespace
        where n.nspname = 'pgque'
          and tbl.relname = 'idem'
          and ix.indisvalid
          and pg_catalog.pg_get_indexdef(ix.indexrelid) like '%(queue_id, expires_at)%'
    ), 'pgque.idem must have a composite (queue_id, expires_at) index';

    perform pgque.create_queue('idem_scale_q');

    select s.event_id, s.deduped
    into v_eid, v_dedup
    from pgque.send_idem('idem_scale_q', 'ev', '{"single":1}', 'single:k1', interval '1 hour') as s;

    assert not v_dedup and v_eid is not null, 'fresh send_idem must insert';

    select k.cmin::text::int into strict v_idem_cmin
    from pgque.idem as k
    join pgque.queue as q on q.queue_id = k.queue_id
    where q.queue_name = 'idem_scale_q' and k.idem_key = 'single:k1';

    select pgque.quote_fqname(queue_data_pfx || '_' || queue_cur_table::text)
    into strict v_table
    from pgque.queue
    where queue_name = 'idem_scale_q';

    execute format('select cmin::text::int from %s where ev_id = $1', v_table)
    into strict v_ev_cmin
    using v_eid;

    /* Single-write INSERT puts the final event_id in pgque.idem BEFORE
       appending the event, so idem.cmin < event.cmin (zero dead-on-birth UPDATE). */
    assert v_idem_cmin < v_ev_cmin,
        format('fresh uncontended send_idem must not UPDATE pgque.idem after insert (idem.cmin=%s, ev.cmin=%s)',
            v_idem_cmin, v_ev_cmin);

    /* Sequence must advance by exactly 1 per fresh send and 0 per live duplicate
       so ticker max_count and get_queue_info ev_per_sec stay accurate. */
    declare
        v_eid2 bigint;
        v_dedup2 boolean;
        v_eid_dup bigint;
        v_dedup_dup boolean;
    begin
        select s.event_id, s.deduped into v_eid_dup, v_dedup_dup
        from pgque.send_idem('idem_scale_q', 'ev', '{"single":1}', 'single:k1', interval '1 hour') as s;
        assert v_dedup_dup and v_eid_dup = v_eid, 'live duplicate must return original event_id';

        select s.event_id, s.deduped into v_eid2, v_dedup2
        from pgque.send_idem('idem_scale_q', 'ev', '{"single":2}', 'single:k2', interval '1 hour') as s;
        assert not v_dedup2 and v_eid2 = v_eid + 1,
            format('fresh send_idem must advance queue_event_seq by exactly 1 (first=%s, second=%s)',
                v_eid, v_eid2);
    end;

    raise notice 'PASS: pgque.idem fillfactor=80, (queue_id, expires_at) index, single-write fresh send, and exact 1-step sequence verified';
end $$;

/* 2. Input validation: empty idem_key, overflowing TTL, and calendar-past TTL -> 22023. */
do $$
declare
    v_state text;
begin
    begin
        perform 1 from pgque.send_idem('idem_scale_q', 'ev', '{}', '', interval '1 hour');
        raise exception 'empty idem_key must be rejected';
    exception when others then
        v_state := sqlstate;
        assert v_state = '22023',
            format('empty idem_key must raise 22023, got %s (%s)', v_state, sqlerrm);
    end;

    begin
        perform 1 from pgque.send_idem('idem_scale_q', 'ev', '{}', 'huge:ttl', interval '300000 years');
        raise exception 'overflowing finite TTL must be rejected';
    exception when others then
        v_state := sqlstate;
        assert v_state = '22023',
            format('overflowing TTL must raise 22023, got %s (%s)', v_state, sqlerrm);
    end;

    /* -1 year + 361 days compares > 0 under PG's 30-day-month interval comparison
       (-360 + 361 = +1 day), but always lands in the past when added to any date. */
    begin
        perform 1 from pgque.send_idem('idem_scale_q', 'ev', '{}', 'past:ttl', interval '-1 year 361 days');
        raise exception 'calendar-past TTL must be rejected';
    exception when others then
        v_state := sqlstate;
        assert v_state = '22023',
            format('calendar-past TTL must raise 22023, got %s (%s)', v_state, sqlerrm);
    end;

    raise notice 'PASS: empty idem_key, overflowing TTL, and calendar-past TTL all raise 22023';
end $$;

/* 3. Looped GC in pgque.maint(): 25,000 expired rows drained in a single maint() call. */
do $$
declare
    v_queue_id int4;
    v_remaining bigint;
    v_live bigint;
begin
    select queue_id into strict v_queue_id
    from pgque.queue
    where queue_name = 'idem_scale_q';

    insert into pgque.idem (queue_id, idem_key, event_id, expires_at)
    select v_queue_id, 'bulk-expired:' || g::text, g, clock_timestamp() - interval '5 minutes'
    from generate_series(1, 25000) as g;

    perform pgque.maint();

    select count(*) into v_remaining
    from pgque.idem
    where queue_id = v_queue_id
      and idem_key like 'bulk-expired:%';

    assert v_remaining = 0,
        format('single pgque.maint() call must drain 25,000 expired idem rows, left %s', v_remaining);

    select count(*) into v_live
    from pgque.idem
    where queue_id = v_queue_id
      and idem_key = 'single:k1';
    assert v_live = 1, 'live claim must remain untouched by looped GC';

    perform pgque.drop_queue('idem_scale_q', true);
    raise notice 'PASS: single pgque.maint() drained 25,000 expired idem rows while preserving live claims';
end $$;
