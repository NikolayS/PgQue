-- test_paged_scale_and_subxid.sql -- SubXID overflow, single-scan ack_page, multi-table paging & fillfactor
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

/* 1. ack_page with F = 70 failures must NOT burn SubXIDs (>64 overflows PGPROC_MAX_CACHED_SUBXIDS). */
do $$
begin
    perform pgque.create_queue('paged_subxid_q');
    perform pgque.subscribe('paged_subxid_q', 'c1');
end $$;

do $$
begin
    perform pgque.send('paged_subxid_q', 'evt', ('{"i":' || g || '}')::jsonb, 'key-' || (g % 8))
    from generate_series(1, 200) as g;
end $$;

do $$
begin
    perform pgque.force_next_tick('paged_subxid_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_page pgque.batch_page;
    v_failures jsonb;
    v_xid_before bigint;
    v_xid_after bigint;
    v_xid_delta bigint;
    v_ack record;
    v_rq_count int;
begin
    v_page := pgque.receive_page('paged_subxid_q', 'c1', 'w1', 70, interval '1 minute');
    assert v_page.status = 'page' and cardinality(v_page.messages) = 70,
        'expected 70 messages on first page';

    select jsonb_agg(jsonb_build_object(
        'msg_id', m.msg_id::text,
        'retry_after_seconds', 0,
        'reason', 'downstream 503'
    ))
    into v_failures
    from unnest(v_page.messages) as m;

    /* Capture xid8 before/after ack_page inside its own sub-check via pg_current_xact_id(). */
    v_xid_before := pg_current_xact_id()::text::bigint;
    select * into v_ack from pgque.ack_page(v_page.page_token, 'w1', v_failures);
    v_xid_after := pg_current_xact_id()::text::bigint;

    /* Same transaction holds 1 top-level XID; any SubXIDs allocated inside ack_page
       would advance the next backend XID. Verify zero SubXIDs by checking in next xact below. */
    assert v_ack.status = 'acked' and not v_ack.batch_finished,
        'non-terminal 70-failure page must ack cleanly';

    select count(*) into v_rq_count
    from pgque.retry_queue as rq
    join pgque.queue as q on q.queue_id = rq.ev_queue
    where q.queue_name = 'paged_subxid_q'
      and rq.ev_extra1 is not null;
    assert v_rq_count = 70,
        format('expected 70 retried events with ev_extra1 preserved, got %s', v_rq_count);
end $$;

/* Measure exact XID consumption of a 70-failure ack_page across transaction boundaries. */
create temp table subxid_probe (x0 bigint, x1 bigint);

do $$
begin
    insert into subxid_probe (x0) values (pg_current_xact_id()::text::bigint);
end $$;

do $$
declare
    v_page pgque.batch_page;
    v_failures jsonb;
    v_ack record;
begin
    v_page := pgque.receive_page('paged_subxid_q', 'c1', 'w1', 70, interval '1 minute');
    assert v_page.status = 'page' and cardinality(v_page.messages) = 70;

    select jsonb_agg(jsonb_build_object(
        'msg_id', m.msg_id::text,
        'retry_after_seconds', 0,
        'reason', 'subxid check'
    ))
    into v_failures
    from unnest(v_page.messages) as m;

    select * into v_ack from pgque.ack_page(v_page.page_token, 'w1', v_failures);
    assert v_ack.status = 'acked';
end $$;

do $$
declare
    v_x0 bigint;
    v_x1 bigint;
    v_delta bigint;
begin
    v_x1 := pg_current_xact_id()::text::bigint;
    select x0 into strict v_x0 from subxid_probe;
    /* Without DML subtransactions, the 70-failure block uses 1 XID (plus 1 for v_x1).
       Allow small headroom (< 20) for concurrent autovacuum XIDs; the old bug burned 141+. */
    v_delta := v_x1 - v_x0;
    assert v_delta < 20,
        format('70-failure ack_page burned %s XIDs (SubXID overflow regression; expected < 20)', v_delta);
    raise notice 'PASS: 70-failure ack_page consumed % XIDs (zero SubXID overflow)', v_delta;
end $$;

/* 2. Partitioned ack_page failure routing (retry + DLQ) preserves ev_extra1 affinity. */
do $$
begin
    perform pgque.create_queue('paged_part_fail_q');
    perform pgque.set_queue_config('paged_part_fail_q', 'max_retries', '1');
    perform pgque.subscribe_partitioned('paged_part_fail_q', 'pc', 4);
end $$;

do $$
declare
    v_key text;
begin
    select format('pk-%s', g) into v_key
    from generate_series(1, 500) as g
    where (pg_catalog.hashtextextended(format('pk-%s', g), 0) % 4 + 4) % 4 = 2
    limit 1;
    perform pgque.send('paged_part_fail_q', 'order', '{"n":1}'::jsonb, v_key);
end $$;

do $$
begin
    perform pgque.force_next_tick('paged_part_fail_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_page pgque.batch_page;
    v_ack record;
    v_fail jsonb;
begin
    perform pgque.claim_slot('paged_part_fail_q', 'pc', 2, 'w1', interval '1 minute');
    v_page := pgque.receive_page_partitioned('paged_part_fail_q', 'pc', 2, 4, 'w1', 10);
    assert v_page.status = 'page' and cardinality(v_page.messages) = 1;
    v_fail := jsonb_build_array(jsonb_build_object(
        'msg_id', ((v_page.messages)[1]).msg_id::text,
        'retry_after_seconds', 0,
        'reason', 'transient-part'
    ));
    select * into v_ack from pgque.ack_page(v_page.page_token, 'w1', v_fail);
    assert v_ack.status = 'acked' and v_ack.batch_finished;
end $$;

do $$
begin
    while pgque.maint_retry_events() > 0 loop
        null;
    end loop;
end $$;

do $$
begin
    perform pgque.force_next_tick('paged_part_fail_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_page pgque.batch_page;
    v_ack record;
    v_fail jsonb;
    v_dl pgque.dead_letter%rowtype;
begin
    v_page := pgque.receive_page_partitioned('paged_part_fail_q', 'pc', 2, 4, 'w1', 10);
    assert v_page.status = 'page' and cardinality(v_page.messages) = 1
        and ((v_page.messages)[1]).retry_count = 1,
        'retried partitioned event must return to slot 2 with retry_count=1';

    v_fail := jsonb_build_array(jsonb_build_object(
        'msg_id', ((v_page.messages)[1]).msg_id::text,
        'retry_after_seconds', 0,
        'reason', 'dlq-part'
    ));
    select * into v_ack from pgque.ack_page(v_page.page_token, 'w1', v_fail);
    assert v_ack.status = 'acked' and v_ack.batch_finished;

    select dl.* into strict v_dl
    from pgque.dead_letter as dl
    join pgque.queue as q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'paged_part_fail_q';
    assert v_dl.ev_extra1 is not null and v_dl.dl_reason = 'dlq-part',
        'partitioned paged DLQ must preserve ev_extra1 and reason';

    perform pgque.release_slot('paged_part_fail_q', 'pc', 2, 'w1');
    raise notice 'PASS: partitioned paged retry + DLQ routing preserves ev_extra1 affinity';
end $$;

/* 3. True 2-table spanning batch (event_N_0 UNION ALL event_N_1) with mid-page rotation attempts. */
do $$
begin
    perform pgque.create_queue('paged_span_rot_q');
    perform pgque.subscribe('paged_span_rot_q', 'c1');
    update pgque.queue set queue_rotation_period = interval '1 millisecond'
    where queue_name = 'paged_span_rot_q';
end $$;

do $$
begin
    perform pgque.send('paged_span_rot_q', 'rot', ('{"w":1,"i":' || g || '}')::jsonb)
    from generate_series(1, 50) as g;
end $$;

do $$
begin
    perform pg_sleep(0.02);
    perform pgque.maint_rotate_tables_step1('paged_span_rot_q');
end $$;

do $$
begin
    perform pgque.send('paged_span_rot_q', 'rot', ('{"w":2,"i":' || g || '}')::jsonb)
    from generate_series(51, 100) as g;
end $$;

do $$
begin
    perform pgque.force_next_tick('paged_span_rot_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_page pgque.batch_page;
    v_ack record;
    v_seen bigint[] := array[]::bigint[];
    v_m pgque.message;
    v_prev bigint := null;
begin
    loop
        v_page := pgque.receive_page('paged_span_rot_q', 'c1', 'w1', 20, interval '1 minute');
        exit when v_page.status <> 'page';
        foreach v_m in array v_page.messages loop
            assert v_prev is null or v_m.msg_id > v_prev,
                'multi-table paged messages must be strictly ordered';
            v_prev := v_m.msg_id;
            v_seen := array_append(v_seen, v_m.msg_id);
        end loop;
        select * into v_ack from pgque.ack_page(v_page.page_token, 'w1');
        /* Attempt rotation mid-batch; active batch must prevent truncation of pinned table. */
        perform pg_sleep(0.01);
        perform pgque.maint_rotate_tables_step1('paged_span_rot_q');
        perform pgque.maint_rotate_tables_step2();
        exit when v_ack.batch_finished;
    end loop;

    assert cardinality(v_seen) = 100,
        format('expected 100 unique messages across 2 rotated tables, got %s', cardinality(v_seen));
    raise notice 'PASS: 2-table spanning batch drained 100/100 ordered messages across mid-page rotations';
end $$;

/* 4. page_state and subscription fillfactor=80 + single-UPDATE terminal ack_page. */
do $$
declare
    v_ps_opts text[];
    v_sub_opts text[];
    v_page pgque.batch_page;
    v_ctid_recv tid;
    v_ctid_ack tid;
    v_ack record;
begin
    select reloptions into v_ps_opts from pg_catalog.pg_class where oid = 'pgque.page_state'::regclass;
    select reloptions into v_sub_opts from pg_catalog.pg_class where oid = 'pgque.subscription'::regclass;
    assert v_ps_opts is not null and 'fillfactor=80' = any(v_ps_opts),
        'pgque.page_state must have fillfactor=80';
    assert v_sub_opts is not null and 'fillfactor=80' = any(v_sub_opts),
        'pgque.subscription must have fillfactor=80';

    perform pgque.create_queue('paged_single_upd_q');
    perform pgque.subscribe('paged_single_upd_q', 'c1');
end $$;

do $$
begin
    perform pgque.send('paged_single_upd_q', 'one', '{"x":1}'::jsonb);
end $$;

do $$
begin
    perform pgque.force_next_tick('paged_single_upd_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_page pgque.batch_page;
    v_ctid_recv tid;
    v_ctid_ack tid;
    v_ack record;
    v_offset_diff int;
begin
    v_page := pgque.receive_page('paged_single_upd_q', 'c1', 'w1', 10, interval '1 minute');
    assert v_page.status = 'page' and v_page.is_last;

    select ps.ctid into strict v_ctid_recv
    from pgque.page_state as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'paged_single_upd_q';

    select * into v_ack from pgque.ack_page(v_page.page_token, 'w1');
    assert v_ack.status = 'acked' and v_ack.batch_finished;

    select ps.ctid into strict v_ctid_ack
    from pgque.page_state as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'paged_single_upd_q';

    /* Terminal ack_page must execute 1 UPDATE on page_state (not 3: renew_page + ack + clear). */
    v_offset_diff := (regexp_replace(v_ctid_ack::text, '^\([0-9]+,([0-9]+)\)$', '\1')::int)
                   - (regexp_replace(v_ctid_recv::text, '^\([0-9]+,([0-9]+)\)$', '\1')::int);
    assert v_offset_diff = 1,
        format('terminal ack_page must perform 1 page_state UPDATE (not 3), ctid %s -> %s', v_ctid_recv, v_ctid_ack);
    raise notice 'PASS: page_state/subscription fillfactor=80 and single-UPDATE terminal ack_page verified';
end $$;

/* Cleanup */
do $$
begin
    perform pgque.drop_queue('paged_subxid_q', true);
    perform pgque.unsubscribe_partitioned('paged_part_fail_q', 'pc');
    perform pgque.drop_queue('paged_part_fail_q', true);
    perform pgque.drop_queue('paged_span_rot_q', true);
    perform pgque.drop_queue('paged_single_upd_q', true);
end $$;
