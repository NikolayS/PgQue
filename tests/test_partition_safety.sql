-- test_partition_safety.sql -- Partition fencing, cross-slot nack, slot guards & edge keys
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

/* Setup shared queue for partition safety regressions. */
do $$
begin
    perform pgque.create_queue('part_safety_q');
    perform pgque.subscribe_partitioned('part_safety_q', 'pc', 4);
end $$;

/* 1. Cross-slot nack_partitioned must reject another slot's msg_id (no orphaned retry). */
do $$
declare
    v_key0 text;
    v_key1 text;
    v_ev0 bigint;
    v_ev1 bigint;
    v_msg pgque.message;
    v_batch0 bigint;
    v_state text;
    v_raised boolean := false;
begin
    select format('k-%s', g) into v_key0
    from generate_series(1, 500) as g
    where (pg_catalog.hashtextextended(format('k-%s', g), 0) % 4 + 4) % 4 = 0
    limit 1;

    select format('k-%s', g) into v_key1
    from generate_series(1, 500) as g
    where (pg_catalog.hashtextextended(format('k-%s', g), 0) % 4 + 4) % 4 = 1
    limit 1;

    v_ev0 := pgque.send('part_safety_q', 'evt', '{"slot":0}'::jsonb, v_key0);
    v_ev1 := pgque.send('part_safety_q', 'evt', '{"slot":1}'::jsonb, v_key1);
end $$;

do $$
begin
    perform pgque.force_next_tick('part_safety_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_ev1 bigint;
    v_key1 text;
    v_batch0 bigint;
    v_batch1 bigint;
    v_msg pgque.message;
    v_state text;
    v_raised boolean := false;
begin
    perform pgque.claim_slot('part_safety_q', 'pc', 1, 'w1', '1 minute');
    select * into v_msg
    from pgque.receive_partitioned('part_safety_q', 'pc', 1, 4, 'w1', 100)
    limit 1;
    v_ev1 := v_msg.msg_id;
    v_key1 := v_msg.extra1;
    v_batch1 := v_msg.batch_id;
    assert v_ev1 is not null, 'slot 1 must receive its keyed event';
    perform pgque.ack_partitioned('part_safety_q', 'pc', 1, 4, 'w1', v_batch1);
    perform pgque.release_slot('part_safety_q', 'pc', 1, 'w1');

    perform pgque.claim_slot('part_safety_q', 'pc', 0, 'w0', '1 minute');
    select batch_id into v_batch0
    from pgque.receive_partitioned('part_safety_q', 'pc', 0, 4, 'w0', 100)
    limit 1;
    assert v_batch0 is not null, 'slot 0 must receive its keyed event';

    begin
        perform pgque.nack_partitioned(
            'part_safety_q', 'pc', 0, 4, 'w0',
            row(v_ev1, v_batch0, 'evt', '{"slot":1}', null, now(), v_key1, null, null, null)::pgque.message,
            '0 seconds', 'cross-slot-attempt'
        );
    exception when others then
        get stacked diagnostics v_state = returned_sqlstate;
        v_raised := true;
    end;
    assert v_raised and v_state = '22023',
        'nack_partitioned must reject cross-slot msg_id with 22023, got raised=' || v_raised || ' state=' || coalesce(v_state, 'null');

    assert pgque.ack_partitioned('part_safety_q', 'pc', 0, 4, 'w0', v_batch0) = 1,
        'slot 0 active batch must still ack cleanly';
    assert pgque.ack_partitioned('part_safety_q', 'pc', 0, 4, 'w0', v_batch0) = 0,
        'repeated ack_partitioned with no active batch must return 0';
    perform pgque.release_slot('part_safety_q', 'pc', 0, 'w0');
    raise notice 'PASS: cross-slot nack_partitioned rejected';
end $$;

/* 2. ABA fencing: stale worker re-claiming slot cannot blindly ack a newer batch. */
do $$
begin
    perform pgque.send('part_safety_q', 'aba', '{"b":1}'::jsonb, null);
end $$;

do $$
begin
    perform pgque.force_next_tick('part_safety_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_b1 bigint;
    v_b2 bigint;
    v_b1_heir bigint;
    v_state text;
    v_raised boolean := false;
begin
    /* wk-a claims epoch 1 and opens B1, then stalls past TTL. */
    perform pgque.claim_slot('part_safety_q', 'pc', 0, 'wk-a', '1 minute');
    select batch_id into v_b1
    from pgque.receive_partitioned('part_safety_q', 'pc', 0, 4, 'wk-a', 10)
    limit 1;
    assert v_b1 is not null, 'wk-a must open B1';

    /* Force expiry deterministically without sleeping. */
    update pgque.partition_slot as ps
    set lease_until = clock_timestamp() - interval '1 second'
    from pgque.queue as q
    where q.queue_name = 'part_safety_q'
      and ps.queue_id = q.queue_id
      and ps.co_name = 'pc'
      and ps.slot = 0;

    /* wk-b takes over, processes and acks B1, and releases slot 0. */
    perform pgque.claim_slot('part_safety_q', 'pc', 0, 'wk-b', '1 minute');
    select batch_id into v_b1_heir
    from pgque.receive_partitioned('part_safety_q', 'pc', 0, 4, 'wk-b', 10)
    limit 1;
    assert v_b1_heir = v_b1, 'heir must redeliver B1';
    assert pgque.ack_partitioned('part_safety_q', 'pc', 0, 4, 'wk-b', v_b1_heir) = 1;
    perform pgque.release_slot('part_safety_q', 'pc', 0, 'wk-b');
end $$;

do $$
begin
    perform pgque.send('part_safety_q', 'aba', '{"b":2}'::jsonb, null);
end $$;

do $$
begin
    perform pgque.force_next_tick('part_safety_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_b2 bigint;
    v_stale_b1 bigint;
    v_state text;
    v_raised boolean := false;
    v_still_open int;
begin
    /* wk-a reconnects (epoch 3) and opens B2. */
    perform pgque.claim_slot('part_safety_q', 'pc', 0, 'wk-a', '1 minute');
    select batch_id into v_b2
    from pgque.receive_partitioned('part_safety_q', 'pc', 0, 4, 'wk-a', 10)
    limit 1;
    assert v_b2 is not null, 'wk-a must open B2';
    v_stale_b1 := v_b2 - 1;

    /* Stale handler tries to ack old batch_id -> must fence with 55000, NOT drop B2! */
    begin
        perform pgque.ack_partitioned('part_safety_q', 'pc', 0, 4, 'wk-a', v_stale_b1);
    exception when others then
        get stacked diagnostics v_state = returned_sqlstate;
        v_raised := true;
    end;
    assert v_raised and v_state = '55000',
        'stale batch_id ack_partitioned must fence with 55000, got raised=' || v_raised || ' state=' || coalesce(v_state, 'null');

    /* NULL batch_id must fail with 22023. */
    v_raised := false;
    v_state := null;
    begin
        perform pgque.ack_partitioned('part_safety_q', 'pc', 0, 4, 'wk-a', null::bigint);
    exception when others then
        get stacked diagnostics v_state = returned_sqlstate;
        v_raised := true;
    end;
    assert v_raised and v_state = '22023',
        'null batch_id ack_partitioned must raise 22023, got ' || coalesce(v_state, 'null');

    /* B2 must still be intact and redeliverable. */
    select count(*) into v_still_open
    from pgque.receive_partitioned('part_safety_q', 'pc', 0, 4, 'wk-a', 10);
    assert v_still_open = 1, 'B2 must remain open after fenced stale ack';
    assert pgque.ack_partitioned('part_safety_q', 'pc', 0, 4, 'wk-a', v_b2) = 1;
    perform pgque.release_slot('part_safety_q', 'pc', 0, 'wk-a');
    raise notice 'PASS: ABA ack_partitioned fenced by batch_id';
end $$;

/* 3. Unguarded unsubscribe, subscribe_subconsumer, and register_consumer_at must reject cataloged slots. */
do $$
declare
    v_tick bigint;
    v_raised boolean;
begin
    v_raised := false;
    begin
        perform pgque.unsubscribe('part_safety_q', 'pc#0/4');
    exception when others then
        v_raised := true;
    end;
    assert v_raised, 'pgque.unsubscribe() must reject cataloged partition slot pc#0/4';

    v_raised := false;
    begin
        perform pgque.subscribe_subconsumer('part_safety_q', 'pc#0/4', 'm1', true);
    exception when others then
        v_raised := true;
    end;
    assert v_raised, 'pgque.subscribe_subconsumer() must reject cataloged partition slot pc#0/4';

    select max(t.tick_id) into v_tick
    from pgque.tick as t
    join pgque.queue as q on q.queue_id = t.tick_queue
    where q.queue_name = 'part_safety_q';

    v_raised := false;
    begin
        perform pgque.register_consumer_at('part_safety_q', 'pc#0/4', v_tick);
    exception when others then
        v_raised := true;
    end;
    assert v_raised, 'pgque.register_consumer_at() must reject repositioning cataloged slot pc#0/4';

    raise notice 'PASS: cataloged partition slots guarded against unsubscribe/coop/reposition';
end $$;

/* 4. NULL, empty-string, negative-hash, and multi-byte UTF-8 keys route deterministically. */
do $$
begin
    perform pgque.create_queue('part_safety_edge_q');
    perform pgque.subscribe_partitioned('part_safety_edge_q', 'pc', 4);
end $$;

do $$
declare
    v_neg_key text;
begin
    select format('neg-%s', g) into v_neg_key
    from generate_series(1, 500) as g
    where pg_catalog.hashtextextended(format('neg-%s', g), 0) < 0
    limit 1;
    assert v_neg_key is not null, 'must find a key with negative hashtextextended';

    perform pgque.send('part_safety_edge_q', 'edge', '{"k":"null"}'::jsonb, null);
    perform pgque.send('part_safety_edge_q', 'edge', '{"k":"empty"}'::jsonb, '');
    perform pgque.send('part_safety_edge_q', 'edge', '{"k":"neg"}'::jsonb, v_neg_key);
    perform pgque.send('part_safety_edge_q', 'edge', '{"k":"utf8"}'::jsonb, 'tenant-日本語-🚀');
end $$;

do $$
begin
    perform pgque.force_next_tick('part_safety_edge_q');
    perform pgque.ticker();
end $$;

do $$
declare
    v_s int;
    v_b bigint;
    v_total int := 0;
    v_null_on_zero int := 0;
    v_cnt int;
    v_null_cnt int;
begin
    for v_s in 0..3 loop
        perform pgque.claim_slot('part_safety_edge_q', 'pc', v_s, 'w-edge', '1 minute');
        select count(*), count(*) filter (where extra1 is null), max(batch_id)
        into v_cnt, v_null_cnt, v_b
        from pgque.receive_partitioned('part_safety_edge_q', 'pc', v_s, 4, 'w-edge', 100);

        if v_b is not null then
            perform pgque.ack_partitioned('part_safety_edge_q', 'pc', v_s, 4, 'w-edge', v_b);
        end if;
        perform pgque.release_slot('part_safety_edge_q', 'pc', v_s, 'w-edge');

        v_total := v_total + v_cnt;
        if v_s = 0 then
            v_null_on_zero := v_null_cnt;
        else
            assert v_null_cnt = 0, 'NULL partition_key must route only to slot 0';
        end if;
    end loop;

    assert v_total = 4 and v_null_on_zero = 1,
        format('expected 4 edge events total with 1 NULL on slot 0, got total=%s null_on_zero=%s', v_total, v_null_on_zero);
    perform pgque.unsubscribe_partitioned('part_safety_edge_q', 'pc');
    perform pgque.drop_queue('part_safety_edge_q', true);
    raise notice 'PASS: NULL, empty, negative-hash, and UTF-8 partition keys routed cleanly';
end $$;

/* 5. partition_slot fillfactor=80 and idle-poll renewal throttle. */
do $$
declare
    v_opts text[];
    v_ctid0 tid;
    v_ctid1 tid;
    v_until0 timestamptz;
    v_until1 timestamptz;
    v_i int;
begin
    select reloptions into v_opts
    from pg_catalog.pg_class
    where oid = 'pgque.partition_slot'::regclass;
    assert v_opts is not null and 'fillfactor=80' = any(v_opts),
        'pgque.partition_slot must have fillfactor=80, got ' || coalesce(v_opts::text, 'null');

    perform pgque.claim_slot('part_safety_q', 'pc', 0, 'w-idle', '30 seconds');
    select ps.ctid, ps.lease_until into v_ctid0, v_until0
    from pgque.partition_slot as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'part_safety_q' and ps.co_name = 'pc' and ps.slot = 0;

    for v_i in 1..20 loop
        perform * from pgque.receive_partitioned('part_safety_q', 'pc', 0, 4, 'w-idle', 100);
    end loop;

    select ps.ctid, ps.lease_until into v_ctid1, v_until1
    from pgque.partition_slot as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'part_safety_q' and ps.co_name = 'pc' and ps.slot = 0;

    assert v_ctid1 = v_ctid0 and v_until1 = v_until0,
        format('20 rapid idle receive_partitioned polls must not rewrite partition_slot tuple (ctid %s->%s)', v_ctid0, v_ctid1);

    perform pgque.release_slot('part_safety_q', 'pc', 0, 'w-idle');
    raise notice 'PASS: partition_slot fillfactor=80 and idle-poll throttle verified (ctid unchanged)';
end $$;

/* Cleanup */
do $$
begin
    perform pgque.unsubscribe_partitioned('part_safety_q', 'pc');
    perform pgque.drop_queue('part_safety_q', true);
end $$;
