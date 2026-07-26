-- test_streaming.sql -- pgque experimental streaming SQL prototype
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Red/green TDD: this test file was written BEFORE the implementation.
-- Tests the streaming SQL prototype:
--   pgque.stream()                -- base reader over queue retention
--   pgque.tumble()                -- tumbling windows on ev_time
--   pgque.hop()                   -- hopping/sliding windows on ev_time
--   pgque.session()               -- session windows by ev_extra partition
--   pgque.stream_join()           -- temporal join across two queues
--   pgque.create_continuous_query()/run_continuous_query()/drop_continuous_query()

\set ON_ERROR_STOP on

-- Setup: two queues with synthetic events. We backdate ev_time by directly
-- updating the queue data table because insert_event() uses now() internally.
do $$ begin
    perform pgque.create_queue('stream_a');
    perform pgque.create_queue('stream_b');
    perform pgque.register_consumer('stream_a', 'c1');
    perform pgque.register_consumer('stream_b', 'c1');

    -- 6 events on stream_a, partition keys u1/u2 in ev_extra1
    perform pgque.insert_event('stream_a', 'click', '{"n":1}', 'u1', null, null, null);
    perform pgque.insert_event('stream_a', 'click', '{"n":2}', 'u1', null, null, null);
    perform pgque.insert_event('stream_a', 'click', '{"n":3}', 'u2', null, null, null);
    perform pgque.insert_event('stream_a', 'click', '{"n":4}', 'u1', null, null, null);
    perform pgque.insert_event('stream_a', 'click', '{"n":5}', 'u2', null, null, null);
    perform pgque.insert_event('stream_a', 'click', '{"n":6}', 'u2', null, null, null);

    -- 3 events on stream_b, same ev_extra1 partition keys for join
    perform pgque.insert_event('stream_b', 'purchase', '{"amt":10}', 'u1', null, null, null);
    perform pgque.insert_event('stream_b', 'purchase', '{"amt":20}', 'u2', null, null, null);
    perform pgque.insert_event('stream_b', 'purchase', '{"amt":30}', 'u1', null, null, null);
end $$;

-- Backdate ev_time so we can exercise time windows in a deterministic way.
-- stream_a events get spaced 10s apart starting 60s ago.
-- stream_b events get spaced 20s apart starting 50s ago (close enough to join).
do $$
declare
    v_qid_a int;
    v_qid_b int;
    v_pfx_a text;
    v_pfx_b text;
begin
    select queue_id, queue_data_pfx into v_qid_a, v_pfx_a
        from pgque.queue where queue_name = 'stream_a';
    select queue_id, queue_data_pfx into v_qid_b, v_pfx_b
        from pgque.queue where queue_name = 'stream_b';

    execute format(
        'update %s set ev_time = now() - (60 - 10 * (ev_id %% 100))::int * interval ''1 second''',
        v_pfx_a);
    execute format(
        'update %s set ev_time = now() - (50 - 20 * (ev_id %% 100))::int * interval ''1 second''',
        v_pfx_b);
end $$;

-- ---------------------------------------------------------------------------
-- Test 1: pgque.stream() returns events from a queue's retention window.
-- ---------------------------------------------------------------------------
do $$
declare
    v_count bigint;
begin
    select count(*) into v_count
        from pgque.stream('stream_a', '5 minutes'::interval);
    assert v_count = 6, 'stream(stream_a) should return 6 events, got ' || v_count;

    select count(*) into v_count
        from pgque.stream('stream_b', '5 minutes'::interval);
    assert v_count = 3, 'stream(stream_b) should return 3 events, got ' || v_count;

    raise notice 'PASS: stream() returns retained events';
end $$;

-- Errors on missing queue.
do $$
begin
    begin
        perform * from pgque.stream('does_not_exist', '5 minutes'::interval);
        assert false, 'stream() should raise on missing queue';
    exception when others then
        assert sqlerrm like '%queue not found%',
            'unexpected error: ' || sqlerrm;
    end;
    raise notice 'PASS: stream() raises on missing queue';
end $$;

-- ---------------------------------------------------------------------------
-- Test 2: pgque.tumble() partitions events into non-overlapping windows.
-- 6 events spaced 10s over 60s with a 30s bucket -> exactly 6 (event,bucket)
-- pairs since tumbling does not duplicate.
-- ---------------------------------------------------------------------------
do $$
declare
    v_total bigint;
    v_distinct_buckets bigint;
begin
    select count(*), count(distinct bucket_start)
        into v_total, v_distinct_buckets
        from pgque.tumble('stream_a', '30 seconds'::interval, '5 minutes'::interval);

    assert v_total = 6,
        'tumble() should not duplicate events; expected 6 rows, got ' || v_total;
    assert v_distinct_buckets between 2 and 4,
        'tumble() should produce 2..4 buckets covering 60s of events at 30s, got '
        || v_distinct_buckets;

    raise notice 'PASS: tumble() rows=%, buckets=%', v_total, v_distinct_buckets;
end $$;

-- Aggregation in a tumbling window: count per bucket per ev_extra1.
do $$
declare
    v_max_per_bucket bigint;
begin
    select max(c) into v_max_per_bucket
        from (
            select bucket_start, ev_extra1, count(*) c
            from pgque.tumble('stream_a', '60 seconds'::interval,
                              '5 minutes'::interval)
            group by 1, 2
        ) sub;
    assert v_max_per_bucket >= 1,
        'tumble() agg should produce at least one nonzero count';
    raise notice 'PASS: tumble() aggregation works (max=%)', v_max_per_bucket;
end $$;

-- ---------------------------------------------------------------------------
-- Test 3: pgque.hop() duplicates events into overlapping windows.
-- window=30s, slide=10s -> each event lives in up to 3 windows.
-- ---------------------------------------------------------------------------
do $$
declare
    v_total bigint;
    v_distinct_events bigint;
begin
    select count(*), count(distinct ev_id)
        into v_total, v_distinct_events
        from pgque.hop('stream_a', '30 seconds'::interval,
                       '10 seconds'::interval, '5 minutes'::interval);

    assert v_distinct_events = 6,
        'hop() should still cover 6 distinct events, got ' || v_distinct_events;
    assert v_total > v_distinct_events,
        'hop() with overlapping windows should duplicate events; '
        || 'total=' || v_total || ' distinct=' || v_distinct_events;

    raise notice 'PASS: hop() rows=%, distinct events=%', v_total, v_distinct_events;
end $$;

-- ---------------------------------------------------------------------------
-- Test 4: pgque.session() groups events by ev_extra1 with a gap rule.
-- All u1 events fall within a single 30s gap -> 1 session.
-- ---------------------------------------------------------------------------
do $$
declare
    v_u1_sessions bigint;
    v_u2_sessions bigint;
begin
    select count(distinct session_id) into v_u1_sessions
        from pgque.session('stream_a', '30 seconds'::interval, 1,
                           '5 minutes'::interval)
        where partition_key = 'u1';
    select count(distinct session_id) into v_u2_sessions
        from pgque.session('stream_a', '30 seconds'::interval, 1,
                           '5 minutes'::interval)
        where partition_key = 'u2';

    assert v_u1_sessions >= 1, 'session() should find at least 1 u1 session';
    assert v_u2_sessions >= 1, 'session() should find at least 1 u2 session';

    raise notice 'PASS: session() u1=%, u2=%', v_u1_sessions, v_u2_sessions;
end $$;

-- ---------------------------------------------------------------------------
-- Test 5: pgque.stream_join() temporal join across two queues on ev_extra1.
-- max_skew=120s should match all u1/u2 pairs across stream_a and stream_b.
-- ---------------------------------------------------------------------------
do $$
declare
    v_count bigint;
    v_u1_count bigint;
begin
    select count(*) into v_count
        from pgque.stream_join('stream_a', 'stream_b',
                               '120 seconds'::interval, 1,
                               '5 minutes'::interval);
    assert v_count > 0,
        'stream_join() with wide skew should produce matches, got ' || v_count;

    select count(*) into v_u1_count
        from pgque.stream_join('stream_a', 'stream_b',
                               '120 seconds'::interval, 1,
                               '5 minutes'::interval)
        where join_key = 'u1';
    -- u1 has 3 stream_a events x 2 stream_b events = 6
    assert v_u1_count = 6,
        'stream_join() u1 cartesian within skew should be 6, got ' || v_u1_count;

    raise notice 'PASS: stream_join() rows=%, u1=%', v_count, v_u1_count;
end $$;

-- Narrow skew prunes matches.
do $$
declare
    v_wide bigint;
    v_narrow bigint;
begin
    select count(*) into v_wide
        from pgque.stream_join('stream_a', 'stream_b',
                               '120 seconds'::interval, 1,
                               '5 minutes'::interval);
    select count(*) into v_narrow
        from pgque.stream_join('stream_a', 'stream_b',
                               '5 seconds'::interval, 1,
                               '5 minutes'::interval);
    assert v_narrow < v_wide,
        'narrower skew should yield fewer matches; wide=' || v_wide
        || ' narrow=' || v_narrow;
    raise notice 'PASS: stream_join() skew pruning works (% -> %)', v_wide, v_narrow;
end $$;

-- ---------------------------------------------------------------------------
-- Test 6: continuous query lifecycle. Register, run, see rows in target.
-- ---------------------------------------------------------------------------
do $$
declare
    v_rows bigint;
    v_runs bigint;
begin
    create table if not exists public.cq_clicks_per_minute (
        bucket_start timestamptz,
        ev_extra1    text,
        n            bigint
    );
    truncate public.cq_clicks_per_minute;

    perform pgque.create_continuous_query(
        i_name => 'clicks_per_minute',
        i_sql => $sql$
            select bucket_start, ev_extra1, count(*)::bigint
            from pgque.tumble('stream_a', '60 seconds'::interval,
                              '5 minutes'::interval)
            group by 1, 2
        $sql$,
        i_target_table => 'public.cq_clicks_per_minute',
        i_every => '1 minute'::interval
    );

    -- Idempotent registration: second call should update, not error.
    perform pgque.create_continuous_query(
        i_name => 'clicks_per_minute',
        i_sql => $sql$
            select bucket_start, ev_extra1, count(*)::bigint
            from pgque.tumble('stream_a', '60 seconds'::interval,
                              '5 minutes'::interval)
            group by 1, 2
        $sql$,
        i_target_table => 'public.cq_clicks_per_minute',
        i_every => '1 minute'::interval
    );

    perform pgque.run_continuous_query('clicks_per_minute');

    select count(*) into v_rows from public.cq_clicks_per_minute;
    assert v_rows > 0,
        'continuous query should have written rows to sink, got ' || v_rows;

    select cq_runs into v_runs from pgque.continuous_query
        where cq_name = 'clicks_per_minute';
    assert v_runs = 1, 'cq_runs should be 1 after one execution, got ' || v_runs;

    raise notice 'PASS: continuous_query rows=%, runs=%', v_rows, v_runs;
end $$;

-- list_continuous_queries() is the public read of the registry.
do $$
declare
    v_count bigint;
begin
    select count(*) into v_count from pgque.list_continuous_queries()
        where name = 'clicks_per_minute';
    assert v_count = 1, 'list_continuous_queries() should show clicks_per_minute';
    raise notice 'PASS: list_continuous_queries() works';
end $$;

-- drop_continuous_query() removes it.
do $$
declare
    v_count bigint;
begin
    perform pgque.drop_continuous_query('clicks_per_minute');
    select count(*) into v_count from pgque.continuous_query
        where cq_name = 'clicks_per_minute';
    assert v_count = 0, 'drop_continuous_query() should remove the row';
    raise notice 'PASS: drop_continuous_query()';
end $$;

-- ---------------------------------------------------------------------------
-- Cleanup
-- ---------------------------------------------------------------------------
do $$ begin
    drop table if exists public.cq_clicks_per_minute;
    perform pgque.drop_queue('stream_a', true);
    perform pgque.drop_queue('stream_b', true);
end $$;

\echo 'streaming tests passed'
