-- Paged retry delays start at scheduling, not at transaction start.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on

create temporary table _paged_retry_clock_events (msg_id bigint, delay_seconds integer);

do $$
begin
    perform pgque.create_queue('paged_retry_clock_test');
    perform pgque.subscribe('paged_retry_clock_test', 'clock-consumer');
end $$;

/* Commit publication before taking the batch's closing snapshot. */
do $$
begin
    insert into _paged_retry_clock_events values
        (pgque.send('paged_retry_clock_test', 'delayed', 'later'), 2),
        (pgque.send('paged_retry_clock_test', 'immediate', 'now'), 0);
end $$;

do $$
begin
    perform pgque.force_next_tick('paged_retry_clock_test');
    perform pgque.ticker('paged_retry_clock_test');
end $$;

begin;
set local statement_timeout = '15s';
select pg_sleep(2.25);

do $$
declare
    v_page pgque.batch_page;
    v_ack record;
    v_failures jsonb;
    v_before timestamptz;
    v_after timestamptz;
    v_retry record;
    v_count integer := 0;
begin
    v_page := pgque.receive_page(
        'paged_retry_clock_test', 'clock-consumer', 'clock-worker', 2, interval '1 minute');
    assert v_page.status = 'page' and v_page.is_last
        and cardinality(v_page.messages) = 2,
        'retry clock fixture must have one terminal two-event page';
    select jsonb_agg(jsonb_build_object(
        'msg_id', msg_id::text, 'retry_after_seconds', delay_seconds))
    into v_failures
    from _paged_retry_clock_events;

    v_before := clock_timestamp();
    select * into v_ack
    from pgque.ack_page(v_page.page_token, 'clock-worker', v_failures);
    v_after := clock_timestamp();
    assert v_ack.status = 'acked' and v_ack.batch_finished,
        'retry scheduling must finish the page';

    for v_retry in
        select rq.ev_retry_after, e.delay_seconds
        from pgque.retry_queue as rq
        join pgque.queue as q on q.queue_id = rq.ev_queue
        join _paged_retry_clock_events as e on e.msg_id = rq.ev_id
        where q.queue_name = 'paged_retry_clock_test'
    loop
        v_count := v_count + 1;
        assert v_retry.ev_retry_after between
            v_before + make_interval(secs => v_retry.delay_seconds)
            and v_after + make_interval(secs => v_retry.delay_seconds),
            format('paged retry delay must start at scheduling: delay=%s retry_at=%s scheduled_between=%s..%s',
                v_retry.delay_seconds, v_retry.ev_retry_after, v_before, v_after);
    end loop;
    assert v_count = 2, 'both failed events must be scheduled exactly once';
end $$;
commit;

select pgque.drop_queue('paged_retry_clock_test', true);
drop table _paged_retry_clock_events;
\echo 'PASS: paged retry delays use scheduling time, including zero-delay retries'
