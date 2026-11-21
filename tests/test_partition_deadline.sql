-- Slot claims and automatic receive renewal use the same deadline invariant.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on
select pgque.create_queue('partition_deadline');
select pgque.subscribe_slot('partition_deadline', 'part', 0, 1);
select pgque.send('partition_deadline', 'clock', 'one');
select pgque.force_next_tick('partition_deadline');
select pgque.ticker('partition_deadline');

do $$
declare
    v_bad interval := interval '-1 year 360 days 1 second';
    v_failed text[] := '{}';
    v_action text;
    v_before pgque.partition_slot%rowtype;
    v_after pgque.partition_slot%rowtype;
    v_qid int;
    v_page pgque.batch_page;
begin
    select queue_id into strict v_qid from pgque.queue where queue_name = 'partition_deadline';
    if not (v_bad >= interval '1 second' and clock_timestamp() + v_bad < clock_timestamp()) then
        raise exception 'fixture must meet claim minimum but produce a past deadline';
    end if;
    foreach v_action in array array['initial claim', 'owner reclaim', 'expired takeover', 'page receive', 'page reconnect'] loop
        if v_action = 'owner reclaim' then
            perform pgque.claim_slot('partition_deadline', 'part', 0, 'worker', interval '1 month');
        elsif v_action = 'expired takeover' then
            update pgque.partition_slot set lease_until = clock_timestamp() - interval '1 second'
            where queue_id = v_qid and co_name = 'part' and slot = 0;
        elsif v_action = 'page receive' then
            perform pgque.claim_slot('partition_deadline', 'part', 0, 'worker', interval '1 month');
            update pgque.partition_slot set lease_ttl = v_bad
            where queue_id = v_qid and co_name = 'part' and slot = 0;
        elsif v_action = 'page reconnect' then
            perform pgque.claim_slot('partition_deadline', 'part', 0, 'worker', interval '1 month');
            v_page := pgque.receive_page_partitioned('partition_deadline', 'part', 0, 1, 'worker', 1);
            if v_page.status is distinct from 'page' or v_page.lease_until <= clock_timestamp() then
                raise exception 'fixture must issue a live partition page';
            end if;
            update pgque.partition_slot set lease_ttl = v_bad
            where queue_id = v_qid and co_name = 'part' and slot = 0;
        end if;
        select * into strict v_before from pgque.partition_slot
        where queue_id = v_qid and co_name = 'part' and slot = 0;
        begin
            if v_action in ('page receive', 'page reconnect') then
                perform pgque.receive_page_partitioned('partition_deadline', 'part', 0, 1, 'worker', 1);
            else
                perform pgque.claim_slot('partition_deadline', 'part', 0,
                    case when v_action = 'expired takeover' then 'successor' else 'worker' end, v_bad);
            end if;
            raise exception 'nonfuture slot deadline accepted' using errcode = 'PT001';
        exception
            when invalid_parameter_value then null;
            when sqlstate 'PT001' then
                v_failed := array_append(v_failed, v_action);
                raise notice 'FAIL: partition % accepted a nonfuture deadline', v_action;
        end;
        select * into strict v_after from pgque.partition_slot
        where queue_id = v_qid and co_name = 'part' and slot = 0;
        if v_after is distinct from v_before then
            raise exception 'rejected % changed slot owner, epoch or deadline', v_action;
        end if;
    end loop;
    if cardinality(v_failed) <> 0 then
        raise exception 'nonfuture partition deadline accepted by: %', array_to_string(v_failed, ', ');
    end if;
end $$;
select pgque.drop_queue('partition_deadline', true);
\echo 'PASS: slot claim/reclaim/takeover and partition receive reject nonfuture deadlines atomically'
