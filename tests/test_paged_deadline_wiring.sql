-- Real page issuance and renewal must reject positive but nonfuture TTLs.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on

select pgque.create_queue('page_deadline_wiring');
select pgque.subscribe('page_deadline_wiring', 'normal');
select pgque.register_subconsumer('page_deadline_wiring', 'coop', 'member');
select pgque.subscribe_slot('page_deadline_wiring', 'part', 0, 1);
-- Commit publication before the closing snapshot.
select pgque.send('page_deadline_wiring', 'clock', 'one');
select pgque.force_next_tick('page_deadline_wiring');
select pgque.ticker('page_deadline_wiring');

do $$
declare
    -- Nominal interval comparison uses 30-day months. Calendar addition uses
    -- a 365/366-day year: this is positive yet goes back about five days.
    v_bad interval := interval '-1 year 360 days 1 second';
    v_now timestamptz := clock_timestamp();
    v_mode text;
    v_page pgque.batch_page;
    v_repeat pgque.batch_page;
    v_failures text[] := '{}';
    v_before timestamptz;
    v_until timestamptz;
begin
    if not (v_bad > interval '0' and v_now + v_bad < v_now) then
        raise exception 'fixture needs a positive interval with a past calendar deadline';
    end if;
    foreach v_mode in array array['normal', 'coop', 'partition'] loop
        if v_mode <> 'partition' then
            begin
                if v_mode = 'normal' then
                    v_page := pgque.receive_page('page_deadline_wiring', 'normal', 'worker', 1, v_bad);
                else
                    v_page := pgque.receive_page_coop('page_deadline_wiring', 'coop', 'member',
                        'worker', 1, null, v_bad);
                end if;
                raise exception 'nonfuture issuance accepted' using errcode = 'PT001';
            exception
                when invalid_parameter_value then null;
                when sqlstate 'PT001' then
                    v_failures := array_append(v_failures, v_mode || ' issuance');
                    raise notice 'FAIL: % issuance accepted a nonfuture deadline', v_mode;
            end;
        end if;
        -- A valid calendar month is still supported, not converted to seconds.
        if v_mode = 'normal' then
            v_page := pgque.receive_page('page_deadline_wiring', 'normal', 'worker', 1, interval '1 month');
        elsif v_mode = 'coop' then
            v_page := pgque.receive_page_coop('page_deadline_wiring', 'coop', 'member',
                'worker', 1, null, interval '1 month');
        else
            perform pgque.claim_slot('page_deadline_wiring', 'part', 0, 'worker', interval '1 month');
            v_page := pgque.receive_page_partitioned('page_deadline_wiring', 'part', 0, 1, 'worker', 1);
        end if;
        if v_page.status is distinct from 'page' or cardinality(v_page.messages) <> 1
            or v_page.lease_until <= clock_timestamp() then
            raise exception 'fixture must issue a live % page', v_mode;
        end if;
        v_before := clock_timestamp();
        v_until := pgque.renew_page(v_page.page_token, 'worker');
        if v_until not between v_before + interval '1 month'
            and clock_timestamp() + interval '1 month' then
            raise exception '% renewal changed calendar-month semantics', v_mode;
        end if;
        if v_mode = 'partition' then
            update pgque.partition_slot set lease_ttl = v_bad
            where queue_id = (select queue_id from pgque.queue where queue_name = 'page_deadline_wiring')
                and co_name = 'part' and slot = 0;
        else
            update pgque.page_state set pending_lease_ttl = v_bad where pending_token = v_page.page_token;
            begin
                -- Good API argument must not mask a bad stored pending TTL.
                if v_mode = 'normal' then
                    v_repeat := pgque.receive_page('page_deadline_wiring', 'normal', 'worker', 1, interval '1 minute');
                else
                    v_repeat := pgque.receive_page_coop('page_deadline_wiring', 'coop', 'member',
                        'worker', 1, null, interval '1 minute');
                end if;
                raise exception 'nonfuture reconnect accepted' using errcode = 'PT001';
            exception
                when invalid_parameter_value then null;
                when sqlstate 'PT001' then
                    v_failures := array_append(v_failures, v_mode || ' reconnect');
                    raise notice 'FAIL: % reconnect accepted a nonfuture stored TTL', v_mode;
            end;
        end if;
        begin
            perform pgque.renew_page(v_page.page_token, 'worker');
            raise exception 'nonfuture renewal accepted' using errcode = 'PT001';
        exception
            when invalid_parameter_value then null;
            when sqlstate 'PT001' then
                v_failures := array_append(v_failures, v_mode || ' renewal');
                raise notice 'FAIL: % renewal accepted a nonfuture stored TTL', v_mode;
        end;
        if v_mode = 'partition' then
            if (select lease_until from pgque.partition_slot
                where queue_id = (select queue_id from pgque.queue where queue_name = 'page_deadline_wiring')
                    and co_name = 'part' and slot = 0) is distinct from v_until then
                raise exception 'rejected partition renewal changed the lease';
            end if;
        elsif (select pending_lease_until from pgque.page_state
            where pending_token = v_page.page_token) is distinct from v_until then
            raise exception 'rejected % renewal/reconnect changed the lease', v_mode;
        end if;
    end loop;
    if cardinality(v_failures) <> 0 then
        raise exception 'nonfuture deadline accepted by: %', array_to_string(v_failures, ', ');
    end if;
end $$;

select pgque.drop_queue('page_deadline_wiring', true);
\echo 'PASS: actual issuance/reconnect/renewal rejects nonfuture deadlines; calendar months remain valid'
