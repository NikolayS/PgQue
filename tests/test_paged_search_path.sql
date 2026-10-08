-- Paging and legacy wrapper definer functions must place the temporary namespace last.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on

/* Check installed configuration, not source text or simulated shadow objects.
   PostgreSQL's CREATE FUNCTION guidance requires trusted schemas before pg_temp. */
do $$
declare
    v_signature text;
    v_oid oid;
    v_definer boolean;
    v_path text;
    v_bad integer := 0;
begin
    foreach v_signature in array array[
        'pgque._lease_deadline(timestamp with time zone,interval)',
        'pgque._assert_unpaged(bigint)',
        'pgque._clear_paged_active(integer,integer)',
        'pgque._transfer_paged_active(integer,integer,integer,bigint)',
        'pgque._event_retry_core(bigint,bigint,timestamp with time zone)',
        'pgque._nack_paged_event(bigint,pgque.message,interval,text)',
        'pgque._page_messages(pgque.page_state,integer)',
        'pgque._receive_page(bigint,text,text,integer,interval,text,integer,integer)',
        'pgque._validate_page_args(text,text,text,integer,interval)',
        'pgque.receive_page(text,text,text,integer,interval)',
        'pgque.receive_page_coop(text,text,text,text,integer,interval,interval)',
        'pgque.receive_page_partitioned(text,text,integer,integer,text,integer)',
        'pgque._lock_page(uuid)',
        'pgque._validate_pending_page(pgque.page_state,uuid,text)',
        'pgque.renew_page(uuid,text)',
        'pgque._page_failures(jsonb)',
        'pgque.ack_page(uuid,text,jsonb)',
        'pgque.event_retry(bigint,bigint,timestamp with time zone)',
        'pgque.event_retry(bigint,bigint,integer)',
        'pgque.batch_retry(bigint,integer)',
        'pgque.register_consumer_at(text,text,bigint)',
        'pgque.drop_queue(text,boolean)'
    ] loop
        v_oid := to_regprocedure(v_signature);
        if v_oid is null then
            raise exception 'expected function missing: %', v_signature;
        end if;
        select p.prosecdef, (
            select setting
            from unnest(p.proconfig) as setting
            where setting like 'search_path=%'
        )
        into v_definer, v_path
        from pg_proc as p
        where p.oid = v_oid;

        if not v_definer or v_path is distinct from 'search_path=pgque, pg_catalog, pg_temp' then
            v_bad := v_bad + 1;
            raise warning 'paging function % must be a definer with pg_temp last, got definer=% path=%',
                v_signature, v_definer, v_path;
        end if;
    end loop;
    if v_bad <> 0 then
        raise exception '% function(s) have an unsafe configured search_path', v_bad;
    end if;
end $$;
\echo 'PASS: all 22 lease, paging and legacy wrapper functions explicitly place pg_temp last'
