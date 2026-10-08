-- Every PgQue definer must explicitly place the temporary namespace last.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on

/* Check installed configuration, not shadow objects or simulated resolution.
   Required signatures prevent missing functions or lost SECURITY DEFINER flags
   from making the all-definer catalog scan pass vacuously. Keep this manifest
   when adding functions: the catalog scan also covers new unlisted definers. */
do $$
declare
    v_signature text;
    v_oid oid;
    v_definer boolean;
    v_routine record;
    v_bad integer := 0;
    v_checked integer := 0;
begin
    foreach v_signature in array array[
        'pgque._assert_unpaged(bigint)',
        'pgque._clear_member_cursor(integer,integer)',
        'pgque._clear_paged_active(integer,integer)',
        'pgque._event_retry_core(bigint,bigint,timestamp with time zone)',
        'pgque._is_partition_slot_consumer(integer,text)',
        'pgque._lease_deadline(timestamp with time zone,interval)',
        'pgque._lock_page(uuid)',
        'pgque._nack_batch_event(bigint,pgque.message,interval,text)',
        'pgque._nack_paged_event(bigint,pgque.message,interval,text)',
        'pgque._next_batch_coop(text,text,text,interval,integer,interval,interval,boolean)',
        'pgque._next_batch_custom(text,text,interval,integer,interval,boolean)',
        'pgque._page_failures(jsonb)',
        'pgque._page_messages(pgque.page_state,integer)',
        'pgque._partition_n_cap_guard()',
        'pgque._receive_page(bigint,text,text,integer,interval,text,integer,integer)',
        'pgque._slot_batch(text,text,integer,integer)',
        'pgque._slot_guard(text,text,integer,integer,text)',
        'pgque._transfer_paged_active(integer,integer,integer,bigint)',
        'pgque._validate_coop_names(text,text,text)',
        'pgque._validate_page_args(text,text,text,integer,interval)',
        'pgque._validate_pending_page(pgque.page_state,uuid,text)',
        'pgque.ack(bigint)',
        'pgque.ack_page(uuid,text,jsonb)',
        'pgque.ack_partitioned(text,text,integer,integer,text)',
        'pgque.batch_retry(bigint,integer)',
        'pgque.claim_slot(text,text,integer,text,interval)',
        'pgque.create_queue(text)',
        'pgque.dlq_inspect(text,integer)',
        'pgque.dlq_purge(text,interval)',
        'pgque.dlq_replay(bigint)',
        'pgque.dlq_replay_all(text)',
        'pgque.drop_queue(text,boolean)',
        'pgque.event_dead(bigint,bigint,text,timestamp with time zone,xid8,integer,text,text,text,text,text,text)',
        'pgque.event_retry(bigint,bigint,integer)',
        'pgque.event_retry(bigint,bigint,timestamp with time zone)',
        'pgque.event_retry_raw(text,text,timestamp with time zone,bigint,timestamp with time zone,integer,text,text,text,text,text,text)',
        'pgque.finish_batch(bigint)',
        'pgque.force_next_tick(text)',
        'pgque.force_tick(text)',
        'pgque.get_batch_info(bigint)',
        'pgque.get_consumer_info()',
        'pgque.get_consumer_info(text)',
        'pgque.get_consumer_info(text,text)',
        'pgque.get_queue_info()',
        'pgque.get_queue_info(text)',
        'pgque.grant_perms(text)',
        'pgque.insert_event(text,text,text,text,text,text,text)',
        'pgque.insert_event_bulk(text,text,text[])',
        'pgque.maint()',
        'pgque.maint_idem()',
        'pgque.maint_idem(text)',
        'pgque.nack(bigint,pgque.message,interval,text)',
        'pgque.nack_partitioned(text,text,integer,integer,text,pgque.message,interval,text)',
        'pgque.next_batch(text,text,text,interval)',
        'pgque.next_batch_custom(text,text,interval,integer,interval)',
        'pgque.next_batch_custom(text,text,text,interval,integer,interval,interval)',
        'pgque.receive(text,text,integer)',
        'pgque.receive_coop(text,text,text,integer,interval)',
        'pgque.receive_page(text,text,text,integer,interval)',
        'pgque.receive_page_coop(text,text,text,text,integer,interval,interval)',
        'pgque.receive_page_partitioned(text,text,integer,integer,text,integer)',
        'pgque.receive_partitioned(text,text,integer,integer,text,integer)',
        'pgque.register_consumer(text,text)',
        'pgque.register_consumer_at(text,text,bigint)',
        'pgque.register_subconsumer(text,text,text,boolean)',
        'pgque.release_slot(text,text,integer,text)',
        'pgque.renew_page(uuid,text)',
        'pgque.send(text,jsonb)',
        'pgque.send(text,text)',
        'pgque.send(text,text,jsonb)',
        'pgque.send(text,text,jsonb,text)',
        'pgque.send(text,text,text)',
        'pgque.send(text,text,text,text)',
        'pgque.send_batch(text,jsonb[])',
        'pgque.send_batch(text,text,jsonb[])',
        'pgque.send_batch(text,text,text[])',
        'pgque.send_batch(text,text[])',
        'pgque.send_idem(text,text,jsonb,text,interval,text)',
        'pgque.send_idem(text,text,text,text,interval,text)',
        'pgque.set_queue_config(text,text,text)',
        'pgque.set_tick_period_ms(integer)',
        'pgque.start()',
        'pgque.start_timetable(integer)',
        'pgque.status()',
        'pgque.stop()',
        'pgque.stop_timetable()',
        'pgque.subscribe(text,text)',
        'pgque.subscribe_partitioned(text,text,integer)',
        'pgque.subscribe_slot(text,text,integer,integer)',
        'pgque.subscribe_subconsumer(text,text,text,boolean)',
        'pgque.ticker()',
        'pgque.ticker(text)',
        'pgque.ticker(text,bigint,timestamp with time zone,bigint)',
        'pgque.touch_subconsumer(text,text,text)',
        'pgque.uninstall()',
        'pgque.unregister_consumer(text,text)',
        'pgque.unregister_subconsumer(text,text,text,integer)',
        'pgque.unsubscribe(text,text)',
        'pgque.unsubscribe_partitioned(text,text)',
        'pgque.unsubscribe_slot(text,text,integer)',
        'pgque.unsubscribe_subconsumer(text,text,text,integer)',
        'pgque.version()'
    ] loop
        v_oid := pg_catalog.to_regprocedure(v_signature);
        if v_oid is null then
            raise exception 'expected definer function missing: %', v_signature;
        end if;
        select p.prosecdef into v_definer
        from pg_catalog.pg_proc as p
        where p.oid = v_oid;
        if v_definer is distinct from true then
            raise exception 'expected SECURITY DEFINER function: %', v_signature;
        end if;
    end loop;

    for v_routine in
        select p.oid::pg_catalog.regprocedure as signature, (
            select setting
            from pg_catalog.unnest(p.proconfig) as setting
            where setting like 'search_path=%'
        ) as path
        from pg_catalog.pg_proc as p
        join pg_catalog.pg_namespace as n on n.oid = p.pronamespace
        where n.nspname = 'pgque' and p.prosecdef
        order by p.oid::pg_catalog.regprocedure::text
    loop
        v_checked := v_checked + 1;
        if v_routine.path is distinct from 'search_path=pgque, pg_catalog, pg_temp' then
            v_bad := v_bad + 1;
            raise warning 'definer % must explicitly place pg_temp last, got path=%',
                v_routine.signature, v_routine.path;
        end if;
    end loop;
    if v_bad <> 0 then
        raise exception '% definer routine(s) have an unsafe configured search_path', v_bad;
    end if;
    if v_checked = 0 then
        raise exception 'no PgQue SECURITY DEFINER routines were checked';
    end if;
    raise notice 'checked all % PgQue SECURITY DEFINER routines', v_checked;
end $$;
\echo 'PASS: all required definers exist and every PgQue definer explicitly places pg_temp last'
