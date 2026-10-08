-- Force-drop orphan cleanup versus another queue's registration.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Disposable test database only. Statement trigger is timing instrumentation;
-- the installed drop_queue and registration functions remain unchanged.
\set ON_ERROR_STOP on
create extension if not exists dblink;
create schema force_drop_test;

create function force_drop_test.gate() returns trigger as $$
declare
    v_key bigint := nullif(current_setting('force_drop_test.gate', true), '')::bigint;
    v_inject jsonb := nullif(current_setting('force_drop_test.inject', true), '')::jsonb;
begin
    if tg_table_name = 'consumer' and v_inject is not null then
        raise exception 'C05 injected integrity error'
            using errcode = v_inject->>'code', schema = v_inject->>'schema',
                table = v_inject->>'table', constraint = v_inject->>'constraint';
    end if;
    if v_key is not null and (
        (current_setting('force_drop_test.phase', true) = 'consumer'
            and tg_table_name = 'consumer')
        or (current_setting('force_drop_test.phase', true) = 'subscription'
            and tg_table_name = 'subscription')
    ) then
        raise log 'C05 barrier enter: pid=%, table=%, timing=%, snapshot=%, isolation=%',
            pg_backend_pid(), tg_table_name, tg_when,
            pg_current_snapshot(), current_setting('transaction_isolation');
        perform pg_advisory_xact_lock(v_key);
        raise notice 'C05 barrier released';
    end if;
    return null;
end $$ language plpgsql;
create trigger force_drop_test_gate before delete on pgque.consumer
for each statement execute function force_drop_test.gate();

create trigger force_drop_test_subscription_gate after delete on pgque.subscription
for each statement execute function force_drop_test.gate();

create function force_drop_test.consumer_locked(i_consumer int) returns boolean as $$
begin
    perform 1 from pgque.consumer where co_id=i_consumer for update nowait;
    return false;
exception when lock_not_available then return true;
end $$ language plpgsql;

create function force_drop_test.attempt(i_queue text) returns jsonb as $$
declare
    v_state text;
    v_message text;
    v_constraint text;
    v_result int;
begin
    begin
        v_result := pgque.drop_queue(i_queue, true);
        return jsonb_build_object('sqlstate', '00000', 'return_value', v_result);
    exception when others then
        get stacked diagnostics v_state = returned_sqlstate, v_message = message_text,
            v_constraint = constraint_name;
        return jsonb_build_object('sqlstate', v_state, 'message', v_message, 'constraint', v_constraint);
    end;
end $$ language plpgsql;

create function force_drop_test.snapshot(i_queue text) returns jsonb as $$
declare
    v_q pgque.queue%rowtype;
    v_events bigint;
begin
    select * into v_q from pgque.queue where queue_name=i_queue;
    if not found then return null; end if;
    execute 'select count(*) from ' || pgque.quote_fqname(v_q.queue_data_pfx) into v_events;
    return jsonb_build_object('queue', to_jsonb(v_q), 'events', v_events,
        'subscriptions', (select jsonb_agg(to_jsonb(s) order by sub_consumer)
            from pgque.subscription s where sub_queue=v_q.queue_id),
        'ticks', (select jsonb_agg(to_jsonb(t) order by tick_id)
            from pgque.tick t where tick_queue=v_q.queue_id),
        'retries', (select jsonb_agg(to_jsonb(r)) from pgque.retry_queue r where ev_queue=v_q.queue_id),
        'relations', (select jsonb_agg(jsonb_build_array(oid, relname, relkind) order by oid)
            from pg_class where relnamespace='pgque'::regnamespace
                and relname like split_part(v_q.queue_data_pfx, '.', 2) || '%'));
end $$ language plpgsql;

create table force_drop_test.owned_queues (queue_name text primary key);
create table force_drop_test.results (
    isolation text, scenario text, result jsonb, target_atomic boolean,
    surviving_registration boolean, expected_outcome boolean
);

-- Setup needs committed states for the independent connections below.
do $$
declare
    v_iso text;
    v_scenario text;
    v_suffix text;
begin
    foreach v_iso in array array['read committed', 'repeatable read', 'serializable'] loop
        foreach v_scenario in array array['still_locked', 'committed_before', 'snapshot_then_commit', 'old_transaction_snapshot'] loop
            v_suffix := replace(v_iso, ' ', '_') || '_' || v_scenario;
            if exists(select 1 from pgque.queue where queue_name in
                ('c05_target_' || v_suffix, 'c05_other_' || v_suffix))
                or exists(select 1 from pgque.consumer where co_name='c05_consumer_' || v_suffix) then
                raise exception 'C05 fixture name collision: %',v_suffix;
            end if;
            insert into force_drop_test.owned_queues values
                ('c05_target_' || v_suffix), ('c05_other_' || v_suffix);
            perform pgque.create_queue('c05_target_' || v_suffix);
            perform pgque.create_queue('c05_other_' || v_suffix);
            perform pgque.subscribe('c05_target_' || v_suffix, 'c05_consumer_' || v_suffix);
            perform pgque.send('c05_target_' || v_suffix, 'test', 'target-event');
        end loop;
    end loop;
end $$;

do $$
declare
    v_iso text;
    v_scenario text;
    v_suffix text;
    v_target text;
    v_other text;
    v_consumer text;
    v_before jsonb;
    v_after jsonb;
    v_result jsonb;
    v_dropper int;
    v_registrar int;
    v_actual_isolation text;
    v_locked boolean;
    v_retry jsonb;
    v_consumer_id int;
    v_key bigint := 604050707;
    v_deadline timestamptz;
    v_gate_seen boolean;
    v_survives boolean;
    v_atomic boolean;
    v_expected boolean;
    v_fk record;
begin
    select condeferrable, condeferred, confdeltype into strict v_fk
    from pg_constraint where conrelid='pgque.subscription'::regclass
        and confrelid='pgque.consumer'::regclass and contype='f';
    if v_fk.condeferrable or v_fk.condeferred or v_fk.confdeltype <> 'a' then
        raise exception 'fixture requires the intact immediate non-cascading subscription consumer FK';
    end if;
    perform dblink_connect('c05_observer', 'dbname=' || quote_literal(current_database()));
    foreach v_iso in array array['read committed', 'repeatable read', 'serializable'] loop
        foreach v_scenario in array array['still_locked', 'committed_before', 'snapshot_then_commit', 'old_transaction_snapshot'] loop
            v_suffix := replace(v_iso, ' ', '_') || '_' || v_scenario;
            v_target := 'c05_target_' || v_suffix;
            v_other := 'c05_other_' || v_suffix;
            v_consumer := 'c05_consumer_' || v_suffix;
            select co_id into strict v_consumer_id from pgque.consumer where co_name=v_consumer;
            select snapshot into v_before from dblink('c05_observer',
                format('select force_drop_test.snapshot(%L)',v_target)) as t(snapshot jsonb);
            perform dblink_connect('c05_registrar', 'dbname=' || quote_literal(current_database()));
            perform dblink_connect('c05_dropper', 'dbname=' || quote_literal(current_database()));
            perform dblink_exec('c05_registrar', 'set statement_timeout=''15s''');
            perform dblink_exec('c05_dropper', 'set statement_timeout=''15s''');
            select pid into v_registrar from dblink('c05_registrar', 'select pg_backend_pid()') as t(pid int);
            select pid into v_dropper from dblink('c05_dropper', 'select pg_backend_pid()') as t(pid int);
            perform dblink_exec('c05_registrar', 'begin');
            perform result from dblink('c05_registrar',
                format('select pgque.register_consumer(%L,%L)',v_other,v_consumer)) as t(result int);
            -- dblink returns only after the registration and its FK check finish.
            if v_scenario = 'committed_before' then
                perform dblink_exec('c05_registrar', 'commit');
            end if;
            perform dblink_exec('c05_dropper', 'begin isolation level ' || v_iso);
            select isolation into v_actual_isolation from dblink('c05_dropper',
                'select current_setting(''transaction_isolation'')') as t(isolation text);
            if v_actual_isolation is distinct from v_iso then
                raise exception 'requested isolation % but got %',v_iso,v_actual_isolation;
            end if;
            if v_scenario in ('snapshot_then_commit', 'old_transaction_snapshot') then
                perform pg_advisory_lock(v_key);
                perform dblink_exec('c05_dropper', format('set local force_drop_test.gate=%L',v_key::text));
                perform dblink_exec('c05_dropper', format('set local force_drop_test.phase=%L',
                    case when v_scenario='snapshot_then_commit' then 'consumer' else 'subscription' end));
            end if;
            perform dblink_send_query('c05_dropper', format('select force_drop_test.attempt(%L)',v_target));
            if v_scenario in ('snapshot_then_commit', 'old_transaction_snapshot') then
                v_deadline := clock_timestamp()+interval '10 seconds';
                v_gate_seen := false;
                while clock_timestamp()<v_deadline loop
                    if pg_backend_pid()=any(pg_blocking_pids(v_dropper)) then
                        v_gate_seen := true;
                        exit;
                    end if;
                    if dblink_is_busy('c05_dropper')=0 then exit; end if;
                    perform pg_sleep(0.01);
                end loop;
                if not v_gate_seen then raise exception '%: dropper did not reach the snapshot gate',v_iso; end if;
                select locked into v_locked from dblink('c05_observer',
                    format('select force_drop_test.consumer_locked(%s)',v_consumer_id)) as t(locked boolean);
                if not v_locked then raise exception 'registrar must still own its consumer lock'; end if;
                raise notice 'C05 observed barrier: isolation=%, scenario=%, dropper=%, controller=%, registrar=%',
                    v_iso,v_scenario,v_dropper,pg_backend_pid(),v_registrar;
                -- The consumer gate pauses the old combined DELETE before its
                -- CTE candidate scan. The repaired code may already have skipped
                -- the locked identity in its separate locking statement. The
                -- subscription gate separately tests a retained transaction
                -- snapshot before any consumer candidate lock.
                perform dblink_exec('c05_registrar', 'commit');
                select locked into v_locked from dblink('c05_observer',
                    format('select force_drop_test.consumer_locked(%s)',v_consumer_id)) as t(locked boolean);
                if v_locked then raise exception 'dropper must not yet own the consumer row at this gate'; end if;
                perform pg_advisory_unlock(v_key);
            end if;
            select result into strict v_result from dblink_get_result('c05_dropper') as t(result jsonb);
            -- Drain libpq's final result before the next remote command.
            perform * from dblink_get_result('c05_dropper') as t(result jsonb);
            perform dblink_exec('c05_dropper',
                case when v_result->>'sqlstate'='00000' then 'commit' else 'rollback' end);
            if v_scenario = 'still_locked' then
                perform dblink_exec('c05_registrar', 'commit');
            end if;
            select snapshot into v_after from dblink('c05_observer',
                format('select force_drop_test.snapshot(%L)',v_target)) as t(snapshot jsonb);
            v_atomic := case when v_result->>'sqlstate'='00000'
                then v_after is null else v_after is not distinct from v_before end;
            select exists(select 1 from pgque.subscription s join pgque.queue q on q.queue_id=s.sub_queue
                join pgque.consumer c on c.co_id=s.sub_consumer
                where q.queue_name=v_other and c.co_name=v_consumer and c.co_id=v_consumer_id)
            into v_survives;
            v_expected := v_result->>'sqlstate'='00000'
                or (v_iso<>'read committed' and v_result->>'sqlstate'='40001');
            insert into force_drop_test.results values(v_iso,v_scenario,v_result,v_atomic,v_survives,v_expected);
            raise notice 'C05 result: %', jsonb_build_object('isolation',v_iso,'scenario',v_scenario,
                'result',v_result,'target_atomic',v_atomic,'surviving_registration',v_survives,
                'expected_outcome',v_expected);
            if v_result->>'sqlstate'='40001' then
                -- The caller retries the entire transaction after the failure.
                perform dblink_exec('c05_dropper', 'begin isolation level ' || v_iso);
                select result into v_retry from dblink('c05_dropper',
                    format('select force_drop_test.attempt(%L)',v_target)) as t(result jsonb);
                perform dblink_exec('c05_dropper', 'commit');
                if v_retry->>'sqlstate' is distinct from '00000'
                    or force_drop_test.snapshot(v_target) is not null then
                    raise exception 'fresh transaction retry did not finish force drop: %',v_retry;
                end if;
                raise notice 'C05 retry succeeded: isolation=%, scenario=%',v_iso,v_scenario;
            end if;
            perform dblink_disconnect('c05_dropper');
            perform dblink_disconnect('c05_registrar');
        end loop;
    end loop;
    perform dblink_disconnect('c05_observer');
end $$;

table force_drop_test.results;

-- Check every error discriminator. Only the exact higher-isolation race above
-- may become a serialization error; unrelated failures must remain visible.
do $$
begin
    if exists(select 1 from pgque.queue where queue_name in ('c05_error_scope','c05_orphan_control'))
        or exists(select 1 from pgque.consumer where co_name in ('c05_error_scope','c05_orphan_control')) then
        raise exception 'C05 error-scope fixture name collision';
    end if;
    insert into force_drop_test.owned_queues values ('c05_error_scope'),('c05_orphan_control');
    perform pgque.create_queue('c05_error_scope');
    perform pgque.subscribe('c05_error_scope','c05_error_scope');
    perform pgque.send('c05_error_scope','test','retained-event');
    perform pgque.create_queue('c05_orphan_control');
    perform pgque.subscribe('c05_orphan_control','c05_orphan_control');
end $$;
do $$
declare
    v_case record;
    v_inject jsonb;
    v_before jsonb;
    v_after jsonb;
    v_result jsonb;
begin
    perform dblink_connect('c05_scope', 'dbname=' || quote_literal(current_database()));
    perform dblink_connect('c05_observer', 'dbname=' || quote_literal(current_database()));
    perform dblink_exec('c05_scope','set statement_timeout=''15s''');
    select snapshot into v_before from dblink('c05_observer',
        'select force_drop_test.snapshot(''c05_error_scope'')') as t(snapshot jsonb);
    for v_case in select * from (values
        ('schema', 'repeatable read', '23503', 'other_schema', 'subscription', 'sub_consumer_fkey'),
        ('table', 'repeatable read', '23503', 'pgque', 'other_table', 'sub_consumer_fkey'),
        ('constraint', 'repeatable read', '23503', 'pgque', 'subscription', 'other_fkey'),
        ('sqlstate', 'repeatable read', '23514', 'pgque', 'subscription', 'sub_consumer_fkey'),
        ('isolation', 'read committed', '23503', 'pgque', 'subscription', 'sub_consumer_fkey')
    ) as cases(label, isolation, code, schema_name, table_name, constraint_name) loop
        v_inject := jsonb_build_object('code',v_case.code,'schema',v_case.schema_name,
            'table',v_case.table_name,'constraint',v_case.constraint_name);
        perform dblink_exec('c05_scope','begin isolation level ' || v_case.isolation);
        perform dblink_exec('c05_scope',format('set local force_drop_test.inject=%L',v_inject::text));
        select result into v_result from dblink('c05_scope',
            'select force_drop_test.attempt(''c05_error_scope'')') as t(result jsonb);
        perform dblink_exec('c05_scope','rollback');
        select snapshot into v_after from dblink('c05_observer',
            'select force_drop_test.snapshot(''c05_error_scope'')') as t(snapshot jsonb);
        if v_result->>'sqlstate' is distinct from v_case.code
            or v_result->>'message' is distinct from 'C05 injected integrity error'
            or v_after is distinct from v_before then
            raise exception 'C05 error discriminator % failed: %',v_case.label,v_result;
        end if;
        raise notice 'PASS: C05 error discriminator % preserves % and target state',v_case.label,v_case.code;
    end loop;
    select result into v_result from dblink('c05_scope',
        'select force_drop_test.attempt(''c05_orphan_control'')') as t(result jsonb);
    if v_result->>'sqlstate' is distinct from '00000'
        or exists(select 1 from pgque.queue where queue_name='c05_orphan_control')
        or exists(select 1 from pgque.consumer where co_name='c05_orphan_control') then
        raise exception 'C05 unreferenced consumer cleanup regressed: %',v_result;
    end if;
    raise notice 'PASS: C05 unreferenced consumer is removed';
    perform dblink_disconnect('c05_scope');
    perform dblink_disconnect('c05_observer');
end $$;

-- Keep the result table until the final assertion; always remove instrumentation.
drop trigger force_drop_test_gate on pgque.consumer;
drop trigger force_drop_test_subscription_gate on pgque.subscription;
do $$
declare v_queue text;
begin
    for v_queue in select q.queue_name from pgque.queue q
        join force_drop_test.owned_queues own on own.queue_name=q.queue_name loop
        perform pgque.drop_queue(v_queue,true);
    end loop;
end $$;
do $$
begin
    if exists(select 1 from force_drop_test.results where not target_atomic or not surviving_registration or not expected_outcome) then
        raise exception 'C05: force-drop outcome, FK survival, or atomicity regression (see result rows)';
    end if;
end $$;
drop schema force_drop_test cascade;
\echo 'PASS: force-drop registration snapshot/lock qualification'
