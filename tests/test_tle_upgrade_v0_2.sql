-- test_tle_upgrade_v0_2.sql -- Non-destructive pg_tle 0.2.0 to 0.2.1 upgrade.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- The CI caller writes the tagged v0.2.0 pg_tle installer to
-- /tmp/pgque-tle-v0.2.0.sql before running this test.

\set ON_ERROR_STOP on

create extension pg_tle;
\i /tmp/pgque-tle-v0.2.0.sql
create extension pgque;

select pgque.create_queue('tle_upgrade_overflow');
select pgque.register_consumer('tle_upgrade_overflow', 'processor');
select pgque.send('tle_upgrade_overflow', 'event', '{"n":1}'::jsonb);
select pgque.send('tle_upgrade_overflow', 'event', '{"n":2}'::jsonb);
select pgque.send('tle_upgrade_overflow', 'event', '{"n":3}'::jsonb);

select pgque.create_queue('tle_upgrade_active');
select pgque.register_consumer('tle_upgrade_active', 'processor');
select pgque.send('tle_upgrade_active', 'event', '{"active":true}'::jsonb);

select pgque.create_queue('tle_upgrade_coop');
select pgque.register_subconsumer('tle_upgrade_coop', 'workers', 'worker_1');
select pgque.send('tle_upgrade_coop', 'event', '{"n":1}'::jsonb);
select pgque.send('tle_upgrade_coop', 'event', '{"n":2}'::jsonb);
select pgque.send('tle_upgrade_coop', 'event', '{"n":3}'::jsonb);

select pgque.force_next_tick('tle_upgrade_overflow');
select pgque.force_next_tick('tle_upgrade_active');
select pgque.force_next_tick('tle_upgrade_coop');
select pgque.ticker();

do $$
declare
    v_msg pgque.message;
begin
    select * into strict v_msg
    from pgque.receive('tle_upgrade_active', 'processor', 10);
    assert v_msg.batch_id is not null, 'expected active batch before upgrade';
end $$;

create temporary table tle_upgrade_state as
select q.queue_name, s.sub_batch, s.sub_last_tick, s.sub_next_tick
from pgque.subscription as s
join pgque.queue as q on q.queue_id = s.sub_queue
join pgque.consumer as c on c.co_id = s.sub_consumer
where (q.queue_name in ('tle_upgrade_overflow', 'tle_upgrade_active')
       and c.co_name = 'processor')
   or (q.queue_name = 'tle_upgrade_coop' and c.co_name = 'workers.worker_1');

\i sql/pgque-tle.sql
\i sql/pgque-tle.sql

do $$
begin
    assert (select extversion = '0.2.0'
            from pg_catalog.pg_extension where extname = 'pgque'),
        'registering the update must not mutate the installed extension';
    assert exists (
        select 1
        from pgtle.extension_update_paths('pgque')
        where source = '0.2.0' and target = '0.2.1' and path is not null
    ), 'pg_tle must expose the 0.2.0 to 0.2.1 update path';
    assert (select default_version = '0.2.1'
            from pgtle.available_extensions() where name = 'pgque'),
        'pg_tle default version must be 0.2.1';
end $$;

alter extension pgque update to '0.2.1';

do $$
declare
    v_before record;
    v_after record;
    v_msg pgque.message;
    v_count integer := 0;
    v_batch_id bigint;
    v_raised boolean := false;
    v_sqlstate text;
begin
    assert (select extversion = '0.2.1'
            from pg_catalog.pg_extension where extname = 'pgque'),
        'pg_extension.extversion must be 0.2.1';
    assert pgque.version() = '0.2.1',
        format('pgque.version() must be 0.2.1, got %s', pgque.version());

    for v_before in select * from tle_upgrade_state
    loop
        select s.sub_batch, s.sub_last_tick, s.sub_next_tick
        into strict v_after
        from pgque.subscription as s
        join pgque.queue as q on q.queue_id = s.sub_queue
        join pgque.consumer as c on c.co_id = s.sub_consumer
        where q.queue_name = v_before.queue_name
          and c.co_name = case
              when v_before.queue_name = 'tle_upgrade_coop'
              then 'workers.worker_1'
              else 'processor'
          end;

        assert v_after.sub_batch is not distinct from v_before.sub_batch
           and v_after.sub_last_tick is not distinct from v_before.sub_last_tick
           and v_after.sub_next_tick is not distinct from v_before.sub_next_tick,
            format('subscription state changed for queue %s', v_before.queue_name);
    end loop;

    begin
        perform * from pgque.receive('tle_upgrade_overflow', 'processor', 2);
    exception
        when sqlstate '54000' then
            get stacked diagnostics v_sqlstate = returned_sqlstate;
            v_raised := true;
    end;
    assert v_raised and v_sqlstate = '54000',
        'upgraded receive must reject overflow with SQLSTATE 54000';

    for v_msg in
        select * from pgque.receive('tle_upgrade_overflow', 'processor', 3)
    loop
        v_count := v_count + 1;
        v_batch_id := v_msg.batch_id;
    end loop;
    assert v_count = 3,
        format('expected all 3 overflow events after retry, got %s', v_count);
    perform pgque.ack(v_batch_id);

    v_raised := false;
    v_sqlstate := null;
    begin
        perform * from pgque.receive_coop(
            'tle_upgrade_coop', 'workers', 'worker_1', 2
        );
    exception
        when sqlstate '54000' then
            get stacked diagnostics v_sqlstate = returned_sqlstate;
            v_raised := true;
    end;
    assert v_raised and v_sqlstate = '54000',
        'upgraded receive_coop must reject overflow with SQLSTATE 54000';

    v_count := 0;
    v_batch_id := null;
    for v_msg in
        select * from pgque.receive_coop(
            'tle_upgrade_coop', 'workers', 'worker_1', 3
        )
    loop
        v_count := v_count + 1;
        v_batch_id := v_msg.batch_id;
    end loop;
    assert v_count = 3,
        format('expected all 3 cooperative events after retry, got %s', v_count);
    perform pgque.ack(v_batch_id);
end $$;

-- The update edge must also make the new default usable for a fresh create.
drop extension pgque cascade;
create extension pgque;

do $$
begin
    assert (select extversion = '0.2.1'
            from pg_catalog.pg_extension where extname = 'pgque'),
        'fresh create after update registration must install 0.2.1';
    assert pgque.version() = '0.2.1',
        'fresh create after update registration must expose version 0.2.1';
end $$;

\echo '=== test_tle_upgrade_v0_2: ALL PASSED ==='
