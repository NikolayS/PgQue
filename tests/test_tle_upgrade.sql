-- test_tle_upgrade.sql -- Data-preserving pg_tle upgrade from 0.2.0.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Preconditions: pg_tle is preloaded and PgQue is installed from the
-- v0.2.0-tagged sql/pgque-tle.sql artifact. CI deliberately reads the tag,
-- rather than the mutable sql/ stable directory, so final promotion cannot
-- accidentally replace the source fixture with the version under test.
--
-- Local setup from the repository root:
--   PAGER=cat psql --no-psqlrc "$PGQUE_TEST_DSN" --command='create extension pg_tle'
--   git show v0.2.0:sql/pgque-tle.sql | PAGER=cat psql --no-psqlrc "$PGQUE_TEST_DSN" --set=ON_ERROR_STOP=1
--   PAGER=cat psql --no-psqlrc "$PGQUE_TEST_DSN" --command='create extension pgque'
--   PAGER=cat psql --no-psqlrc "$PGQUE_TEST_DSN" --set=ON_ERROR_STOP=1 --file=tests/test_tle_upgrade.sql

\set ON_ERROR_STOP on

\echo '=== test_tle_upgrade (0.2.0 -> current via real pg_tle) ==='

do $$
begin
    assert pgque.version() = '0.2.0',
        format('upgrade fixture must start at 0.2.0, got %s', pgque.version());
end $$;

create temporary table _tle_upgrade_state (
    kind text primary key,
    event_id bigint not null,
    batch_id bigint
);

-- Preserve a queue, subscription, a terminal DLQ event, a scheduled retry,
-- and a not-yet-ticked event. These cover each durable event location plus
-- the consumer cursor state that users would lose on uninstall/reinstall.
select pgque.create_queue('tle_upgrade');
select pgque.subscribe('tle_upgrade', 'worker');
select pgque.set_queue_config('tle_upgrade', 'max_retries', '0');

insert into _tle_upgrade_state(kind, event_id)
select 'dead_letter', pgque.send('tle_upgrade', 'upgrade.dlq', '{"state":"dlq"}'::jsonb);
select pgque.force_next_tick('tle_upgrade');
select pgque.ticker();

do $$
declare
    v_msg pgque.message;
begin
    select * into strict v_msg from pgque.receive('tle_upgrade', 'worker', 1);
    assert v_msg.msg_id = (select event_id from _tle_upgrade_state where kind = 'dead_letter');
    perform pgque.nack(v_msg.batch_id, v_msg, interval '1 day', 'upgrade fixture');
    update _tle_upgrade_state set batch_id = v_msg.batch_id where kind = 'dead_letter';
end $$;

do $$
begin
    perform pgque.ack((select batch_id from _tle_upgrade_state where kind = 'dead_letter'));
end $$;

select pgque.set_queue_config('tle_upgrade', 'max_retries', '5');
insert into _tle_upgrade_state(kind, event_id)
select 'retry', pgque.send('tle_upgrade', 'upgrade.retry', '{"state":"retry"}'::jsonb);
select pgque.force_next_tick('tle_upgrade');
select pgque.ticker();

do $$
declare
    v_msg pgque.message;
begin
    select * into strict v_msg from pgque.receive('tle_upgrade', 'worker', 1);
    assert v_msg.msg_id = (select event_id from _tle_upgrade_state where kind = 'retry');
    perform pgque.nack(v_msg.batch_id, v_msg, interval '1 day', 'upgrade fixture');
    update _tle_upgrade_state set batch_id = v_msg.batch_id where kind = 'retry';
end $$;

do $$
begin
    perform pgque.ack((select batch_id from _tle_upgrade_state where kind = 'retry'));
end $$;

insert into _tle_upgrade_state(kind, event_id)
select 'pending', pgque.send('tle_upgrade', 'upgrade.pending', '{"state":"pending"}'::jsonb);

do $$
declare
    v_pending_id bigint := (select event_id from _tle_upgrade_state where kind = 'pending');
    v_pending_exists boolean;
begin
    assert exists (
        select 1 from pgque.queue where queue_name = 'tle_upgrade'
    ), '0.2 queue fixture missing before upgrade';
    assert exists (
        select 1 from pgque.subscription as s
        join pgque.queue as q on q.queue_id = s.sub_queue
        join pgque.consumer as c on c.co_id = s.sub_consumer
        where q.queue_name = 'tle_upgrade' and c.co_name = 'worker'
    ), '0.2 subscription fixture missing before upgrade';
    assert exists (
        select 1 from pgque.dead_letter
        where ev_id = (select event_id from _tle_upgrade_state where kind = 'dead_letter')
    ), '0.2 DLQ fixture missing before upgrade';
    assert exists (
        select 1 from pgque.retry_queue
        where ev_id = (select event_id from _tle_upgrade_state where kind = 'retry')
    ), '0.2 retry fixture missing before upgrade';

    execute format(
        'select exists (select 1 from %s where ev_id = $1)',
        pgque.current_event_table('tle_upgrade')
    ) into v_pending_exists using v_pending_id;
    assert v_pending_exists, '0.2 pending-event fixture missing before upgrade';
end $$;

-- Registration must not mutate the active 0.2.0 extension. A second run is a
-- no-op, proving deployment retries cannot duplicate version/update records.
\i devel/sql/pgque-tle.sql
\i devel/sql/pgque-tle.sql

do $$
declare
    v_installed text;
    v_target text;
begin
    select default_version into v_target
    from pgtle.available_extensions() where name = 'pgque';
    select extversion into v_installed
    from pg_catalog.pg_extension where extname = 'pgque';

    assert v_target <> '0.2.0',
        'registration must advertise the new target version';
    assert v_installed = '0.2.0',
        format('registration must not update active extension, got %s', v_installed);
    assert exists (
        select 1 from pgtle.extension_update_paths('pgque')
        where source = '0.2.0' and target = v_target
          and path = '0.2.0--' || v_target
    ), '0.2.0 -> current pg_tle update path missing';
end $$;

alter extension pgque update;

do $$
declare
    v_target text;
    v_pending_id bigint := (select event_id from _tle_upgrade_state where kind = 'pending');
    v_pending_exists boolean;
    v_first_id bigint;
    v_second_id bigint;
    v_deduped boolean;
begin
    select default_version into v_target
    from pgtle.available_extensions() where name = 'pgque';

    assert pgque.version() = v_target,
        format('runtime version should be %s, got %s', v_target, pgque.version());
    assert (select extversion from pg_catalog.pg_extension where extname = 'pgque') = v_target,
        'pg_extension version did not advance';

    assert exists (
        select 1 from pgque.queue where queue_name = 'tle_upgrade'
    ), 'queue was lost during pg_tle update';
    assert exists (
        select 1 from pgque.subscription as s
        join pgque.queue as q on q.queue_id = s.sub_queue
        join pgque.consumer as c on c.co_id = s.sub_consumer
        where q.queue_name = 'tle_upgrade' and c.co_name = 'worker'
    ), 'subscription was lost during pg_tle update';
    assert exists (
        select 1 from pgque.dead_letter
        where ev_id = (select event_id from _tle_upgrade_state where kind = 'dead_letter')
          and ev_data::jsonb = '{"state":"dlq"}'::jsonb
    ), 'dead-letter state was lost during pg_tle update';
    assert exists (
        select 1 from pgque.retry_queue
        where ev_id = (select event_id from _tle_upgrade_state where kind = 'retry')
          and ev_data::jsonb = '{"state":"retry"}'::jsonb
    ), 'retry state was lost during pg_tle update';

    execute format(
        'select exists (select 1 from %s where ev_id = $1 and ev_data::jsonb = $2)',
        pgque.current_event_table('tle_upgrade')
    ) into v_pending_exists using v_pending_id, '{"state":"pending"}'::jsonb;
    assert v_pending_exists, 'pending event was lost during pg_tle update';

    select event_id, deduped into v_first_id, v_deduped
    from pgque.send_idem(
        'tle_upgrade', 'upgrade.new-api', '{"v":3}'::jsonb,
        'tle-upgrade:new-api', interval '1 hour'
    );
    assert v_first_id is not null and not v_deduped,
        'new send_idem API did not insert after upgrade';

    select event_id, deduped into v_second_id, v_deduped
    from pgque.send_idem(
        'tle_upgrade', 'upgrade.new-api', '{"v":3}'::jsonb,
        'tle-upgrade:new-api', interval '1 hour'
    );
    assert v_second_id = v_first_id and v_deduped,
        'new send_idem API did not deduplicate after upgrade';

    raise notice 'PASS: 0.2.0 pg_tle state survived and 0.3 APIs work';
end $$;

\echo '=== test_tle_upgrade: ALL PASSED ==='
