-- test_tick_period.sql -- Verify configurable tick period
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- Test 1: default tick_period_ms is 100 (10 ticks/sec)
do $$
declare
    v_period integer;
begin
    select tick_period_ms into v_period from pgque.config;
    assert v_period = 100,
        'expected default tick_period_ms = 100 (10 ticks/sec), got ' || coalesce(v_period::text, 'NULL');
    raise notice 'PASS: default tick_period_ms is 100 ms (10 ticks/sec)';
end $$;

-- Test 2: pgque.set_tick_period_ms updates the config
do $$
declare
    v_period integer;
begin
    perform pgque.set_tick_period_ms(250);
    select tick_period_ms into v_period from pgque.config;
    assert v_period = 250,
        'expected tick_period_ms = 250 after set_tick_period_ms(250), got ' || v_period;
    perform pgque.set_tick_period_ms(100);
    raise notice 'PASS: set_tick_period_ms updates pgque.config';
end $$;

-- Test 3: out-of-range and NULL values are rejected
do $$
declare
    v_caught boolean;
begin
    v_caught := false;
    begin perform pgque.set_tick_period_ms(0);
    exception when others then v_caught := true;
    end;
    assert v_caught, 'set_tick_period_ms(0) should raise';

    v_caught := false;
    begin perform pgque.set_tick_period_ms(60001);
    exception when others then v_caught := true;
    end;
    assert v_caught, 'set_tick_period_ms(60001) should raise';

    v_caught := false;
    begin perform pgque.set_tick_period_ms(null);
    exception when others then v_caught := true;
    end;
    assert v_caught, 'set_tick_period_ms(NULL) should raise';

    raise notice 'PASS: set_tick_period_ms rejects out-of-range / NULL values';
end $$;

-- Test 4: ticker_loop runs ticker() multiple times per pg_cron slot when
-- tick_period_ms < 1000.  Procedures that call commit() can only be invoked
-- from top-level CALL (not inside a DO block), hence the bookkeeping via a
-- temp table.
select pgque.create_queue('test_tick_period_loop');
select pgque.set_queue_config('test_tick_period_loop', 'ticker_max_lag', '0 seconds');
select pgque.set_queue_config('test_tick_period_loop', 'ticker_idle_period', '0 seconds');
select pgque.set_tick_period_ms(200);

create temp table _tick_period_before as
select count(*)::bigint as v
from pgque.tick t
join pgque.queue q on q.queue_id = t.tick_queue
where q.queue_name = 'test_tick_period_loop';

call pgque.ticker_loop();

do $$
declare
    v_before bigint;
    v_after bigint;
begin
    select v into v_before from _tick_period_before;
    select count(*) into v_after
    from pgque.tick t
    join pgque.queue q on q.queue_id = t.tick_queue
    where q.queue_name = 'test_tick_period_loop';

    assert v_after >= v_before + 2,
        'ticker_loop @ 200 ms should produce >=2 ticks/s; '
        || 'got ticks_before=' || v_before || ' ticks_after=' || v_after;

    raise notice 'PASS: ticker_loop drives % ticks per pg_cron slot (200 ms period)',
        v_after - v_before;
end $$;

drop table _tick_period_before;
select pgque.drop_queue('test_tick_period_loop');
select pgque.set_tick_period_ms(100);

-- Test 5: ticker_loop with tick_period_ms = 1000 ticks exactly once per slot
select pgque.create_queue('test_tick_period_once');
select pgque.set_queue_config('test_tick_period_once', 'ticker_max_lag', '0 seconds');
select pgque.set_queue_config('test_tick_period_once', 'ticker_idle_period', '0 seconds');
select pgque.set_tick_period_ms(1000);

create temp table _tick_period_once_before as
select count(*)::bigint as v
from pgque.tick t
join pgque.queue q on q.queue_id = t.tick_queue
where q.queue_name = 'test_tick_period_once';

call pgque.ticker_loop();

do $$
declare
    v_before bigint;
    v_after bigint;
begin
    select v into v_before from _tick_period_once_before;
    select count(*) into v_after
    from pgque.tick t
    join pgque.queue q on q.queue_id = t.tick_queue
    where q.queue_name = 'test_tick_period_once';

    assert v_after = v_before + 1,
        'ticker_loop @ 1000 ms should tick exactly once, '
        || 'got delta = ' || (v_after - v_before);

    raise notice 'PASS: ticker_loop ticks once per slot at 1000 ms period';
end $$;

drop table _tick_period_once_before;
select pgque.drop_queue('test_tick_period_once');
select pgque.set_tick_period_ms(100);

-- Test 6: pg_cron schedule uses CALL pgque.ticker_loop()
do $$
declare
    v_command text;
begin
    if not exists (select 1 from pg_extension where extname = 'pg_cron') then
        raise notice 'SKIP: pg_cron not installed';
        return;
    end if;

    perform pgque.start();

    select command into v_command from cron.job where jobname = 'pgque_ticker';
    assert v_command ilike '%ticker_loop%',
        'pg_cron pgque_ticker job should call ticker_loop, got: ' || coalesce(v_command, 'NULL');

    perform pgque.stop();
    raise notice 'PASS: pgque.start() schedules CALL pgque.ticker_loop()';
end $$;
