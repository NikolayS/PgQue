-- test_force_next_tick_alias.sql -- pgque.force_next_tick / force_tick equivalence
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- force_next_tick is the clearer name for force_tick; the two must behave
-- identically (same return type, same body semantics, same grants) so callers
-- can switch without changing behavior.

\set ON_ERROR_STOP on

\echo '--- test_force_next_tick_alias ---'

-- Clean slate
do $$
begin
    if exists (select 1 from pgque.queue where queue_name = 'test_alias_q') then
        perform pgque.drop_queue('test_alias_q');
    end if;
end $$;

select pgque.create_queue('test_alias_q');

-- Test 1: both functions exist with the same signature
do $$
begin
    if to_regprocedure('pgque.force_next_tick(text)') is null then
        raise exception 'pgque.force_next_tick(text) is missing';
    end if;
    if to_regprocedure('pgque.force_tick(text)') is null then
        raise exception 'pgque.force_tick(text) is missing';
    end if;
end $$;

-- Test 2: force_next_tick returns the same value as force_tick (last existing tick id).
-- After create_queue, the last tick id is stable (no new ticks have been inserted),
-- so two consecutive calls must return the same id.
do $$
declare
    v_next bigint;
    v_legacy bigint;
begin
    v_next := pgque.force_next_tick('test_alias_q');
    v_legacy := pgque.force_tick('test_alias_q');
    if v_next is distinct from v_legacy then
        raise exception 'force_next_tick (%) and force_tick (%) returned different last tick ids',
            v_next, v_legacy;
    end if;
end $$;

-- Test 3: force_next_tick advances queue_event_seq just like force_tick.
-- Calling force_next_tick should bump the seq by ticker_max_count*2 + 1000.
do $$
declare
    v_seq_before bigint;
    v_seq_after bigint;
    v_max_count int;
    v_seqname text;
begin
    select queue_event_seq, queue_ticker_max_count
      into v_seqname, v_max_count
      from pgque.queue where queue_name = 'test_alias_q';

    execute format('select last_value from %s', v_seqname) into v_seq_before;
    perform pgque.force_next_tick('test_alias_q');
    execute format('select last_value from %s', v_seqname) into v_seq_after;

    if v_seq_after <= v_seq_before then
        raise exception 'force_next_tick did not advance queue_event_seq (before=%, after=%)',
            v_seq_before, v_seq_after;
    end if;

    -- Sanity check: bump should be at least max_count*2 + 1000 (force_tick semantics).
    if v_seq_after - v_seq_before < (v_max_count * 2 + 1000) then
        raise exception 'force_next_tick bump too small: %->% (expected at least %)',
            v_seq_before, v_seq_after, v_max_count * 2 + 1000;
    end if;
end $$;

-- Test 4: grant parity — both functions must be granted to the same roles.
-- The schema-wide "grant execute on all functions … to pgque_admin" covers both,
-- and the schema-wide revoke from PUBLIC keeps PUBLIC out. Verify directly.
do $$
declare
    v_admin_next boolean;
    v_admin_legacy boolean;
    v_public_next boolean;
    v_public_legacy boolean;
begin
    select has_function_privilege('pgque_admin', 'pgque.force_next_tick(text)', 'execute')
      into v_admin_next;
    select has_function_privilege('pgque_admin', 'pgque.force_tick(text)', 'execute')
      into v_admin_legacy;
    select has_function_privilege('public',     'pgque.force_next_tick(text)', 'execute')
      into v_public_next;
    select has_function_privilege('public',     'pgque.force_tick(text)', 'execute')
      into v_public_legacy;

    if v_admin_next is distinct from v_admin_legacy then
        raise exception 'admin grant mismatch: force_next_tick=% force_tick=%',
            v_admin_next, v_admin_legacy;
    end if;
    if v_public_next is distinct from v_public_legacy then
        raise exception 'public grant mismatch: force_next_tick=% force_tick=%',
            v_public_next, v_public_legacy;
    end if;
    if not v_admin_next then
        raise exception 'pgque_admin should have execute on force_next_tick';
    end if;
    if v_public_next then
        raise exception 'PUBLIC should not have execute on force_next_tick';
    end if;
end $$;

-- Test 5: force_next_tick + ticker materialises a new tick (the canonical idiom).
do $$
declare
    v_tick_before bigint;
    v_tick_after bigint;
begin
    select last_tick_id into v_tick_before
      from pgque.get_queue_info('test_alias_q');

    perform pgque.force_next_tick('test_alias_q');
    perform pgque.ticker();

    select last_tick_id into v_tick_after
      from pgque.get_queue_info('test_alias_q');

    if v_tick_after <= v_tick_before then
        raise exception 'force_next_tick + ticker did not advance tick id (before=%, after=%)',
            v_tick_before, v_tick_after;
    end if;
end $$;

-- Test 6: force_tick still works as an alias (verify the legacy idiom).
do $$
declare
    v_tick_before bigint;
    v_tick_after bigint;
begin
    select last_tick_id into v_tick_before
      from pgque.get_queue_info('test_alias_q');

    perform pgque.force_tick('test_alias_q');
    perform pgque.ticker();

    select last_tick_id into v_tick_after
      from pgque.get_queue_info('test_alias_q');

    if v_tick_after <= v_tick_before then
        raise exception 'force_tick + ticker did not advance tick id (before=%, after=%)',
            v_tick_before, v_tick_after;
    end if;
end $$;

-- Cleanup
select pgque.drop_queue('test_alias_q');

\echo 'PASS: test_force_next_tick_alias'
