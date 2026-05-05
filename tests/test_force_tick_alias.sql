-- test_force_tick_alias.sql -- pgque.prime_tick / force_tick equivalence
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- prime_tick is the readability-friendly name for force_tick; the two
-- must behave identically (same return type, same body semantics, same
-- grants) so that callers can switch without changing behavior.

\set ON_ERROR_STOP on

\echo '--- test_force_tick_alias ---'

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
    if to_regprocedure('pgque.prime_tick(text)') is null then
        raise exception 'pgque.prime_tick(text) is missing';
    end if;
    if to_regprocedure('pgque.force_tick(text)') is null then
        raise exception 'pgque.force_tick(text) is missing';
    end if;
end $$;

-- Test 2: both return bigint (proven by usage below)
-- Test 3: prime_tick returns the same value as force_tick (last existing tick id).
-- After create_queue, the last tick id is stable (no new ticks have been inserted)
-- so two consecutive calls must return the same id.
do $$
declare
    v_prime bigint;
    v_force bigint;
begin
    v_prime := pgque.prime_tick('test_alias_q');
    v_force := pgque.force_tick('test_alias_q');
    if v_prime is distinct from v_force then
        raise exception 'prime_tick (%) and force_tick (%) returned different last tick ids',
            v_prime, v_force;
    end if;
end $$;

-- Test 4: prime_tick advances queue_event_seq just like force_tick.
-- Calling prime_tick should bump the seq by ticker_max_count*2 + 1000.
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
    perform pgque.prime_tick('test_alias_q');
    execute format('select last_value from %s', v_seqname) into v_seq_after;

    if v_seq_after <= v_seq_before then
        raise exception 'prime_tick did not advance queue_event_seq (before=%, after=%)',
            v_seq_before, v_seq_after;
    end if;

    -- Sanity check: bump should be at least max_count*2 + 1000 (force_tick semantics).
    if v_seq_after - v_seq_before < (v_max_count * 2 + 1000) then
        raise exception 'prime_tick bump too small: %->% (expected at least %)',
            v_seq_before, v_seq_after, v_max_count * 2 + 1000;
    end if;
end $$;

-- Test 5: grant parity — both functions must be granted to the same roles.
-- The schema-wide "grant execute on all functions … to pgque_admin" covers both,
-- and the schema-wide revoke from PUBLIC keeps PUBLIC out. Verify directly.
do $$
declare
    v_admin_prime boolean;
    v_admin_force boolean;
    v_public_prime boolean;
    v_public_force boolean;
begin
    select has_function_privilege('pgque_admin', 'pgque.prime_tick(text)', 'execute')
      into v_admin_prime;
    select has_function_privilege('pgque_admin', 'pgque.force_tick(text)', 'execute')
      into v_admin_force;
    select has_function_privilege('public',     'pgque.prime_tick(text)', 'execute')
      into v_public_prime;
    select has_function_privilege('public',     'pgque.force_tick(text)', 'execute')
      into v_public_force;

    if v_admin_prime is distinct from v_admin_force then
        raise exception 'admin grant mismatch: prime_tick=% force_tick=%',
            v_admin_prime, v_admin_force;
    end if;
    if v_public_prime is distinct from v_public_force then
        raise exception 'public grant mismatch: prime_tick=% force_tick=%',
            v_public_prime, v_public_force;
    end if;
    if not v_admin_prime then
        raise exception 'pgque_admin should have execute on prime_tick';
    end if;
    if v_public_prime then
        raise exception 'PUBLIC should not have execute on prime_tick';
    end if;
end $$;

-- Test 6: prime_tick + ticker materialises a new tick (the canonical idiom).
do $$
declare
    v_tick_before bigint;
    v_tick_after bigint;
begin
    select last_tick_id into v_tick_before
      from pgque.get_queue_info('test_alias_q');

    perform pgque.prime_tick('test_alias_q');
    perform pgque.ticker();

    select last_tick_id into v_tick_after
      from pgque.get_queue_info('test_alias_q');

    if v_tick_after <= v_tick_before then
        raise exception 'prime_tick + ticker did not advance tick id (before=%, after=%)',
            v_tick_before, v_tick_after;
    end if;
end $$;

-- Test 7: force_tick still works as an alias (verify the legacy idiom).
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

\echo 'PASS: test_force_tick_alias'
