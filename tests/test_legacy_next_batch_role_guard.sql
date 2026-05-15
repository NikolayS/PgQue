-- Test: legacy next_batch_custom(5-arg) must not stamp sub_batch on a
-- coop_main row, even if the existing coop_main-with-members rejection
-- check is bypassed (e.g. a memberless coop_main row, which is normally
-- unreachable but possible if a future code path leaves one behind).
-- Defense-in-depth: the UPDATE WHERE clause must include sub_role = 'normal'.
--
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).

\set ON_ERROR_STOP on

do $setup$
begin
    perform pgque.create_queue('legacy_role_guard_q');
    perform pgque.register_consumer('legacy_role_guard_q', 'billing');
    perform pgque.insert_event('legacy_role_guard_q', 'test.role_guard', '{"n":1}');
    perform pgque.force_tick('legacy_role_guard_q');
    perform pgque.ticker('legacy_role_guard_q');
end
$setup$;

-- Manually promote the billing row to coop_main without inserting any
-- coop_member. This state is not produced by current code paths
-- (register_subconsumer always inserts a member; unregister_subconsumer
-- demotes back to normal when removing the last member) but it is what a
-- future regression could leave behind, and the function must not corrupt
-- the row if it sees it.
update pgque.subscription s
   set sub_role = 'coop_main'
  from pgque.queue q, pgque.consumer c
 where q.queue_id = s.sub_queue
   and c.co_id = s.sub_consumer
   and q.queue_name = 'legacy_role_guard_q'
   and c.co_name = 'billing';

do $exercise$
declare
    v_batch_id bigint;
    v_role text;
    v_sub_batch bigint;
begin
    -- Legacy 5-arg next_batch_custom (via the 2-arg next_batch shim).
    -- The coop_main-with-members rejection at line ~5769 does NOT fire here
    -- because no coop_member rows exist for this sub_id. Pre-fix, the
    -- function falls through and the UPDATE stamps sub_batch on the
    -- coop_main row, violating the invariant
    -- "coop_main must never have sub_batch IS NOT NULL".
    v_batch_id := pgque.next_batch('legacy_role_guard_q', 'billing');

    select sub_role, sub_batch
      into v_role, v_sub_batch
      from pgque.subscription s
      join pgque.queue q on q.queue_id = s.sub_queue
      join pgque.consumer c on c.co_id = s.sub_consumer
     where q.queue_name = 'legacy_role_guard_q'
       and c.co_name = 'billing';

    assert v_role = 'coop_main',
        format('precondition: expected sub_role = coop_main, got %s', v_role);

    assert v_sub_batch is null,
        format('invariant violated: coop_main row has sub_batch = %s '
               || '(legacy next_batch_custom must filter UPDATE on sub_role = ''normal'')',
               v_sub_batch);
end
$exercise$;

-- Cleanup
update pgque.subscription s
   set sub_role = 'normal'
  from pgque.queue q, pgque.consumer c
 where q.queue_id = s.sub_queue
   and c.co_id = s.sub_consumer
   and q.queue_name = 'legacy_role_guard_q'
   and c.co_name = 'billing';
select pgque.unregister_consumer('legacy_role_guard_q', 'billing');
select pgque.drop_queue('legacy_role_guard_q', true);

\echo PASS: legacy next_batch_custom rejects writing sub_batch on a coop_main row
