\set ON_ERROR_STOP on

-- Test pgque.send_idem() -- producer idempotency (Phase 1B, US-13.x)
-- Requires devel/sql/pgque.sql + devel/sql/pgque-api/send_idem.sql.
-- dblink is used for the cross-session race test (extensions are allowed
-- in tests/; the managed-PG-compat rule applies only to the default install).

create extension if not exists dblink;

/* This temp table carries event ids across the transaction boundaries used
   by the expiry tests. */
create temporary table idem_state (k text primary key, v bigint);

-- Setup
do $$
begin
  perform pgque.create_queue('test_idem');
  perform pgque.subscribe('test_idem', 'c1');
end $$;

-- US-13.1: TTL dedup -- fresh send inserts and returns deduped=false
do $$
declare
  v_eid bigint;
  v_dedup boolean;
  v_table text;
  v_count int;
begin
  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"tenant":"t1","v":1}', 'migrate:t1:v1',
    '1 hour') s;

  assert v_eid is not null, 'fresh send_idem should return an event id';
  assert v_dedup = false, 'fresh send_idem should return deduped=false';
  insert into idem_state values ('first_eid', v_eid);

  /* The event must actually be in the queue, with the idem key riding in
     ev_extra2 (partition key rides ev_extra1; see spec section 7). */
  select pgque.quote_fqname(queue_data_pfx || '_' || queue_cur_table::text)
  into v_table
  from pgque.queue
  where queue_name = 'test_idem';

  execute format('select count(*) from %s where ev_extra2 = $1', v_table)
  into v_count
  using 'migrate:t1:v1';
  assert v_count = 1,
    'fresh send_idem should insert exactly 1 event, got ' || v_count;

  raise notice 'PASS: US-13.1 fresh send inserts, deduped=false';
end $$;

/* A claim's TTL starts when send_idem runs, not when its surrounding
   transaction began. The committed claim must therefore still be live even
   when the transaction was already older than the requested TTL. */
do $$
declare
  v_eid bigint;
  v_dedup boolean;
  v_expires_at timestamptz;
begin
  perform pg_sleep(1.25);

  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"late":1}', 'late-txn:k1',
    '1 second') s;
  select expires_at into v_expires_at
  from pgque.idem as k
  join pgque.queue as q using (queue_id)
  where q.queue_name = 'test_idem'
    and k.idem_key = 'late-txn:k1';

  assert not v_dedup, 'first late-transaction send must insert';
  assert v_expires_at > clock_timestamp(),
    format('new claim must expire after send time, got expires_at=%s now=%s',
      v_expires_at, clock_timestamp());
  insert into idem_state values ('late_txn_eid', v_eid);
end $$;

do $$
declare
  v_eid bigint;
  v_dedup boolean;
  v_first bigint;
begin
  select v into v_first from idem_state where k = 'late_txn_eid';
  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"late":1}', 'late-txn:k1',
    '100 milliseconds') s;

  assert v_dedup and v_eid = v_first,
    'immediate post-commit retry must deduplicate a late-transaction send';
  raise notice 'PASS: idempotency TTL starts at send time, not transaction start';
end $$;

-- Infinite TTLs create claims that maintenance can never reclaim (PG19+).
do $$
declare
  v_raised boolean := false;
begin
  if current_setting('server_version_num')::int >= 190000 then
    begin
      execute $q$
        select * from pgque.send_idem(
          'test_idem', 'migrate', '{}', 'infinite:k1', 'infinity'::interval)
      $q$;
    exception
      when others then
        v_raised := true;
        assert sqlerrm = 'ttl must be a positive finite interval',
          format('unexpected infinite-TTL error: %s', sqlerrm);
    end;
    assert v_raised, 'send_idem must reject an infinite TTL';
  end if;
end $$;

-- US-13.1: duplicate within window returns the ORIGINAL event_id,
-- deduped=true, and inserts nothing
do $$
declare
  v_eid bigint;
  v_dedup boolean;
  v_first bigint;
  v_table text;
  v_count int;
begin
  select v into v_first from idem_state where k = 'first_eid';

  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"tenant":"t1","v":1}', 'migrate:t1:v1',
    '1 hour') s;

  assert v_dedup = true, 'duplicate within window should return deduped=true';
  assert v_eid = v_first,
    format('duplicate should return original event id %s, got %s',
           v_first, v_eid);

  select pgque.quote_fqname(queue_data_pfx || '_' || queue_cur_table::text)
  into v_table
  from pgque.queue
  where queue_name = 'test_idem';

  execute format('select count(*) from %s where ev_extra2 = $1', v_table)
  into v_count
  using 'migrate:t1:v1';
  assert v_count = 1,
    'duplicate send_idem must not insert an event, count=' || v_count;

  raise notice 'PASS: US-13.1 duplicate returns original id, deduped=true, no insert';
end $$;

-- US-13.2: effect-scoped keys -- a DIFFERENT key is not suppressed.
-- 'migrate:t1:v2' (new effect for the same tenant) must insert even while
-- 'migrate:t1:v1' is live. This is exactly why keys must encode the effect.
do $$
declare
  v_eid bigint;
  v_dedup boolean;
  v_first bigint;
begin
  select v into v_first from idem_state where k = 'first_eid';

  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"tenant":"t1","v":2}', 'migrate:t1:v2',
    '1 hour') s;

  assert v_dedup = false,
    'different idem_key must not be suppressed by a live sibling key';
  assert v_eid is not null and v_eid <> v_first,
    'different idem_key should produce a new event';

  raise notice 'PASS: US-13.2 different (effect-scoped) key not suppressed';
end $$;

-- US-13.1/US-12 composition: i_partition_key passes through to ev_extra1
do $$
declare
  v_eid bigint;
  v_dedup boolean;
  v_table text;
  v_extra1 text;
  v_extra2 text;
begin
  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"tenant":"t9"}', 'migrate:t9:v1',
    '1 hour', 'tenant-9') s;
  assert v_dedup = false, 'fresh keyed send should insert';

  select pgque.quote_fqname(queue_data_pfx || '_' || queue_cur_table::text)
  into v_table
  from pgque.queue
  where queue_name = 'test_idem';

  execute format('select ev_extra1, ev_extra2 from %s where ev_id = $1', v_table)
  into v_extra1, v_extra2
  using v_eid;
  assert v_extra1 = 'tenant-9',
    'partition key should ride ev_extra1, got ' || coalesce(v_extra1, 'NULL');
  assert v_extra2 = 'migrate:t9:v1',
    'idem key should ride ev_extra2, got ' || coalesce(v_extra2, 'NULL');

  raise notice 'PASS: partition_key -> ev_extra1, idem_key -> ev_extra2';
end $$;

-- jsonb overload: explicit ::jsonb cast picks the jsonb variant (send.sql
-- overload-resolution convention) and behaves identically
do $$
declare
  v_eid bigint;
  v_dedup boolean;
begin
  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"jsonb":true}'::jsonb, 'migrate:jsonb:v1',
    '1 hour') s;
  assert v_dedup = false and v_eid is not null,
    'jsonb overload fresh send should insert';

  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"jsonb":true}'::jsonb, 'migrate:jsonb:v1',
    '1 hour') s;
  assert v_dedup = true, 'jsonb overload duplicate should dedup';

  raise notice 'PASS: jsonb overload dedups like the text overload';
end $$;

-- send_idem rejects a NULL idem key (silent non-dedup would be a footgun)
do $$
begin
  perform 1
  from pgque.send_idem('test_idem', 'migrate', '{}', null::text, '1 hour');
  raise exception 'send_idem(null idem_key) should fail';
exception when others then
  assert sqlerrm = 'idem_key must not be null',
    'unexpected null idem_key error: ' || sqlerrm;
end $$;

-- Roles: send_idem is a producer surface (pgque_writer); the pgque.idem
-- claim table is internal -- no app role may touch it directly
do $$
declare
  v_eid bigint;
  v_dedup boolean;
begin
  assert has_function_privilege(
    'pgque_writer',
    'pgque.send_idem(text, text, text, text, interval, text)',
    'execute'
  ), 'pgque_writer must be able to execute send_idem(text payload)';
  assert has_function_privilege(
    'pgque_writer',
    'pgque.send_idem(text, text, jsonb, text, interval, text)',
    'execute'
  ), 'pgque_writer must be able to execute send_idem(jsonb payload)';
  assert not has_table_privilege('pgque_reader', 'pgque.idem', 'select'),
    'pgque_reader must not read pgque.idem directly';
  assert not has_table_privilege('pgque_writer', 'pgque.idem', 'select'),
    'pgque_writer must not read pgque.idem directly';
  assert not has_table_privilege('pgque_writer', 'pgque.idem', 'insert'),
    'pgque_writer must not write pgque.idem directly';

  set role pgque_writer;
  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"role":"writer"}', 'migrate:writer:v1',
    '1 hour') s;
  reset role;
  assert v_dedup = false and v_eid is not null,
    'pgque_writer send_idem should insert through the SECURITY DEFINER wrapper';

  raise notice 'PASS: role surface (writer executes, idem table locked down)';
exception when others then
  reset role;
  raise;
end $$;

-- I3 atomicity: if the event insert fails after the claim wins, the whole
-- transaction rolls back -- the claim must not survive and suppress a job
-- that was never enqueued
do $$
declare
  v_eid bigint;
  v_dedup boolean;
begin
  update pgque.queue
  set queue_disable_insert = true
  where queue_name = 'test_idem';

  begin
    perform 1
    from pgque.send_idem('test_idem', 'migrate', '{}', 'atomic:k1', '1 hour');
    raise exception 'send_idem on disabled queue should fail';
  exception when others then
    assert sqlerrm = 'Insert into queue disallowed',
      'unexpected disabled queue error: ' || sqlerrm;
  end;

  update pgque.queue
  set queue_disable_insert = false
  where queue_name = 'test_idem';

  assert not exists (
    select 1
    from pgque.idem k
    join pgque.queue q using (queue_id)
    where q.queue_name = 'test_idem'
      and k.idem_key = 'atomic:k1'
  ), 'failed insert must roll the claim back (no permanently-suppressed job)';

  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem('test_idem', 'migrate', '{}', 'atomic:k1', '1 hour') s;
  assert v_dedup = false and v_eid is not null,
    'the key must be usable again after the rolled-back attempt';

  raise notice 'PASS: I3 atomic claim+append (failed insert releases the claim)';
exception when others then
  update pgque.queue
  set queue_disable_insert = false
  where queue_name = 'test_idem';
  raise;
end $$;

-- US-13.3: window expiry -- after the TTL passes the key inserts anew.
-- Split across statements: each do-block is its own transaction, so now()
-- advances across the pg_sleep between them.
do $$
declare
  v_eid bigint;
  v_dedup boolean;
begin
  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"exp":1}', 'exp:k1', '100 milliseconds') s;
  assert v_dedup = false, 'fresh short-ttl send should insert';
  insert into idem_state values ('exp_eid', v_eid);
end $$;

select pg_sleep(0.25);

do $$
declare
  v_eid bigint;
  v_dedup boolean;
  v_old bigint;
begin
  select v into v_old from idem_state where k = 'exp_eid';

  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"exp":2}', 'exp:k1', '100 milliseconds') s;

  assert v_dedup = false, 'expired key should be reusable (deduped=false)';
  assert v_eid is not null and v_eid <> v_old,
    'expired key should insert a NEW event';

  raise notice 'PASS: US-13.3 key reusable after TTL expiry';
end $$;

-- US-13.4: GC -- pgque.maint_idem() purges expired claim rows, live rows stay
select pg_sleep(0.25);

do $$
declare
  v_expired int;
  v_live int;
begin
  select count(*) into v_expired
  from pgque.idem
  where expires_at < now();
  assert v_expired > 0, 'setup: expected expired idem rows before GC';

  perform pgque.maint_idem();

  select count(*) into v_expired
  from pgque.idem
  where expires_at < now();
  assert v_expired = 0,
    'maint_idem() should purge all expired rows, left ' || v_expired;

  select count(*) into v_live
  from pgque.idem k
  join pgque.queue q using (queue_id)
  where q.queue_name = 'test_idem'
    and k.idem_key = 'migrate:t1:v1';
  assert v_live = 1, 'maint_idem() must not touch live (unexpired) rows';

  raise notice 'PASS: US-13.4 maint_idem() purges expired rows only';
end $$;

-- US-13.4: wiring -- send_idem self-registers pgque.maint_idem in the
-- queue's queue_extra_maint, so the stock pgque.maint() runner picks it up
-- with no edit to maint.sql
do $$
declare
  v_eid bigint;
  v_dedup boolean;
begin
  assert exists (
    select 1 from pgque.queue
    where queue_name = 'test_idem'
      and queue_extra_maint @> array['pgque.maint_idem']
  ), 'send_idem should register pgque.maint_idem in queue_extra_maint';

  select s.event_id, s.deduped
  into v_eid, v_dedup
  from pgque.send_idem(
    'test_idem', 'migrate', '{"gc":2}', 'gc:k2', '100 milliseconds') s;
  assert v_dedup = false, 'fresh gc:k2 send should insert';
end $$;

select pg_sleep(0.25);

do $$
begin
  perform pgque.maint();

  assert not exists (
    select 1
    from pgque.idem k
    join pgque.queue q using (queue_id)
    where q.queue_name = 'test_idem'
      and k.idem_key = 'gc:k2'
  ), 'pgque.maint() should reap expired idem rows via the extra-maint hook';

  raise notice 'PASS: US-13.4 pgque.maint() picks up maint_idem via queue_extra_maint';
end $$;

-- I2: concurrent duplicate race -- two sessions, same key, at most one insert
-- Connect back into this very server: derive socket dir + port from the
-- running instance so the test works on non-default clusters too.
select dblink_connect('idem_a', format('host=%s port=%s dbname=%s user=%s',
  split_part(current_setting('unix_socket_directories'), ',', 1),
  current_setting('port'), current_database(), current_user));
select dblink_connect('idem_b', format('host=%s port=%s dbname=%s user=%s',
  split_part(current_setting('unix_socket_directories'), ',', 1),
  current_setting('port'), current_database(), current_user));
select dblink_send_query('idem_a',
  $q$select event_id, deduped from pgque.send_idem('test_idem', 'migrate', '{"race":1}', 'race:k1', '1 hour')$q$);
select dblink_send_query('idem_b',
  $q$select event_id, deduped from pgque.send_idem('test_idem', 'migrate', '{"race":1}', 'race:k1', '1 hour')$q$);

do $$
declare
  r record;
  v_inserts int := 0;
  v_ids bigint[] := '{}';
  v_table text;
  v_count int;
begin
  for r in
    select * from dblink_get_result('idem_a') as t(event_id bigint, deduped boolean)
  loop
    if not r.deduped then v_inserts := v_inserts + 1; end if;
    v_ids := v_ids || r.event_id;
  end loop;
  for r in
    select * from dblink_get_result('idem_b') as t(event_id bigint, deduped boolean)
  loop
    if not r.deduped then v_inserts := v_inserts + 1; end if;
    v_ids := v_ids || r.event_id;
  end loop;

  assert v_inserts = 1,
    'exactly one of two racing send_idem calls must insert, got ' || v_inserts;
  assert cardinality(v_ids) = 2 and v_ids[1] = v_ids[2],
    'both racers must resolve to the same original event id: ' || v_ids::text;

  select pgque.quote_fqname(queue_data_pfx || '_' || queue_cur_table::text)
  into v_table
  from pgque.queue
  where queue_name = 'test_idem';

  execute format('select count(*) from %s where ev_extra2 = $1', v_table)
  into v_count
  using 'race:k1';
  assert v_count = 1,
    'concurrent duplicates must produce exactly 1 event, got ' || v_count;

  raise notice 'PASS: I2 concurrent duplicate race -> exactly one insert';
end $$;

select dblink_disconnect('idem_a');
select dblink_disconnect('idem_b');

-- Cleanup: dropping the queue cascades its idem claim rows away
do $$
declare
  v_queue_id int4;
begin
  select queue_id into v_queue_id
  from pgque.queue
  where queue_name = 'test_idem';

  perform pgque.unsubscribe('test_idem', 'c1');
  perform pgque.drop_queue('test_idem');

  assert not exists (select 1 from pgque.idem where queue_id = v_queue_id),
    'drop_queue should cascade-delete the idem rows of the queue';

  raise notice 'PASS: cleanup complete (idem rows cascade with drop_queue)';
end $$;
