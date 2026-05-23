\set ON_ERROR_STOP on

-- test_upgrade_v0_1_fixture.sql -- Build representative v0.1.0 state before upgrade
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- Run after installing v0.1.0 and before reinstalling HEAD's sql/pgque.sql.
-- The fixture is idempotent for local reruns in the same database.

do $$
begin
  if to_regnamespace('pgque') is null then
    raise exception 'pgque schema is not installed';
  end if;

  if exists (select 1 from pgque.queue where queue_name = 'upgrade_v01_q') then
    perform pgque.drop_queue('upgrade_v01_q', true);
  end if;
end $$;

-- Queue + consumer survive the upgrade.
do $$
begin
  perform pgque.create_queue('upgrade_v01_q');
  perform pgque.subscribe('upgrade_v01_q', 'upgrade_v01_c');
end $$;

-- Mixed publish styles survive through existing queue tables.
do $$
begin
  perform pgque.send('upgrade_v01_q', '{"kind":"jsonb-default","n":1}'::jsonb);
  perform pgque.send('upgrade_v01_q', 'text.explicit', 'opaque-text-payload');
  perform pgque.send_batch(
    'upgrade_v01_q',
    'batch.mixed',
    array['{"batch":1}'::jsonb, '{"batch":2}'::jsonb]
  );
end $$;

-- Create one retry row and one DLQ row using the v0.1.0 public API.
do $$
begin
  perform pgque.force_tick('upgrade_v01_q');
  perform pgque.ticker();
end $$;

do $$
declare
  v_msg pgque.message;
  v_retry_msg pgque.message;
  v_dlq_msg pgque.message;
  v_seen int := 0;
begin
  for v_msg in select * from pgque.receive('upgrade_v01_q', 'upgrade_v01_c', 10)
  loop
    v_seen := v_seen + 1;
    if v_retry_msg is null then
      v_retry_msg := v_msg;
    elsif v_dlq_msg is null then
      v_dlq_msg := v_msg;
    end if;
  end loop;

  assert v_seen >= 2, 'fixture should receive at least two messages';
  assert v_retry_msg.batch_id is not null, 'fixture batch id should be set';
  assert v_dlq_msg.msg_id is not null, 'fixture DLQ message should be set';

  perform pgque.event_retry(v_retry_msg.batch_id, v_retry_msg.msg_id, 60);
  perform pgque.event_dead(
    v_dlq_msg.batch_id,
    v_dlq_msg.msg_id,
    'upgrade fixture dlq row',
    v_dlq_msg.created_at,
    null::xid8,
    v_dlq_msg.retry_count,
    v_dlq_msg.type,
    v_dlq_msg.payload,
    v_dlq_msg.extra1,
    v_dlq_msg.extra2,
    v_dlq_msg.extra3,
    v_dlq_msg.extra4
  );

  perform pgque.ack(v_retry_msg.batch_id);
end $$;

-- Sanity checks before upgrade.
do $$
begin
  assert exists (select 1 from pgque.queue where queue_name = 'upgrade_v01_q'),
    'fixture queue should exist before upgrade';
  assert exists (select 1 from pgque.consumer where co_name = 'upgrade_v01_c'),
    'fixture consumer should exist before upgrade';
  assert exists (select 1 from pgque.retry_queue),
    'fixture retry row should exist before upgrade';
  assert exists (select 1 from pgque.dead_letter),
    'fixture DLQ row should exist before upgrade';

  raise notice 'PASS: v0.1.0 upgrade fixture prepared';
end $$;
