\set ON_ERROR_STOP on

-- Build v0.2.0 state that exercises the legacy consumer-name namespace.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

do $$
begin
  if exists (
    select 1
    from pgque.queue
    where queue_name = 'upgrade_v02_hash_q'
  ) then
    perform pgque.drop_queue('upgrade_v02_hash_q', true);
  end if;

  perform pgque.create_queue('upgrade_v02_hash_q');
  perform pgque.set_queue_config('upgrade_v02_hash_q', 'max_retries', '0');
  perform pgque.subscribe('upgrade_v02_hash_q', 'team#1');
  perform pgque.subscribe('upgrade_v02_hash_q', 'workers#0/2');
  perform pgque.send(
    'upgrade_v02_hash_q',
    'upgrade.pending',
    '{"phase":"before-upgrade"}'::jsonb
  );
end $$;

do $$
begin
  perform pgque.force_next_tick('upgrade_v02_hash_q');
  perform pgque.ticker();
end $$;

do $$
begin
  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'upgrade_v02_hash_q'
      and c.co_name = 'team#1'
  ), 'legacy # consumer subscription should exist before upgrade';

  assert exists (
    select 1
    from pgque.subscription as s
    join pgque.queue as q on q.queue_id = s.sub_queue
    join pgque.consumer as c on c.co_id = s.sub_consumer
    where q.queue_name = 'upgrade_v02_hash_q'
      and c.co_name = 'workers#0/2'
  ), 'legacy consumer colliding with a generated slot name should exist before upgrade';

  raise notice 'PASS: v0.2.0 legacy # consumer fixture prepared';
end $$;
