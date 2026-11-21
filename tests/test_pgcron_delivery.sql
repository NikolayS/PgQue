-- Real pg_cron delivery: no direct ticker() call is permitted in this test.
\set ON_ERROR_STOP on
select pgque.create_queue('cron_delivery');
select pgque.register_consumer('cron_delivery', 'cron_delivery_consumer');
select pgque.send('cron_delivery', '{"source":"pg_cron"}'::jsonb);
select pgque.force_tick('cron_delivery');
create temporary table cron_delivery_before as
select max(t.tick_id) as tick_id
from pgque.tick as t
inner join pgque.queue as q on q.queue_id = t.tick_queue
where q.queue_name = 'cron_delivery';
select pgque.start();
do $$
declare
  v_deadline timestamptz := clock_timestamp() + interval '30 seconds';
  v_message pgque.message;
  v_count integer := 0;
begin
  loop
    exit when exists (
      select
      from pgque.tick as t
      inner join pgque.queue as q on q.queue_id = t.tick_queue
      where q.queue_name = 'cron_delivery'
        and t.tick_id > (select tick_id from cron_delivery_before)
    );
    assert clock_timestamp() < v_deadline, 'pg_cron did not advance the queue';
    perform pg_sleep(0.1);
  end loop;
  for v_message in select * from pgque.receive('cron_delivery', 'cron_delivery_consumer', 10) loop
    v_count := v_count + 1;
    assert v_message.payload::jsonb = '{"source":"pg_cron"}'::jsonb, 'payload changed';
  end loop;
  assert v_count = 1, 'expected exactly one scheduled delivery';
  perform pgque.ack(v_message.batch_id);
  assert not exists (select from pgque.receive('cron_delivery', 'cron_delivery_consumer', 10)),
    'acknowledged message redelivered';
end $$;
select pgque.stop();
select pgque.unregister_consumer('cron_delivery', 'cron_delivery_consumer');
select pgque.drop_queue('cron_delivery');
drop table cron_delivery_before;
