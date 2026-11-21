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
    if clock_timestamp() >= v_deadline then
      raise exception 'pg_cron did not advance the queue within 30 seconds';
    end if;
    perform pg_sleep(0.1);
  end loop;
  for v_message in select * from pgque.receive('cron_delivery', 'cron_delivery_consumer', 10) loop
    v_count := v_count + 1;
    if v_message.payload::jsonb <> '{"source":"pg_cron"}'::jsonb then
      raise exception 'payload changed: %', v_message.payload;
    end if;
  end loop;
  if v_count <> 1 then
    raise exception 'expected exactly one scheduled delivery, got %', v_count;
  end if;
  perform pgque.ack(v_message.batch_id);
  if exists (select from pgque.receive('cron_delivery', 'cron_delivery_consumer', 10)) then
    raise exception 'acknowledged message redelivered';
  end if;
end $$;
select pgque.stop();
-- Wait for any in-flight ticker_loop() worker started before stop() to finish
-- so drop_queue() cannot deadlock with it or leave background work for later steps.
do $$
declare
  v_deadline timestamptz := clock_timestamp() + interval '15 seconds';
begin
  loop
    perform pg_catalog.pg_stat_clear_snapshot();
    exit when not exists (
      select 1
      from pg_catalog.pg_stat_activity
      where datname = current_database()
        and pid <> pg_backend_pid()
        and state <> 'idle'
        and query ilike '%ticker_loop%'
    );
    if clock_timestamp() >= v_deadline then
      raise exception 'in-flight pg_cron ticker_loop() did not exit after pgque.stop()';
    end if;
    perform pg_sleep(0.1);
  end loop;
end $$;
select pgque.unregister_consumer('cron_delivery', 'cron_delivery_consumer');
select pgque.drop_queue('cron_delivery');
drop table cron_delivery_before;
