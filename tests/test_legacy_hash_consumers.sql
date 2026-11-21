\set ON_ERROR_STOP on

/* Legacy names are identified by queue catalogs, not by the # character.
 * Separate setup, publication and tick transactions keep the window visible. */
do $$
begin
  perform pgque.create_queue('legacy_hash_q');
  perform pgque.create_queue('legacy_hash_slot_q');
  perform pgque.register_consumer('legacy_hash_q', 'team#1');
  perform pgque.register_consumer('legacy_hash_q', 'workers#0/1');
  perform pgque.subscribe_partitioned('legacy_hash_slot_q', 'workers', 1);
end $$;

do $$
begin
  begin
    perform pgque.subscribe_slot('legacy_hash_q', 'workers', 0, 1);
    assert false, 'slot setup must not adopt an ordinary consumer';
  exception when raise_exception then
    assert sqlerrm like '%already registered as an ordinary consumer%';
  end;
  assert not exists (
    select 1 from pgque.partition_consumer as pc
    join pgque.queue as q on q.queue_id = pc.queue_id
    where q.queue_name = 'legacy_hash_q'
  ), 'failed slot setup must leave no partition metadata';
end $$;

select pgque.send('legacy_hash_q', 'legacy', g::text)
from generate_series(1, 3) as g;
select pgque.send('legacy_hash_slot_q', 'slot', 'payload'::text);
select pgque.force_next_tick('legacy_hash_q');
select pgque.force_next_tick('legacy_hash_slot_q');
select pgque.ticker();

set role pgque_reader;
do $$
declare
  v_msg pgque.message;
  v_batch bigint;
  v_count int := 0;
begin
  for v_msg in select * from pgque.receive('legacy_hash_q', 'team#1', 3) loop
    v_count := v_count + 1;
    v_batch := v_msg.batch_id;
    assert pgque.nack(v_batch, v_msg, interval '1 hour') = 1;
  end loop;
  assert v_count = 3, 'legacy # consumer must receive every published event';
  assert pgque.ack(v_batch) = 1, 'legacy # consumer must finish its batch';
end $$;

do $$
declare
  v_page pgque.batch_page;
  v_ack record;
  v_ids bigint[] := '{}';
  v_msg pgque.message;
begin
  for i in 1..2 loop
    v_page := pgque.receive_page('legacy_hash_q', 'workers#0/1', 'legacy-worker', 2);
    assert v_page.status = 'page';
    assert cardinality(v_page.messages) = case i when 1 then 2 else 1 end;
    assert v_page.is_last = (i = 2);
    foreach v_msg in array v_page.messages loop
      assert not v_msg.msg_id = any(v_ids), 'legacy pages must not repeat events';
      v_ids := array_append(v_ids, v_msg.msg_id);
    end loop;
    begin
      perform pgque.ack(v_page.batch_id);
      assert false, 'plain ack must not bypass paging for a legacy # name';
    exception when object_not_in_prerequisite_state then null;
    end;
    begin
      perform pgque.nack(v_page.batch_id, v_page.messages[1]);
      assert false, 'plain nack must not bypass paging for a legacy # name';
    exception when object_not_in_prerequisite_state then null;
    end;
    select * into v_ack from pgque.ack_page(v_page.page_token, 'legacy-worker');
    assert v_ack.status = 'acked' and v_ack.batch_finished = (i = 2);
  end loop;
  assert cardinality(v_ids) = 3;
end $$;
reset role;

/* The identical name on another queue is a real slot and stays fenced. */
do $$
declare
  v_batch bigint;
  v_msg pgque.message;
begin
  v_batch := pgque.next_batch('legacy_hash_slot_q', 'workers#0/1');
  select ev_id, v_batch, ev_type, ev_data, ev_retry, ev_time,
    ev_extra1, ev_extra2, ev_extra3, ev_extra4
  into v_msg from pgque.get_batch_events(v_batch) limit 1;
  assert v_msg.msg_id is not null, 'real slot must have a visible event';
  begin
    perform pgque.receive('legacy_hash_slot_q', 'workers#0/1');
    assert false, 'plain receive must reject a cataloged slot';
  exception when raise_exception then
    assert sqlerrm like '%use pgque.receive_partitioned()%';
  end;
  begin
    perform pgque.ack(v_batch);
    assert false, 'plain ack must reject a cataloged slot';
  exception when raise_exception then
    assert sqlerrm like '%use pgque.ack_partitioned()%';
  end;
  begin
    perform pgque.nack(v_batch, v_msg);
    assert false, 'plain nack must reject a cataloged slot';
  exception when raise_exception then
    assert sqlerrm like '%pgque.nack_partitioned()%';
  end;
  begin
    perform pgque.receive_page('legacy_hash_slot_q', 'workers#0/1', 'worker');
    assert false, 'plain paging must reject a cataloged slot';
  exception when invalid_parameter_value then
    assert sqlerrm like '%receive_page_partitioned%';
  end;
  perform pgque.drop_queue('legacy_hash_q', true);
  perform pgque.drop_queue('legacy_hash_slot_q', true);
end $$;
\echo 'PASS: legacy # consumers and cataloged slot fences'
