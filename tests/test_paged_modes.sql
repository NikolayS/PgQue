\set ON_ERROR_STOP on

/*
 * Regression contracts for durable cooperative/partition pages and atomic
 * failure descriptors.  Lease expiry is forced through trusted fixture DML;
 * wall-clock sleeps would make takeover coverage flaky without adding value.
 */

/* Cooperative takeover preserves progress and each member's ack receipt. */
do $$
begin
  perform pgque.create_queue('paged_coop_takeover');
  perform pgque.register_subconsumer('paged_coop_takeover', 'main_c', 'w1');
  perform pgque.register_subconsumer('paged_coop_takeover', 'main_c', 'w2');
  perform pgque.send('paged_coop_takeover', 'coop', '{"n":1}'::text);
  perform pgque.send('paged_coop_takeover', 'coop', '{"n":2}'::text);
  perform pgque.send('paged_coop_takeover', 'coop', '{"n":3}'::text);
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_coop_takeover');
  perform pgque.ticker();
end $$;

do $$
declare
  v_first record;
  v_pending record;
  v_taken record;
  v_ack record;
  v_state text;
  v_w1_consumer_id int4;
  v_w2_consumer_id int4;
begin
  select * into v_first
  from pgque.receive_page_coop(
    'paged_coop_takeover', 'main_c', 'w1', 'worker-w1',
    1, interval '1 second', interval '1 minute'
  );
  assert v_first.status = 'page' and v_first.page_number = 1,
    'w1 must receive cooperative page one';
  assert ((v_first.messages)[1]).payload = '{"n":1}',
    'cooperative page one payload mismatch';

  select * into v_ack
  from pgque.ack_page(v_first.page_token, 'worker-w1');
  assert v_ack.status = 'acked' and not v_ack.batch_finished,
    'cooperative page one must checkpoint without finishing';

  select * into v_pending
  from pgque.receive_page_coop(
    'paged_coop_takeover', 'main_c', 'w1', 'worker-w1',
    1, interval '1 second', interval '1 minute'
  );
  assert v_pending.page_number = 2
    and ((v_pending.messages)[1]).payload = '{"n":2}',
    'w1 must receive cooperative page two after its checkpoint';

  begin
    perform * from pgque.receive_coop(
      'paged_coop_takeover', 'main_c', 'w1', 100, interval '1 second'
    );
    assert false, 'legacy cooperative receive must reject an active page';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    assert v_state = '55000',
      'legacy cooperative receive must raise 55000, got ' || v_state;
  end;

  select c.co_id into v_w1_consumer_id
  from pgque.consumer as c where c.co_name = 'main_c.w1';
  select c.co_id into v_w2_consumer_id
  from pgque.consumer as c where c.co_name = 'main_c.w2';

  update pgque.subscription as s
  set sub_active = clock_timestamp() - interval '2 minutes'
  from pgque.queue as q
  where q.queue_name = 'paged_coop_takeover'
    and s.sub_queue = q.queue_id
    and s.sub_consumer = v_w1_consumer_id;
  update pgque.page_state as ps
  set pending_lease_until = clock_timestamp() - interval '1 second'
  from pgque.queue as q
  where q.queue_name = 'paged_coop_takeover'
    and ps.queue_id = q.queue_id
    and ps.consumer_id = v_w1_consumer_id;

  select * into v_taken
  from pgque.receive_page_coop(
    'paged_coop_takeover', 'main_c', 'w2', 'worker-w2',
    99, interval '1 second', interval '1 minute'
  );
  assert v_taken.status = 'page', 'w2 must take over expired coop page';
  assert v_taken.batch_id <> v_pending.batch_id,
    'cooperative transfer must mint a fresh batch token';
  assert v_taken.page_token <> v_pending.page_token,
    'cooperative transfer must invalidate the victim page token';
  assert v_taken.page_number = 2,
    'cooperative transfer must preserve the committed page number';
  assert ((v_taken.messages)[1]).msg_id = ((v_pending.messages)[1]).msg_id
    and ((v_taken.messages)[1]).type = ((v_pending.messages)[1]).type
    and ((v_taken.messages)[1]).payload = ((v_pending.messages)[1]).payload
    and ((v_taken.messages)[1]).retry_count is not distinct from
      ((v_pending.messages)[1]).retry_count
    and ((v_taken.messages)[1]).created_at = ((v_pending.messages)[1]).created_at
    and ((v_taken.messages)[1]).extra1 is not distinct from
      ((v_pending.messages)[1]).extra1
    and ((v_taken.messages)[1]).extra2 is not distinct from
      ((v_pending.messages)[1]).extra2
    and ((v_taken.messages)[1]).extra3 is not distinct from
      ((v_pending.messages)[1]).extra3
    and ((v_taken.messages)[1]).extra4 is not distinct from
      ((v_pending.messages)[1]).extra4,
    'cooperative transfer must preserve the canonical pending event';
  assert ((v_taken.messages)[1]).batch_id = v_taken.batch_id,
    'transferred message must expose the successor batch token';

  begin
    perform * from pgque.ack_page(v_pending.page_token, 'worker-w1');
    assert false, 'transferred pending token must be stale';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    assert v_state = 'PQP01',
      'transferred pending token must raise PQP01, got ' || v_state;
  end;

  /* Receipt lookup belongs to w1 and must survive active-state transfer. */
  select * into v_ack
  from pgque.ack_page(v_first.page_token, 'worker-w1');
  assert v_ack.status = 'already_acked' and not v_ack.batch_finished,
    'victim receipt must survive cooperative transfer';
  assert exists (
    select 1
    from pgque.page_state as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'paged_coop_takeover'
      and ps.consumer_id = v_w1_consumer_id
      and ps.last_ack_token = v_first.page_token
  ), 'cooperative victim must retain its receipt';
  assert not exists (
    select 1
    from pgque.page_state as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'paged_coop_takeover'
      and ps.consumer_id = v_w2_consumer_id
      and ps.last_ack_token = v_first.page_token
  ), 'cooperative transfer must not copy the victim receipt to destination';

  begin
    perform pgque.ack(v_taken.batch_id);
    assert false, 'legacy ack must reject a cooperative paged batch';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    assert v_state = '55000',
      'cooperative legacy ack must raise 55000, got ' || v_state;
  end;

  select * into v_ack
  from pgque.ack_page(v_taken.page_token, 'worker-w2');
  assert v_ack.status = 'acked' and not v_ack.batch_finished,
    'taken-over cooperative page two must checkpoint';

  /* The active guard remains between pages, not only while a token exists. */
  begin
    perform pgque.ack(v_taken.batch_id);
    assert false, 'legacy ack must reject cooperative state between pages';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    assert v_state = '55000',
      'between-page cooperative legacy ack must raise 55000, got ' || v_state;
  end;

  select * into v_taken
  from pgque.receive_page_coop(
    'paged_coop_takeover', 'main_c', 'w2', 'worker-w2',
    1, interval '1 second', interval '1 minute'
  );
  assert v_taken.page_number = 3 and v_taken.is_last
    and ((v_taken.messages)[1]).payload = '{"n":3}',
    'cooperative successor must resume at terminal page three';
  select * into v_ack
  from pgque.ack_page(v_taken.page_token, 'worker-w2');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'cooperative terminal page must finish transferred batch';

  assert exists (
    select 1
    from pgque.page_state as ps
    join pgque.queue as q on q.queue_id = ps.queue_id
    where q.queue_name = 'paged_coop_takeover'
      and ps.consumer_id = v_w1_consumer_id
      and ps.last_ack_token = v_first.page_token
  ), 'destination acks must not overwrite the victim receipt';
end $$;

/* Partition receipts survive epoch changes; pending pages are epoch fenced. */
do $$
begin
  perform pgque.create_queue('paged_partition_epoch');
  perform pgque.subscribe_slot('paged_partition_epoch', 'part_c', 0, 1);
  perform pgque.send(
    'paged_partition_epoch', 'part', '{"n":1}'::text, 'tenant-a'
  );
  perform pgque.send(
    'paged_partition_epoch', 'part', '{"n":2}'::text, 'tenant-b'
  );
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_partition_epoch');
  perform pgque.ticker();
end $$;

do $$
declare
  v_epoch_1 bigint;
  v_epoch_2 bigint;
  v_epoch_3 bigint;
  v_first record;
  v_second record;
  v_aba record;
  v_ack record;
  v_state text;
begin
  v_epoch_1 := pgque.claim_slot(
    'paged_partition_epoch', 'part_c', 0, 'worker-a', interval '1 minute'
  );
  select * into v_first
  from pgque.receive_page_partitioned(
    'paged_partition_epoch', 'part_c', 0, 1, 'worker-a', 1
  );
  assert v_first.status = 'page' and v_first.fence_epoch = v_epoch_1,
    'first partition page must expose the claimed epoch';
  select * into v_ack
  from pgque.ack_page(v_first.page_token, 'worker-a');
  assert v_ack.status = 'acked' and not v_ack.batch_finished,
    'first partition page must checkpoint';

  update pgque.partition_slot as ps
  set lease_until = clock_timestamp() - interval '1 second'
  from pgque.queue as q
  where q.queue_name = 'paged_partition_epoch'
    and ps.queue_id = q.queue_id
    and ps.co_name = 'part_c'
    and ps.slot = 0;
  v_epoch_2 := pgque.claim_slot(
    'paged_partition_epoch', 'part_c', 0, 'worker-b', interval '1 minute'
  );
  assert v_epoch_2 > v_epoch_1, 'partition takeover must advance epoch';

  /* A committed receipt is proof of the old commit even under a new owner. */
  select * into v_ack
  from pgque.ack_page(v_first.page_token, 'worker-a');
  assert v_ack.status = 'already_acked' and not v_ack.batch_finished,
    'partition receipt replay must precede current epoch rejection';

  select * into v_second
  from pgque.receive_page_partitioned(
    'paged_partition_epoch', 'part_c', 0, 1, 'worker-b', 1
  );
  assert v_second.page_number = 2 and v_second.fence_epoch = v_epoch_2,
    'new partition owner must resume at page two with its epoch';

  begin
    perform pgque.ack_partitioned(
      'paged_partition_epoch', 'part_c', 0, 1, 'worker-b'
    );
    assert false, 'legacy partition ack must reject an active page';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    assert v_state = '55000',
      'legacy partition ack must raise 55000, got ' || v_state;
  end;

  update pgque.partition_slot as ps
  set lease_until = clock_timestamp() - interval '1 second'
  from pgque.queue as q
  where q.queue_name = 'paged_partition_epoch'
    and ps.queue_id = q.queue_id
    and ps.co_name = 'part_c'
    and ps.slot = 0;
  v_epoch_3 := pgque.claim_slot(
    'paged_partition_epoch', 'part_c', 0, 'worker-a', interval '1 minute'
  );
  assert v_epoch_3 > v_epoch_2,
    'same worker name returning after another owner must get a new epoch';

  select * into v_aba
  from pgque.receive_page_partitioned(
    'paged_partition_epoch', 'part_c', 0, 1, 'worker-a', 99
  );
  assert v_aba.page_token <> v_second.page_token,
    'partition epoch ABA must replace the pending token';
  assert v_aba.fence_epoch = v_epoch_3 and v_aba.page_number = 2,
    'partition epoch ABA must preserve progress and expose the new epoch';
  assert v_aba.messages = v_second.messages,
    'partition epoch ABA must preserve the pending page boundary';

  begin
    perform * from pgque.ack_page(v_second.page_token, 'worker-b');
    assert false, 'old partition epoch token must be fenced';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    assert v_state = 'PQP01',
      'old partition epoch token must raise PQP01, got ' || v_state;
  end;

  select * into v_ack
  from pgque.ack_page(v_aba.page_token, 'worker-a');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'ABA successor must finish the terminal partition page';
  select * into v_ack
  from pgque.ack_page(v_aba.page_token, 'worker-a');
  assert v_ack.status = 'already_acked' and v_ack.batch_finished,
    'terminal partition receipt must replay its original finished value';
end $$;

/* Failure descriptors route atomically and receipts make retries harmless. */
do $$
begin
  perform pgque.create_queue('paged_fail_retry');
  perform pgque.subscribe('paged_fail_retry', 'c1');
  perform pgque.send('paged_fail_retry', 'retry-me', '{"route":"retry"}'::text);

  perform pgque.create_queue('paged_fail_dlq');
  perform pgque.set_queue_config('paged_fail_dlq', 'max_retries', '0');
  perform pgque.subscribe('paged_fail_dlq', 'c1');
  perform pgque.send('paged_fail_dlq', 'dead-me', '{"route":"dlq"}'::text);
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_fail_retry');
  perform pgque.force_next_tick('paged_fail_dlq');
  perform pgque.ticker();
end $$;

create or replace function pgque._test_fail_page_retry()
returns trigger as $$
begin
  raise exception 'forced page retry insert failure';
end;
$$ language plpgsql;

create trigger test_fail_page_retry
before insert on pgque.retry_queue
for each row execute function pgque._test_fail_page_retry();

do $$
declare
  v_page record;
  v_repeat record;
  v_ack record;
  v_failure jsonb;
  v_state text;
begin
  select * into v_page
  from pgque.receive_page(
    'paged_fail_retry', 'c1', 'worker-retry', 1, interval '1 minute'
  );
  v_failure := jsonb_build_array(jsonb_build_object(
    'msg_id', (((v_page.messages)[1]).msg_id)::text,
    'retry_after_seconds', 0,
    'reason', 'transient'
  ));

  begin
    perform * from pgque.ack_page(
      v_page.page_token, 'worker-retry', v_failure
    );
    assert false, 'retry routing failure must abort the entire page ack';
  exception when others then
    assert sqlerrm = 'forced page retry insert failure',
      'unexpected retry-routing failure: ' || sqlerrm;
  end;
  assert not exists (
    select 1
    from pgque.retry_queue as rq
    join pgque.queue as q on q.queue_id = rq.ev_queue
    where q.queue_name = 'paged_fail_retry'
      and rq.ev_id = ((v_page.messages)[1]).msg_id
  ), 'failed ack must not leave retry routing behind';

  select * into v_repeat
  from pgque.receive_page(
    'paged_fail_retry', 'c1', 'worker-retry', 99, interval '10 minutes'
  );
  assert v_repeat.page_token = v_page.page_token
    and v_repeat.messages = v_page.messages,
    'failed ack must leave the identical pending page';

  drop trigger test_fail_page_retry on pgque.retry_queue;
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-retry', v_failure);
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'successful failure routing must atomically finish terminal page';
  assert (
    select count(*)
    from pgque.retry_queue as rq
    join pgque.queue as q on q.queue_id = rq.ev_queue
    where q.queue_name = 'paged_fail_retry'
      and rq.ev_id = ((v_page.messages)[1]).msg_id
  ) = 1, 'failure descriptor must create exactly one retry row';

  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-retry', v_failure);
  assert v_ack.status = 'already_acked' and v_ack.batch_finished,
    'same failure descriptor must replay its ack receipt';
  assert (
    select count(*)
    from pgque.retry_queue as rq
    join pgque.queue as q on q.queue_id = rq.ev_queue
    where q.queue_name = 'paged_fail_retry'
      and rq.ev_id = ((v_page.messages)[1]).msg_id
  ) = 1, 'receipt replay must not route the failed event twice';

  begin
    perform * from pgque.ack_page(
      v_page.page_token,
      'worker-retry',
      jsonb_build_array(jsonb_build_object(
        'msg_id', (((v_page.messages)[1]).msg_id)::text,
        'retry_after_seconds', 1,
        'reason', 'changed'
      ))
    );
    assert false, 'same token with different failures must be rejected';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    assert v_state = 'PQP02',
      'changed failure replay must raise PQP02, got ' || v_state;
  end;
end $$;

drop function pgque._test_fail_page_retry();

do $$
declare
  v_page record;
  v_ack record;
  v_failure jsonb;
  v_dl pgque.dead_letter%rowtype;
begin
  select * into v_page
  from pgque.receive_page(
    'paged_fail_dlq', 'c1', 'worker-dlq', 1, interval '1 minute'
  );
  v_failure := jsonb_build_array(jsonb_build_object(
    'msg_id', (((v_page.messages)[1]).msg_id)::text,
    'retry_after_seconds', 0,
    'reason', 'terminal reason'
  ));
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-dlq', v_failure);
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'DLQ failure descriptor must finish atomically';

  select dl.* into strict v_dl
  from pgque.dead_letter as dl
  join pgque.queue as q on q.queue_id = dl.dl_queue_id
  where q.queue_name = 'paged_fail_dlq';
  assert v_dl.ev_id = ((v_page.messages)[1]).msg_id
    and v_dl.ev_type = 'dead-me'
    and v_dl.ev_data = '{"route":"dlq"}'
    and v_dl.dl_reason = 'terminal reason',
    'DLQ route must use canonical event data and descriptor reason';

  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-dlq', v_failure);
  assert v_ack.status = 'already_acked' and v_ack.batch_finished,
    'DLQ failure replay must return the retained receipt';
  assert (
    select count(*)
    from pgque.dead_letter as dl
    join pgque.queue as q on q.queue_id = dl.dl_queue_id
    where q.queue_name = 'paged_fail_dlq'
  ) = 1, 'DLQ receipt replay must not duplicate routing';
end $$;

\echo 'PASS: test_paged_modes'
