\set ON_ERROR_STOP on

-- NULL is not a lease owner. Exercise public APIs as pgque_reader.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Keep send, ticker, and receive in separate transactions for visibility.

create or replace function pg_temp.expect_partition_fenced(
  i_operation text, i_worker text, i_msg pgque.message default null)
returns void as $$
declare
  v_raised boolean := false;
begin
  begin
    case i_operation
      when 'receive' then
        perform * from pgque.receive_partitioned(
          'pk_null_owner', 'workers', 0, 1, i_worker, 10);
      when 'ack' then
        perform pgque.ack_partitioned('pk_null_owner', 'workers', 0, 1, i_worker);
      when 'nack' then
        perform pgque.nack_partitioned(
          'pk_null_owner', 'workers', 0, 1, i_worker, i_msg, interval '0 seconds');
      else
        raise exception 'unknown test operation: %', i_operation;
    end case;
  exception when others then
    v_raised := true;
    assert sqlstate = 'P0001' and sqlerrm like '%; fenced',
      format('%s: expected lease fence, got [%s] %s', i_operation, sqlstate, sqlerrm);
  end;
  assert v_raised,
    format('%s accepted invalid worker %s', i_operation, coalesce(quote_literal(i_worker), 'NULL'));
end;
$$ language plpgsql;

do $$
begin
  perform pgque.create_queue('pk_null_owner');
  perform pgque.subscribe_slot('pk_null_owner', 'workers', 0, 1);
  perform pgque.claim_slot('pk_null_owner', 'workers', 0, 'owner', interval '1 minute');
end $$;

-- At a batch boundary, NULL must not clear another worker's live lease.
set role pgque_reader;
do $$
begin
  assert pgque.release_slot('pk_null_owner', 'workers', 0, null) is false,
    'NULL worker released a live lease at a batch boundary';
  assert pgque.release_slot('pk_null_owner', 'workers', 0, '') is false,
    'empty worker released a live lease at a batch boundary';
end $$;
reset role;

select pgque.send('pk_null_owner', 'event', 'payload', 'key');
select pgque.force_next_tick('pk_null_owner');
select pgque.ticker();

-- Invalid receive must not create a batch or renew the lease.
create temp table pk_null_owner_before as
select ps.* from pgque.partition_slot ps
join pgque.queue q on q.queue_id = ps.queue_id
where q.queue_name = 'pk_null_owner';

set role pgque_reader;
select pg_temp.expect_partition_fenced('receive', null);
select pg_temp.expect_partition_fenced('receive', '');
reset role;

do $$
begin
  assert not exists (
    select 1 from pgque.subscription s
    join pgque.queue q on q.queue_id = s.sub_queue
    where q.queue_name = 'pk_null_owner' and s.sub_batch is not null
  ), 'invalid receive opened a batch';
  assert exists (
    select 1 from pgque.partition_slot ps
    join pk_null_owner_before b using (queue_id, co_name, slot)
    where ps.lease_owner = b.lease_owner and ps.lease_until = b.lease_until
      and ps.lease_ttl = b.lease_ttl and ps.epoch = b.epoch
  ), 'invalid release/receive changed the lease';
end $$;

-- The real owner opens a batch. Every guarded API must reject NULL even
-- with a valid message and batch, rather than failing for an unrelated cause.
create temp table pk_null_owner_message as
select * from pgque.receive_partitioned('pk_null_owner', 'workers', 0, 1, 'owner', 10);
grant select on pk_null_owner_message to pgque_reader;

set role pgque_reader;
do $$
declare
  v_msg pgque.message;
  v_worker text;
begin
  select * into strict v_msg from pk_null_owner_message;
  foreach v_worker in array array[null, '']::text[] loop
    assert pgque.release_slot('pk_null_owner', 'workers', 0, v_worker) is false,
      'invalid worker release must return false even with an open batch';
    perform pg_temp.expect_partition_fenced('receive', v_worker, v_msg);
    perform pg_temp.expect_partition_fenced('nack', v_worker, v_msg);
    perform pg_temp.expect_partition_fenced('ack', v_worker, v_msg);
  end loop;

  assert pgque.ack_partitioned('pk_null_owner', 'workers', 0, 1, 'owner') = 1,
    'invalid worker must leave the batch open for its owner';
  assert pgque.release_slot('pk_null_owner', 'workers', 0, 'owner') is true,
    'owner must still release after ack';
  assert pgque.release_slot('pk_null_owner', 'workers', 0, null) is false,
    'NULL must not match an unowned slot';
end $$;
reset role;

do $$
begin
  assert not exists (
    select 1 from pgque.retry_queue r
    join pk_null_owner_message m on m.msg_id = r.ev_id
    join pgque.subscription s on s.sub_id = r.ev_owner
    join pgque.queue q on q.queue_id = s.sub_queue
    where q.queue_name = 'pk_null_owner'
  ), 'invalid nack scheduled a retry';
  perform pgque.drop_queue('pk_null_owner', true);
  raise notice 'PASS: NULL/empty workers cannot release, receive, ack, or nack another worker''s slot';
end $$;

drop table pk_null_owner_before, pk_null_owner_message;
drop function pg_temp.expect_partition_fenced(text, text, pgque.message);
