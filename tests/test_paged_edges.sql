-- test_paged_edges.sql -- Edge contracts for durable paged batches
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

\set ON_ERROR_STOP on

/* Invalid/null arguments and int4 maximum page size. */
do $$
declare
  v_state text;
begin
  foreach v_state in array array['queue', 'consumer', 'worker', 'size', 'lease'] loop
    begin
      perform * from pgque.receive_page(
        case when v_state = 'queue' then null else 'missing' end,
        case when v_state = 'consumer' then null else 'c1' end,
        case when v_state = 'worker' then null else 'worker' end,
        case when v_state = 'size' then null else 1 end,
        case when v_state = 'lease' then null else interval '1 second' end
      );
      assert false, 'null ' || v_state || ' must fail';
    exception when sqlstate '22023' then
      null;
    end;
  end loop;
end $$;

do $$
begin
  perform pgque.create_queue('paged_edge_max');
  perform pgque.subscribe('paged_edge_max', 'c1');
  perform pgque.send('paged_edge_max', 'max', 'one');

  perform pgque.create_queue('paged_edge_empty');
  perform pgque.subscribe('paged_edge_empty', 'c1');

  perform pgque.create_queue('paged_edge_ttl');
  perform pgque.subscribe('paged_edge_ttl', 'c1');
  perform pgque.send('paged_edge_ttl', 'ttl', 'one');

  perform pgque.create_queue('paged_edge_receipt');
  perform pgque.subscribe('paged_edge_receipt', 'c1');
  perform pgque.send('paged_edge_receipt', 'receipt', 'first');

  perform pgque.create_queue('paged_edge_rollback');
  perform pgque.subscribe('paged_edge_rollback', 'c1');
  perform pgque.send('paged_edge_rollback', 'rollback', 'one');

  perform pgque.create_queue('paged_edge_reinstall');
  perform pgque.subscribe('paged_edge_reinstall', 'c1');
  perform pgque.send('paged_edge_reinstall', 'reinstall', 'one');
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_edge_max');
  perform pgque.force_next_tick('paged_edge_empty');
  perform pgque.force_next_tick('paged_edge_ttl');
  perform pgque.force_next_tick('paged_edge_receipt');
  perform pgque.force_next_tick('paged_edge_rollback');
  perform pgque.force_next_tick('paged_edge_reinstall');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page pgque.batch_page;
  v_ack record;
begin
  v_page := pgque.receive_page(
    'paged_edge_max', 'c1', 'worker-max', 2147483647, interval '1 minute');
  assert v_page.status = 'page' and cardinality(v_page.messages) = 1
    and v_page.is_last,
    'int4 maximum must remain a valid bounded page size';
  select * into v_ack from pgque.ack_page(v_page.page_token, 'worker-max');
  assert v_ack.batch_finished, 'int4 maximum terminal page must finish';
end $$;

/* One empty tick window advances once; the following poll is idle. */
do $$
declare
  v_page pgque.batch_page;
begin
  v_page := pgque.receive_page(
    'paged_edge_empty', 'c1', 'worker-empty', 1, interval '1 minute');
  assert v_page.status = 'advanced'
    and cardinality(v_page.messages) = 0
    and v_page.page_token is null,
    'empty window must return metadata-only advanced';

  v_page := pgque.receive_page(
    'paged_edge_empty', 'c1', 'worker-empty', 1, interval '1 minute');
  assert v_page.status = 'idle'
    and cardinality(v_page.messages) = 0,
    'poll after one empty advancement must return idle';
end $$;

/* Renewal always uses the immutable TTL selected at first issuance. */
do $$
declare
  v_page pgque.batch_page;
  v_repeat pgque.batch_page;
  v_until timestamptz;
  v_ttl interval;
begin
  v_page := pgque.receive_page(
    'paged_edge_ttl', 'c1', 'worker-ttl', 1, interval '5 seconds');
  v_repeat := pgque.receive_page(
    'paged_edge_ttl', 'c1', 'worker-ttl', 99, interval '1 hour');
  assert v_repeat.page_token = v_page.page_token,
    'same worker retry must preserve its token';

  select pending_lease_ttl into strict v_ttl
  from pgque.page_state
  where pending_token = v_page.page_token;
  assert v_ttl = interval '5 seconds', 'pending lease TTL must be immutable';

  v_until := pgque.renew_page(v_page.page_token, 'worker-ttl');
  assert v_until > clock_timestamp() + interval '4 seconds'
    and v_until < clock_timestamp() + interval '6 seconds',
    'explicit renewal must use the original five-second TTL';
end $$;

/* Reader owns the public page API, not its implementation helpers. */
set role pgque_reader;
do $$
declare
  v_page pgque.batch_page;
  v_denied boolean := false;
begin
  v_page := pgque.receive_page(
    'paged_edge_ttl', 'c1', 'worker-ttl', 1, interval '1 hour');
  assert v_page.status = 'page', 'pgque_reader must execute public paging API';
  begin
    perform pgque._lock_page(v_page.page_token);
  exception when insufficient_privilege then
    v_denied := true;
  end;
  assert v_denied, 'pgque_reader must not execute private page helpers';
end $$;
reset role;

do $$
declare
  v_page pgque.batch_page;
  v_ack record;
begin
  select * into v_ack
  from pgque.ack_page(
    (select pending_token from pgque.page_state
     where pending_worker = 'worker-ttl'),
    'worker-ttl');
  assert v_ack.batch_finished, 'reader-role fixture must remain ackable';
end $$;

/* A retained receipt survives allocation and issuance of the next batch. */
do $$
declare
  v_first pgque.batch_page;
  v_ack record;
begin
  v_first := pgque.receive_page(
    'paged_edge_receipt', 'c1', 'worker-receipt', 1, interval '1 minute');
  select * into v_ack
  from pgque.ack_page(v_first.page_token, 'worker-receipt');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'first receipt fixture must finish';

  perform set_config('pgque.test_receipt_token', v_first.page_token::text, false);
  perform pgque.send('paged_edge_receipt', 'receipt', 'second');
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_edge_receipt');
  perform pgque.ticker('paged_edge_receipt');
end $$;

do $$
declare
  v_next pgque.batch_page;
  v_replay record;
  v_ack record;
begin
  v_next := pgque.receive_page(
    'paged_edge_receipt', 'c1', 'worker-next', 1, interval '1 minute');
  select * into v_replay
  from pgque.ack_page(
    current_setting('pgque.test_receipt_token')::uuid,
    'worker-receipt');
  assert v_replay.status = 'already_acked' and v_replay.batch_finished,
    'old receipt must survive next page issuance';
  select * into v_ack from pgque.ack_page(v_next.page_token, 'worker-next');
  assert v_ack.batch_finished, 'next receipt fixture must finish';
end $$;

/* Duplicate descriptors and an aborted ack leave the issued page untouched. */
do $$
declare
  v_page pgque.batch_page;
  v_repeat pgque.batch_page;
  v_id text;
  v_state text;
begin
  v_page := pgque.receive_page(
    'paged_edge_rollback', 'c1', 'worker-rollback', 1, interval '1 minute');
  v_id := ((v_page.messages)[1]).msg_id::text;
  begin
    perform * from pgque.ack_page(
      v_page.page_token,
      'worker-rollback',
      jsonb_build_array(
        jsonb_build_object('msg_id', v_id),
        jsonb_build_object('msg_id', v_id)));
    assert false, 'duplicate failure descriptors must fail';
  exception when others then
    get stacked diagnostics v_state = returned_sqlstate;
    assert v_state = '22023', 'duplicate descriptors must raise 22023';
  end;

  begin
    perform * from pgque.ack_page(v_page.page_token, 'worker-rollback');
    raise exception 'force rollback after ack';
  exception when others then
    assert sqlerrm = 'force rollback after ack', 'unexpected rollback fixture error';
  end;

  v_repeat := pgque.receive_page(
    'paged_edge_rollback', 'c1', 'worker-rollback', 99, interval '1 hour');
  assert v_repeat.page_token = v_page.page_token
    and v_repeat.messages = v_page.messages,
    'rolled-back ack must retain the exact issued page';
end $$;

/* Keep an outstanding page across an actual idempotent reinstall. */
do $$
declare
  v_page pgque.batch_page;
begin
  v_page := pgque.receive_page(
    'paged_edge_reinstall', 'c1', 'worker-reinstall', 1, interval '1 minute');
  perform set_config('pgque.test_reinstall_token', v_page.page_token::text, false);
end $$;

/* Plain SQL reinstall is not an extension upgrade mechanism. */
select not exists (
    select 1 from pg_extension where extname = 'pgque'
) as page_test_plain_install \gset
\if :page_test_plain_install
\i devel/sql/pgque.sql
\else
\echo 'SKIP: plain SQL reinstall inside pg_tle-owned extension; page continuity still checked'
\endif

do $$
declare
  v_page pgque.batch_page;
  v_ack record;
begin
  v_page := pgque.receive_page(
    'paged_edge_reinstall', 'c1', 'worker-reinstall', 99, interval '1 hour');
  assert v_page.page_token = current_setting('pgque.test_reinstall_token')::uuid,
    'reinstall must preserve the pending token';
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-reinstall');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'page retained through reinstall must remain ackable';
end $$;

do $$
declare
  v_page pgque.batch_page;
  v_ack record;
begin
  v_page := pgque.receive_page(
    'paged_edge_rollback', 'c1', 'worker-rollback', 1, interval '1 minute');
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-rollback');
  assert v_ack.batch_finished, 'rollback fixture cleanup must finish';

  perform pgque.unsubscribe('paged_edge_max', 'c1');
  perform pgque.unsubscribe('paged_edge_empty', 'c1');
  perform pgque.unsubscribe('paged_edge_ttl', 'c1');
  perform pgque.unsubscribe('paged_edge_receipt', 'c1');
  perform pgque.unsubscribe('paged_edge_rollback', 'c1');
  perform pgque.unsubscribe('paged_edge_reinstall', 'c1');
  perform pgque.drop_queue('paged_edge_max');
  perform pgque.drop_queue('paged_edge_empty');
  perform pgque.drop_queue('paged_edge_ttl');
  perform pgque.drop_queue('paged_edge_receipt');
  perform pgque.drop_queue('paged_edge_rollback');
  perform pgque.drop_queue('paged_edge_reinstall');
end $$;

\echo 'PASS: test_paged_edges'
