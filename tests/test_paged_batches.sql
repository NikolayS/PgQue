\set ON_ERROR_STOP on

-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
-- Ordinary-consumer durable paging regression (#364).

/* N-1, N and N+1 prove that page_size is a page bound, not an overflow cap. */
do $$
begin
  perform pgque.create_queue('paged_n_minus_1');
  perform pgque.subscribe('paged_n_minus_1', 'c1');
  perform pgque.send('paged_n_minus_1', 'identity', '{"n":1}'::text);

  perform pgque.create_queue('paged_n');
  perform pgque.subscribe('paged_n', 'c1');
  perform pgque.send('paged_n', 'identity', '{"n":1}'::text);
  perform pgque.send('paged_n', 'identity', '{"n":2}'::text);

  perform pgque.create_queue('paged_n_plus_1');
  perform pgque.subscribe('paged_n_plus_1', 'c1');
  perform pgque.send('paged_n_plus_1', 'identity', '{"n":1}'::text);
  perform pgque.send('paged_n_plus_1', 'identity', '{"n":2}'::text);
  perform pgque.send('paged_n_plus_1', 'identity', '{"n":3}'::text);
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_n_minus_1');
  perform pgque.force_next_tick('paged_n');
  perform pgque.force_next_tick('paged_n_plus_1');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page record;
  v_ack record;
begin
  select * into v_page
  from pgque.receive_page(
    'paged_n_minus_1', 'c1', 'worker-n-minus-1', 2, '1 minute'
  );
  assert v_page.status = 'page', 'N-1 must return page';
  assert cardinality(v_page.messages) = 1, 'N-1 must return one message';
  assert v_page.is_last, 'N-1 page must be terminal';
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-n-minus-1');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'terminal N-1 page ack must finish the batch';

  select * into v_page
  from pgque.receive_page('paged_n', 'c1', 'worker-n', 2, '1 minute');
  assert v_page.status = 'page', 'N must return page';
  assert cardinality(v_page.messages) = 2, 'N must return two messages';
  assert v_page.is_last, 'exact N page must be terminal';
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-n');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'terminal N page ack must finish the batch';

  select * into v_page
  from pgque.receive_page(
    'paged_n_plus_1', 'c1', 'worker-n-plus-1', 2, '1 minute'
  );
  assert v_page.status = 'page', 'N+1 must return page, not overflow';
  assert cardinality(v_page.messages) = 2, 'N+1 first page must contain N';
  assert not v_page.is_last, 'N+1 first page must be nonterminal';
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-n-plus-1');
  assert v_ack.status = 'acked' and not v_ack.batch_finished,
    'N+1 first page ack must not finish the batch';

  select * into v_page
  from pgque.receive_page(
    'paged_n_plus_1', 'c1', 'worker-n-plus-1', 2, '1 minute'
  );
  assert cardinality(v_page.messages) = 1, 'N+1 second page must contain one';
  assert v_page.is_last, 'N+1 second page must be terminal';
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-n-plus-1');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'N+1 terminal page ack must finish the batch';
end $$;

/* Invalid page bounds and leases use the public invalid-parameter SQLSTATE. */
do $$
declare
  v_state text;
begin
  begin
    perform * from pgque.receive_page(
      'paged_n', 'c1', 'worker-invalid-size', 0, '1 minute'
    );
    assert false, 'zero page_size must fail';
  exception
    when others then
      get stacked diagnostics v_state = returned_sqlstate;
      assert v_state = '22023', 'zero page_size must raise 22023, got ' || v_state;
  end;

  begin
    perform * from pgque.receive_page(
      'paged_n', 'c1', 'worker-invalid-lease', 1, '0 seconds'
    );
    assert false, 'nonpositive lease must fail';
  exception
    when others then
      get stacked diagnostics v_state = returned_sqlstate;
      assert v_state = '22023', 'zero lease must raise 22023, got ' || v_state;
  end;
end $$;

/*
 * Five messages cover 2 + 2 + 1 pages, stable pending delivery, busy status,
 * complete message identity, bounded ack receipts and the between-page guard.
 */
do $$
begin
  perform pgque.create_queue('paged_identity');
  perform pgque.subscribe('paged_identity', 'c1');
  perform pgque.send('paged_identity', 'type-1', '{"n":1}'::text);
  perform pgque.send('paged_identity', 'type-2', '{"n":2}'::text);
  perform pgque.send('paged_identity', 'type-3', '{"n":3}'::text);
  perform pgque.send('paged_identity', 'type-4', '{"n":4}'::text);
  perform pgque.send('paged_identity', 'type-5', '{"n":5}'::text);
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_identity');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page_1 record;
  v_repeat record;
  v_busy record;
  v_page_2 record;
  v_page_3 record;
  v_ack record;
  v_batch_id bigint;
  v_ids bigint[] := array[]::bigint[];
  v_payloads text[] := array[]::text[];
  v_types text[] := array[]::text[];
  v_msg pgque.message;
  v_state text;
begin
  select * into v_page_1
  from pgque.receive_page(
    'paged_identity', 'c1', 'worker-primary', 2, '1 minute'
  );
  assert v_page_1.status = 'page', 'first identity receive must return page';
  assert v_page_1.page_number = 1, 'first page number must be one';
  assert cardinality(v_page_1.messages) = 2, 'first page must contain two';
  assert not v_page_1.is_last, 'first page must not be terminal';
  v_batch_id := v_page_1.batch_id;

  foreach v_msg in array v_page_1.messages loop
    v_ids := array_append(v_ids, v_msg.msg_id);
    v_payloads := array_append(v_payloads, v_msg.payload);
    v_types := array_append(v_types, v_msg.type);
    assert v_msg.batch_id = v_batch_id, 'message batch identity must match page';
    assert v_msg.retry_count is null and v_msg.created_at is not null,
      'message retry/time identity must be preserved';
    assert v_msg.extra1 is null and v_msg.extra2 is null
      and v_msg.extra3 is null and v_msg.extra4 is null,
      'message extra identity must be preserved';
  end loop;

  select * into v_repeat
  from pgque.receive_page(
    'paged_identity', 'c1', 'worker-primary', 1, '10 minutes'
  );
  assert v_repeat.page_token = v_page_1.page_token,
    'same worker retry must retain pending token';
  assert v_repeat.page_number = v_page_1.page_number,
    'same worker retry must retain page number';
  assert v_repeat.messages = v_page_1.messages,
    'same worker retry must retain exact messages despite page_size change';
  assert v_repeat.is_last = v_page_1.is_last,
    'same worker retry must retain terminal flag';
  assert v_repeat.lease_until < clock_timestamp() + interval '2 minutes',
    'same-worker retry must renew with the immutable original TTL';

  select * into v_busy
  from pgque.receive_page(
    'paged_identity', 'c1', 'worker-other', 2, '1 minute'
  );
  assert v_busy.status = 'busy', 'other live worker must receive busy';
  assert v_busy.page_token is null, 'busy must not disclose page token';
  assert cardinality(v_busy.messages) = 0, 'busy must return no messages';
  assert v_busy.lease_until is not null, 'busy must expose lease deadline';

  begin
    perform pgque.ack(v_batch_id);
    assert false, 'legacy ack must be blocked while a page is outstanding';
  exception
    when others then
      get stacked diagnostics v_state = returned_sqlstate;
      assert v_state = '55000', 'legacy ack guard must raise 55000, got ' || v_state;
  end;

  select * into v_ack
  from pgque.ack_page(v_page_1.page_token, 'worker-primary');
  assert v_ack.status = 'acked' and not v_ack.batch_finished,
    'first page ack must persist without finishing batch';

  select * into v_ack
  from pgque.ack_page(v_page_1.page_token, 'worker-primary');
  assert v_ack.status = 'already_acked' and not v_ack.batch_finished,
    'duplicate first-page ack must replay its receipt';

  begin
    perform pgque.finish_batch(v_batch_id);
    assert false, 'finish_batch must be blocked between pages';
  exception
    when others then
      get stacked diagnostics v_state = returned_sqlstate;
      assert v_state = '55000',
        'between-page finish_batch guard must raise 55000, got ' || v_state;
  end;

  begin
    perform pgque.ack(v_batch_id);
    assert false, 'legacy ack must be blocked between pages';
  exception
    when others then
      get stacked diagnostics v_state = returned_sqlstate;
      assert v_state = '55000',
        'between-page legacy ack guard must raise 55000, got ' || v_state;
  end;

  select * into v_page_2
  from pgque.receive_page(
    'paged_identity', 'c1', 'worker-primary', 2, '1 minute'
  );
  assert v_page_2.page_number = 2, 'second page number must be two';
  assert cardinality(v_page_2.messages) = 2, 'second page must contain two';
  assert not v_page_2.is_last, 'second page must not be terminal';

  select * into v_ack
  from pgque.ack_page(v_page_1.page_token, 'worker-primary');
  assert v_ack.status = 'already_acked' and not v_ack.batch_finished,
    'last receipt must survive issuance of a later page';

  foreach v_msg in array v_page_2.messages loop
    v_ids := array_append(v_ids, v_msg.msg_id);
    v_payloads := array_append(v_payloads, v_msg.payload);
    v_types := array_append(v_types, v_msg.type);
    assert v_msg.batch_id = v_batch_id, 'second-page batch identity must match';
    assert v_msg.retry_count is null and v_msg.created_at is not null,
      'second-page retry/time identity must be preserved';
  end loop;
  select * into v_ack
  from pgque.ack_page(v_page_2.page_token, 'worker-primary');
  assert v_ack.status = 'acked' and not v_ack.batch_finished,
    'second page must not finish five-message batch';

  select * into v_page_3
  from pgque.receive_page(
    'paged_identity', 'c1', 'worker-primary', 2, '1 minute'
  );
  assert v_page_3.page_number = 3, 'third page number must be three';
  assert cardinality(v_page_3.messages) = 1, 'third page must contain one';
  assert v_page_3.is_last, 'third page must be terminal';
  foreach v_msg in array v_page_3.messages loop
    v_ids := array_append(v_ids, v_msg.msg_id);
    v_payloads := array_append(v_payloads, v_msg.payload);
    v_types := array_append(v_types, v_msg.type);
    assert v_msg.batch_id = v_batch_id, 'third-page batch identity must match';
    assert v_msg.retry_count is null and v_msg.created_at is not null,
      'third-page retry/time identity must be preserved';
  end loop;

  assert cardinality(v_ids) = 5, '2 + 2 + 1 traversal must return five IDs';
  assert v_ids[1] < v_ids[2] and v_ids[2] < v_ids[3]
    and v_ids[3] < v_ids[4] and v_ids[4] < v_ids[5],
    'message IDs must be strictly ordered without gaps or repeats';
  assert v_payloads = array[
    '{"n":1}', '{"n":2}', '{"n":3}', '{"n":4}', '{"n":5}'
  ], '2 + 2 + 1 traversal must preserve payload identity';
  assert v_types = array['type-1', 'type-2', 'type-3', 'type-4', 'type-5'],
    '2 + 2 + 1 traversal must preserve type identity';

  select * into v_ack
  from pgque.ack_page(v_page_3.page_token, 'worker-primary');
  assert v_ack.status = 'acked' and v_ack.batch_finished,
    'terminal third page must finish batch';
end $$;

/* Expired claims preserve the page boundary but rotate the ownership token. */
do $$
begin
  perform pgque.create_queue('paged_takeover');
  perform pgque.subscribe('paged_takeover', 'c1');
  perform pgque.send('paged_takeover', 'takeover', '{"n":1}'::text);
  perform pgque.send('paged_takeover', 'takeover', '{"n":2}'::text);
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_takeover');
  perform pgque.ticker();
end $$;

do $$
declare
  v_old record;
  v_new record;
  v_ack record;
  v_state text;
begin
  select * into v_old
  from pgque.receive_page(
    'paged_takeover', 'c1', 'worker-old', 1, '1 millisecond'
  );
  perform pg_sleep(0.02);
  select * into v_new
  from pgque.receive_page(
    'paged_takeover', 'c1', 'worker-new', 99, '1 minute'
  );

  assert v_new.status = 'page', 'successor must take over expired page';
  assert v_new.page_token <> v_old.page_token, 'takeover must rotate page token';
  assert v_new.page_number = v_old.page_number, 'takeover must preserve page number';
  assert v_new.messages = v_old.messages, 'takeover must preserve pending boundary';

  begin
    perform * from pgque.ack_page(v_old.page_token, 'worker-old');
    assert false, 'superseded token must not acknowledge';
  exception
    when others then
      get stacked diagnostics v_state = returned_sqlstate;
      assert v_state = 'PQP01', 'superseded token must raise PQP01, got ' || v_state;
  end;

  select * into v_ack
  from pgque.ack_page(v_new.page_token, 'worker-new');
  assert v_ack.status = 'acked' and not v_ack.batch_finished,
    'taken-over first page must checkpoint but not finish';
end $$;

/* Raw fixtures pin nullable initial keys and duplicate-boundary fail-closed. */
do $$
begin
  perform pgque.create_queue('paged_negative_ids');
  perform pgque.subscribe('paged_negative_ids', 'c1');
  perform pgque.insert_event_raw(
    'paged_negative_ids', -2, clock_timestamp(), null, null,
    'negative', '{"id":-2}', 'e1-a', 'e2-a', 'e3-a', 'e4-a'
  );
  perform pgque.insert_event_raw(
    'paged_negative_ids', -1, clock_timestamp(), null, null,
    'negative', '{"id":-1}', 'e1-b', 'e2-b', 'e3-b', 'e4-b'
  );

  perform pgque.create_queue('paged_duplicate_ids');
  perform pgque.subscribe('paged_duplicate_ids', 'c1');
  perform pgque.insert_event_raw(
    'paged_duplicate_ids', 7, clock_timestamp(), null, null,
    'duplicate-a', '{"copy":1}', null, null, null, null
  );
  perform pgque.insert_event_raw(
    'paged_duplicate_ids', 7, clock_timestamp(), null, null,
    'duplicate-b', '{"copy":2}', null, null, null, null
  );
end $$;

do $$
begin
  perform pgque.force_next_tick('paged_negative_ids');
  perform pgque.force_next_tick('paged_duplicate_ids');
  perform pgque.ticker();
end $$;

do $$
declare
  v_page record;
  v_ack record;
  v_msg_1 pgque.message;
  v_msg_2 pgque.message;
  v_state text;
begin
  select * into v_page
  from pgque.receive_page(
    'paged_negative_ids', 'c1', 'worker-negative', 2, '1 minute'
  );
  assert cardinality(v_page.messages) = 2, 'negative-ID page must contain both rows';
  v_msg_1 := (v_page.messages)[1];
  v_msg_2 := (v_page.messages)[2];
  assert v_msg_1.msg_id = -2 and v_msg_2.msg_id = -1,
    'nullable initial key must not skip negative IDs';
  assert v_msg_1.extra1 = 'e1-a' and v_msg_1.extra4 = 'e4-a'
    and v_msg_2.extra1 = 'e1-b' and v_msg_2.extra4 = 'e4-b',
    'paged messages must preserve all extra identity fields';
  select * into v_ack
  from pgque.ack_page(v_page.page_token, 'worker-negative');
  assert v_ack.batch_finished, 'negative-ID terminal page must finish';

  begin
    perform * from pgque.receive_page(
      'paged_duplicate_ids', 'c1', 'worker-duplicate', 1, '1 minute'
    );
    assert false, 'duplicate IDs at the N+1 boundary must fail closed';
  exception
    when others then
      get stacked diagnostics v_state = returned_sqlstate;
      assert v_state = '21000',
        'ambiguous duplicate IDs must raise 21000, got ' || v_state;
  end;
end $$;

\echo 'PASS: test_paged_batches'
