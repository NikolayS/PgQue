\set ON_ERROR_STOP on

-- Test #100: hardening for set_queue_config / external ticker / force_tick.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

-- =========================================================================
-- Part 1: pgque.set_queue_config rejects nonsensical values
-- =========================================================================

do $$ begin
  perform pgque.create_queue('th_q1');
end $$;

-- Helper: assert that set_queue_config(param, value) raises with a message
-- matching `like_pattern`. Uses a per-call subtransaction so the surrounding
-- DO block can keep running.
do $$
declare
  v_cases text[][] := array[
    array['max_retries',        '-1', '%max_retries%'],
    array['ticker_max_count',   '0',  '%ticker_max_count%'],
    array['ticker_max_count',   '-5', '%ticker_max_count%'],
    array['ticker_max_lag',     '-1 second', '%ticker_max_lag%'],
    array['ticker_max_lag',     '0',  '%ticker_max_lag%'],
    array['ticker_idle_period', '-1 second', '%ticker_idle_period%'],
    array['ticker_idle_period', '0',  '%ticker_idle_period%'],
    array['rotation_period',    '-1 hour', '%rotation_period%'],
    array['rotation_period',    '0',  '%rotation_period%']
  ];
  i int;
  v_param text;
  v_val   text;
  v_pat   text;
  v_raised boolean;
begin
  for i in 1 .. array_length(v_cases, 1) loop
    v_param := v_cases[i][1];
    v_val   := v_cases[i][2];
    v_pat   := v_cases[i][3];
    v_raised := false;
    begin
      perform pgque.set_queue_config('th_q1', v_param, v_val);
    exception when others then
      v_raised := true;
      assert sqlerrm like v_pat,
        format('set_queue_config(%L, %L) raised wrong message: %s (expected match %L)',
               v_param, v_val, sqlerrm, v_pat);
    end;
    assert v_raised,
      format('set_queue_config(%L, %L) should have raised but did not', v_param, v_val);
  end loop;

  -- Sanity: a valid value still works.
  perform pgque.set_queue_config('th_q1', 'max_retries', '3');
  perform pgque.set_queue_config('th_q1', 'ticker_max_count', '500');
  perform pgque.set_queue_config('th_q1', 'ticker_max_lag', '4 seconds');
  perform pgque.set_queue_config('th_q1', 'ticker_idle_period', '60 seconds');
  perform pgque.set_queue_config('th_q1', 'rotation_period', '1 hour');

  -- NULL is a happy path: it bypasses range validation and resets columns to
  -- their schema defaults, preserving pre-hardening DEFAULT semantics.
  perform pgque.set_queue_config('th_q1', 'ticker_max_lag', null);
  perform pgque.set_queue_config('th_q1', 'rotation_period', null);

  assert (select queue_ticker_max_lag = interval '3 seconds'
            from pgque.queue where queue_name = 'th_q1'),
    'ticker_max_lag NULL should reset to default 3 seconds';
  assert (select queue_rotation_period = interval '2 hours'
            from pgque.queue where queue_name = 'th_q1'),
    'rotation_period NULL should reset to default 2 hours';

  raise notice 'PASS Part 1: set_queue_config rejects nonsensical values and accepts NULL defaults';
end $$;

-- =========================================================================
-- Part 2: pgque.ticker(queue, tick_id, ts, event_seq) external monotonicity
-- =========================================================================

do $$ begin
  perform pgque.create_queue('th_q2_ext');
  perform pgque.set_queue_config('th_q2_ext', 'external_ticker', 'true');
end $$;

do $$
declare
  v_first  bigint;
  v_second bigint;
  v_raised boolean;
begin
  -- First external tick: id=10, event_seq=100 — fresh queue, accepted.
  -- (queue starts at tick_id=1 from create_queue, so 10 is monotonic.)
  v_first := pgque.ticker('th_q2_ext', 10::bigint, now(), 100::bigint);
  assert v_first = 10, format('first external tick should return 10, got %s', v_first);

  -- Strictly higher tick_id and event_seq — accepted.
  v_second := pgque.ticker('th_q2_ext', 11::bigint, now(), 110::bigint);
  assert v_second = 11, format('second external tick should return 11, got %s', v_second);

  -- Reject: same tick_id (11) — non-monotonic.
  v_raised := false;
  begin
    perform pgque.ticker('th_q2_ext', 11::bigint, now(), 120::bigint);
  exception when others then
    v_raised := true;
    assert sqlerrm like '%tick_id%',
      format('expected tick_id monotonicity error, got: %s', sqlerrm);
  end;
  assert v_raised, 'duplicate tick_id should have raised';

  -- Reject: lower tick_id (5) — strictly past.
  v_raised := false;
  begin
    perform pgque.ticker('th_q2_ext', 5::bigint, now(), 130::bigint);
  exception when others then
    v_raised := true;
    assert sqlerrm like '%tick_id%',
      format('expected tick_id monotonicity error, got: %s', sqlerrm);
  end;
  assert v_raised, 'older tick_id should have raised';

  -- Reject: higher tick_id but lower event_seq (109 < 110).
  v_raised := false;
  begin
    perform pgque.ticker('th_q2_ext', 12::bigint, now(), 109::bigint);
  exception when others then
    v_raised := true;
    assert sqlerrm like '%event_seq%',
      format('expected event_seq monotonicity error, got: %s', sqlerrm);
  end;
  assert v_raised, 'lower event_seq should have raised';

  -- Accept: equal event_seq is allowed (no new events between ticks).
  v_first := pgque.ticker('th_q2_ext', 12::bigint, now(), 110::bigint);
  assert v_first = 12, 'equal event_seq with strictly higher tick_id should be accepted';

  raise notice 'PASS Part 2: external ticker enforces tick_id and event_seq monotonicity';
end $$;

-- =========================================================================
-- Part 3: pgque.force_tick raises on missing/paused/external queue
-- =========================================================================

do $$ begin
  perform pgque.create_queue('th_q3_paused');
  perform pgque.set_queue_config('th_q3_paused', 'ticker_paused', 'true');

  perform pgque.create_queue('th_q3_ext');
  perform pgque.set_queue_config('th_q3_ext', 'external_ticker', 'true');
end $$;

do $$
declare
  v_raised boolean;
begin
  -- Missing queue: explicit error, not silent NULL.
  v_raised := false;
  begin
    perform pgque.force_tick('th_q3_does_not_exist');
  exception when others then
    v_raised := true;
    assert sqlerrm like '%th_q3_does_not_exist%' or sqlerrm like '%not found%',
      format('expected queue-not-found error, got: %s', sqlerrm);
  end;
  assert v_raised, 'force_tick on missing queue should have raised';

  -- Paused queue: explicit error.
  v_raised := false;
  begin
    perform pgque.force_tick('th_q3_paused');
  exception when others then
    v_raised := true;
    assert sqlerrm like '%paused%' or sqlerrm like '%th_q3_paused%',
      format('expected paused error, got: %s', sqlerrm);
  end;
  assert v_raised, 'force_tick on paused queue should have raised';

  -- External-ticker queue: force_tick is a no-op for those; explicit error.
  v_raised := false;
  begin
    perform pgque.force_tick('th_q3_ext');
  exception when others then
    v_raised := true;
    assert sqlerrm like '%external%' or sqlerrm like '%th_q3_ext%',
      format('expected external-ticker error, got: %s', sqlerrm);
  end;
  assert v_raised, 'force_tick on external-ticker queue should have raised';

  raise notice 'PASS Part 3: force_tick raises on missing/paused/external queue';
end $$;

-- =========================================================================
-- Cleanup
-- =========================================================================
do $$ begin
  perform pgque.drop_queue('th_q1');
  perform pgque.drop_queue('th_q2_ext');
  perform pgque.drop_queue('th_q3_paused');
  perform pgque.drop_queue('th_q3_ext');
  raise notice 'PASS: config + external ticker + force_tick hardening (#100)';
end $$;
