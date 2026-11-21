-- Expired claims must be reaped even inside a long-running transaction.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
\set ON_ERROR_STOP on

do $$
declare
  v_queue text;
  v_queue_id integer;
begin
  foreach v_queue in array array['idem_clock_scoped', 'idem_clock_global'] loop
    perform pgque.create_queue(v_queue);
    select queue_id into strict v_queue_id
    from pgque.queue where queue_name = v_queue;
    perform pgque.send_idem(v_queue, 'clock', '{}', 'expired', interval '1 second');
    perform pg_sleep(1.25);

    if v_queue = 'idem_clock_scoped' then
      perform pgque.maint_idem(v_queue);
    else
      perform pgque.maint_idem();
    end if;
    assert not exists (
      select 1 from pgque.idem where queue_id = v_queue_id
    ), format('%s maintenance used transaction-start time', v_queue);
    perform pgque.drop_queue(v_queue);
  end loop;
  raise notice 'PASS: scoped and global maintenance use wall-clock time';
end $$;
