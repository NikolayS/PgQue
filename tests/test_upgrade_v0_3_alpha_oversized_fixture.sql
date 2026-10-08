\set ON_ERROR_STOP on

-- Build alpha state above the final partition-slot safety ceiling.
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

create table public.upgrade_v03_oversized_state (
  event_id bigint primary key
);

do $$
declare
  v_event_id bigint;
begin
  perform pgque.create_queue('upgrade_v03_oversized_q');
  perform pgque.subscribe_slot(
    'upgrade_v03_oversized_q', 'oversized-workers', 0, 257);
  v_event_id := pgque.send(
    'upgrade_v03_oversized_q',
    'upgrade.oversized',
    '{}'::jsonb,
    'oversized-key');
  insert into public.upgrade_v03_oversized_state values (v_event_id);
end $$;
