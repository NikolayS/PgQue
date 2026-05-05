-- pgque tick helpers
-- Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
--
-- pgque.prime_tick(queue) is the readability-friendly name for the
-- inherited PgQ helper pgque.force_tick(queue). They share one body
-- (this function delegates to pgque.force_tick) so behavior is identical
-- and there is no drift risk.
--
-- Why "prime"? The function bumps the queue's event sequence past the
-- ticker_max_count threshold so the next pgque.ticker() call sees plenty
-- of "new" events and skips the throttle. It does NOT insert a tick by
-- itself. The canonical idiom is the pair:
--
--     select pgque.prime_tick('q');  -- prime the seq
--     select pgque.ticker();          -- materialise the tick
--
-- The historical name pgque.force_tick is misleading: it suggests the
-- function inserts a tick row directly, which it does not. force_tick
-- stays as a permanent alias for backward compatibility (it is the
-- upstream PgQ name, in use since the Skype/Marko Kreen era ~2007).

create or replace function pgque.prime_tick(i_queue_name text)
returns bigint as $$
-- ----------------------------------------------------------------------
-- Function: pgque.prime_tick(1)
--
--      Prime the queue's event sequence so the NEXT pgque.ticker() call
--      will insert a tick without waiting for ticker_max_count or
--      ticker_max_lag thresholds.
--
--      Bumps queue_event_seq by ticker_max_count * 2 + 1000 to simulate
--      a burst of events. Does NOT insert a tick itself — callers must
--      invoke pgque.ticker() (or pgque.ticker(queue)) afterwards.
--
-- Parameters:
--      i_queue_name     - Name of the queue
--
-- Returns:
--      Currently last tick id (the most recent EXISTING tick on the
--      queue, not a newly created one).
-- ----------------------------------------------------------------------
begin
    return pgque.force_tick(i_queue_name);
end;
$$ language plpgsql security definer set search_path = pgque, pg_catalog;

-- prime_tick is admin-only (matches force_tick). The schema-wide
-- "grant execute on all functions … to pgque_admin" earlier in the
-- install handles the grant; the schema-wide revoke from PUBLIC at
-- the bottom of the install handles the lockdown. Nothing extra to
-- emit here.
