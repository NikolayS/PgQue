# pg_current phases

## Goal

Keep the default install small, understandable, and stable.

The default install should expose only the minimum supported API for v0.1.
Additional features can live in `sql/experimental/` until they prove simple,
useful, and stable enough to promote into the default install.

## Default install (`sql/pg_current.sql`) — v0.1

This section lists the API categories that ship in the default `\i sql/pg_current.sql`
install. Function-by-function signatures, grants, and return types live in
`docs/reference.md`.

### Core engine
- repackaged PgQ batching, ticking, rotation, retry queue, consumer tracking

### Lifecycle
- `pg_current.start()`, `pg_current.stop()`, `pg_current.status()`, `pg_current.version()`
- `pg_current.maint()`, `pg_current.ticker()`, `pg_current.force_tick(queue)`
- `pg_current.uninstall()` (superuser only)

### Queue management
- `pg_current.create_queue(queue)`
- `pg_current.drop_queue(queue)` / `pg_current.drop_queue(queue, force)`
- `pg_current.set_queue_config(queue, param, value)` — `param` is the short name
  (`max_retries`, `ticker_max_count`, `ticker_max_lag`, `ticker_idle_period`,
  `ticker_paused`, `rotation_period`, `external_ticker`); the function
  auto-prefixes `queue_` internally

### Modern API
- `pg_current.send(queue[, type], payload)` — `jsonb` + `text` overloads
- `pg_current.send_batch(queue, type, payloads)` — `jsonb[]` + `text[]` overloads
- `pg_current.subscribe(queue, consumer)` / `pg_current.unsubscribe(queue, consumer)`
- `pg_current.receive(queue, consumer, max_return)`
- `pg_current.ack(batch_id)` / `pg_current.nack(batch_id, msg, retry_after, reason)`

### Dead letter queue
- `pg_current.dead_letter` table (FKs cascade on queue / consumer removal)
- `pg_current.event_dead()` — called by `nack()` when `retry_count >= max_retries`
- `pg_current.dlq_inspect()`, `pg_current.dlq_replay()`, `pg_current.dlq_replay_all()`,
  `pg_current.dlq_purge()`

### Observability
- `pg_current.get_queue_info()` / `pg_current.get_queue_info(queue)`
- `pg_current.get_consumer_info()` / `(queue)` / `(queue, consumer)`
- `pg_current.get_batch_info(batch_id)`

### PgQ primitives (advanced use)
Available but most users should prefer the modern API above. See
`docs/reference.md` for the full list.
- `insert_event`, `register_consumer`, `unregister_consumer`
- `next_batch`, `next_batch_info`, `next_batch_custom`
- `get_batch_events`, `get_batch_cursor`
- `finish_batch`, `event_retry`, `batch_retry`

### Trigger helpers (change-data-capture)
- `pg_current.jsontriga()`, `pg_current.logutriga()`, `pg_current.sqltriga()`

### Roles
- `pg_current_reader`, `pg_current_writer`, `pg_current_admin` (with inheritance
  `admin > writer > reader`)

## Experimental SQL (`sql/experimental/`)

These files are not installed by default in v0.1.

### `sql/experimental/delayed.sql`
- delayed delivery table
- `pg_current.send_at()`
- delayed-delivery maintenance hook

### `sql/experimental/observability.sql`
- queue / consumer stats
- health checks
- OTel export
- throughput / error-rate helpers

## Promotion rule

Experimental SQL can move into the default install only when it is:

1. clearly useful,
2. tested,
3. documented,
4. simple enough that we are unlikely to regret the public API.

## Principle

Default install first. Extras later.
