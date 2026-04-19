# logres phases

## Goal

Keep the default install small, understandable, and stable.

The default install should expose only the minimum supported API for v0.1.
Additional features can live in `sql/experimental/` until they prove simple,
useful, and stable enough to promote into the default install.

## Default install (`sql/logres.sql`) — v0.1

This section lists the API categories that ship in the default `\i sql/logres.sql`
install. Function-by-function signatures, grants, and return types live in
`docs/reference.md`.

### Core engine
- repackaged PgQ batching, ticking, rotation, retry queue, consumer tracking

### Lifecycle
- `logres.start()`, `logres.stop()`, `logres.status()`, `logres.version()`
- `logres.maint()`, `logres.ticker()`, `logres.force_tick(queue)`
- `logres.uninstall()` (superuser only)

### Queue management
- `logres.create_queue(queue)`
- `logres.drop_queue(queue)` / `logres.drop_queue(queue, force)`
- `logres.set_queue_config(queue, param, value)` — `param` is the short name
  (`max_retries`, `ticker_max_count`, `ticker_max_lag`, `ticker_idle_period`,
  `ticker_paused`, `rotation_period`, `external_ticker`); the function
  auto-prefixes `queue_` internally

### Modern API
- `logres.send(queue[, type], payload)` — `jsonb` + `text` overloads
- `logres.send_batch(queue, type, payloads)` — `jsonb[]` + `text[]` overloads
- `logres.subscribe(queue, consumer)` / `logres.unsubscribe(queue, consumer)`
- `logres.receive(queue, consumer, max_return)`
- `logres.ack(batch_id)` / `logres.nack(batch_id, msg, retry_after, reason)`

### Dead letter queue
- `logres.dead_letter` table (FKs cascade on queue / consumer removal)
- `logres.event_dead()` — called by `nack()` when `retry_count >= max_retries`
- `logres.dlq_inspect()`, `logres.dlq_replay()`, `logres.dlq_replay_all()`,
  `logres.dlq_purge()`

### Observability
- `logres.get_queue_info()` / `logres.get_queue_info(queue)`
- `logres.get_consumer_info()` / `(queue)` / `(queue, consumer)`
- `logres.get_batch_info(batch_id)`

### PgQ primitives (advanced use)
Available but most users should prefer the modern API above. See
`docs/reference.md` for the full list.
- `insert_event`, `register_consumer`, `unregister_consumer`
- `next_batch`, `next_batch_info`, `next_batch_custom`
- `get_batch_events`, `get_batch_cursor`
- `finish_batch`, `event_retry`, `batch_retry`

### Trigger helpers (change-data-capture)
- `logres.jsontriga()`, `logres.logutriga()`, `logres.sqltriga()`

### Roles
- `logres_reader`, `logres_writer`, `logres_admin` (with inheritance
  `admin > writer > reader`)

## Experimental SQL (`sql/experimental/`)

These files are not installed by default in v0.1.

### `sql/experimental/delayed.sql`
- delayed delivery table
- `logres.send_at()`
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
