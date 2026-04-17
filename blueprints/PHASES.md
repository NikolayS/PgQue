# PgQue phases

## Goal

Keep the default install small, understandable, and stable.

The default install should expose only the minimum supported API for v0.1.
Additional features can live in `sql/experimental/` until they prove simple,
useful, and stable enough to promote into the default install.

## Default install (`sql/pgque.sql`) — v0.1

This section lists the API categories that ship in the default `\i sql/pgque.sql`
install. Function-by-function signatures, grants, and return types live in
`docs/reference.md`.

### Core engine
- repackaged PgQ batching, ticking, rotation, retry queue, consumer tracking

### Lifecycle
- `pgque.start()`, `pgque.stop()`, `pgque.status()`, `pgque.version()`
- `pgque.maint()`, `pgque.ticker()`, `pgque.force_tick(queue)`
- `pgque.uninstall()` (superuser only)

### Queue management
- `pgque.create_queue(queue)`
- `pgque.drop_queue(queue)` / `pgque.drop_queue(queue, force)`
- `pgque.set_queue_config(queue, param, value)` — `param` is the short name
  (`max_retries`, `ticker_max_count`, `ticker_max_lag`, `ticker_idle_period`,
  `ticker_paused`, `rotation_period`, `external_ticker`); the function
  auto-prefixes `queue_` internally

### Modern API
- `pgque.send(queue[, type], payload)` — `jsonb` + `text` overloads
- `pgque.send_batch(queue, type, payloads)` — `jsonb[]` + `text[]` overloads
- `pgque.subscribe(queue, consumer)` / `pgque.unsubscribe(queue, consumer)`
- `pgque.receive(queue, consumer, max_return)`
- `pgque.ack(batch_id)` / `pgque.nack(batch_id, msg, retry_after, reason)`

### Dead letter queue
- `pgque.dead_letter` table (FKs cascade on queue / consumer removal)
- `pgque.event_dead()` — called by `nack()` when `retry_count >= max_retries`
- `pgque.dlq_inspect()`, `pgque.dlq_replay()`, `pgque.dlq_replay_all()`,
  `pgque.dlq_purge()`

### Observability
- `pgque.get_queue_info()` / `pgque.get_queue_info(queue)`
- `pgque.get_consumer_info()` / `(queue)` / `(queue, consumer)`
- `pgque.get_batch_info(batch_id)`

### PgQ primitives (advanced use)
Available but most users should prefer the modern API above. See
`docs/reference.md` for the full list.
- `insert_event`, `register_consumer`, `unregister_consumer`
- `next_batch`, `next_batch_info`, `next_batch_custom`
- `get_batch_events`, `get_batch_cursor`
- `finish_batch`, `event_retry`, `batch_retry`

### Trigger helpers (change-data-capture)
- `pgque.jsontriga()`, `pgque.logutriga()`, `pgque.sqltriga()`

### Roles
- `pgque_reader`, `pgque_writer`, `pgque_admin` (with inheritance
  `admin > writer > reader`)

## Experimental SQL (`sql/experimental/`)

These files are not installed by default in v0.1.

### `sql/experimental/delayed.sql`
- delayed delivery table
- `pgque.send_at()`
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
