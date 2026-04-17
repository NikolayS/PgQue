# PgQue phases

## Goal

Keep the default install small, understandable, and stable.

The default install should expose only the minimum supported API for v0.1.
Additional features can live in `sql/experimental/` until they prove simple,
useful, and stable enough to promote into the default install.

## Default install (`sql/pgque.sql`) — v0.1

### Core engine
- repackaged PgQ batching, ticking, rotation, retry queue, consumer tracking

### Lifecycle
- `pgque.start()`
- `pgque.stop()`
- `pgque.status()`
- `pgque.maint()`

### Minimal modern API
- `pgque.send(queue, payload)`
- `pgque.send(queue, type, payload)`
- `pgque.subscribe(queue, consumer)`
- `pgque.receive(queue, consumer, max_return)`
- `pgque.ack(batch_id)`
- `pgque.nack(batch_id, msg, retry_after, reason)`

### Dead letter queue
- `pgque.dead_letter` table
- `pgque.event_dead()` — called by `nack()` when `retry_count >= max_retries`
- `pgque.dlq_inspect()`, `pgque.dlq_replay()`, `pgque.dlq_replay_all()`,
  `pgque.dlq_purge()`

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
