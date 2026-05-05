# PgQ: Concepts

Vocabulary adapted from the 2009 PgCon talk by Kreen & Pihlak
([slides](https://www.pgcon.org/2009/schedule/attachments/91_pgq.pdf)).

## Glossary

- **Event** — one row in a queue table. Delivered **at-least-once**.
- **Batch** — events between two ticks, served to a consumer together.
- **Queue** — named event stream; 3 rotating tables, purged by `TRUNCATE`.
  Any number of queues can coexist in one database.
- **Producer** — anything that calls `insert_event` / `pgque.send`. Any
  number of producers can write to the same queue concurrently.
- **Consumer** — subscribes, reads batches, calls `ack` (or `finish_batch`).
  Any number of consumers can subscribe to the same queue; each has its
  own cursor and independently sees every event (fan-out by default).
- **Ticker** — creates ticks, vacuums, rotates, reschedules retries.
  In PgQue: `pg_cron` calling `pgque.ticker()`.
- **Tick** — position marker in the event stream; delimits batches.
- **Roles** — three database roles, all created by the install:
  - `pgque_reader` (consume side: `receive`, `ack`, `nack`, `subscribe`, `unsubscribe`, plus the underlying PgQ batch primitives)
  - `pgque_writer` (produce side: `send`, `send_batch`, `insert_event`)
  - `pgque_admin` (operator; member of both)
  Reader and writer are **siblings** — neither inherits the other. Apps that produce and consume must hold both. See [`reference.md` — Roles and grants](reference.md#roles-and-grants) for the full table and rationale (issues #102, #106, #163).

## Delivery

At-least-once. Exactly-once requires either:

- **Same DB:** process in the same transaction as `finish_batch` (or `pgque.ack`).
- **Cross DB:** target-side batch/event tracking — record the `batch_id` or per-event ids on the target side and skip duplicates. PgQue does not ship a helper for this today.

## Consumer loop

```
batch_id = next_batch(queue, consumer)   -- NULL → sleep, retry
events   = get_batch_events(batch_id)
process(events)                           -- nack individual failures
finish_batch(batch_id)
commit
```

## Event row

`ev_id`, `ev_time`, `ev_txid` (`xid8`), `ev_retry`, `ev_type`, `ev_data`,
`ev_extra1..4`. `ev_extra1` is table name by convention (triggers).
Payload format is a producer/consumer contract — PgQue does not interpret it.

## Health signals

`pgque.get_consumer_info()`:

- **lag** — age of last finished batch; high = falling behind.
- **last_seen** — time since last batch; high = consumer not running.

## Per-queue tuning

Stored on `pgque.queue`, read by `pgque.ticker()` (pg_cron). Set via
`pgque.set_queue_config(queue, param, value)` — `param` is the short name
below; the function auto-prefixes `queue_` internally.

- `ticker_max_lag` — max wall time between ticks.
- `ticker_idle_period` — tick interval when idle.
- `ticker_max_count` — force tick at N events (batch-size cap).
- `rotation_period` — table rotation period (disk vs. history).
- `max_retries` — retry ceiling before a message goes to `pgque.dead_letter`.

## Ticker rule

> Keep the ticker running. No ticks → no batches → no delivery. Long pauses
> produce huge batches consumers can't handle.

— Kreen & Pihlak, PgCon 2009

## Snapshot rule

PgQue is snapshot-based, not row-claiming. The ticker records the
PostgreSQL snapshot it sees; `pgque.receive` only returns events whose
insert committed **before** that snapshot. Consequence: the following
operation chains MUST run in distinct, committed transactions —
combining any chain in one explicit `begin`/`commit` block silently
produces empty batches and dropped messages.

- **Producer → consumer.** `pgque.send` (or `pgque.insert_event`) →
  `pgque.ticker` (or `pgque.force_tick` + `pgque.ticker`) →
  `pgque.receive` (or `pgque.next_batch`).
- **Retry pump.** `pgque.maint_retry_events` (re-inserts retry rows
  into event tables with `pg_current_xact_id()`) → `pgque.ticker`
  (must run in a later tx so the new `ev_txid`s are visible in its
  snapshot) → `pgque.receive`.
- **Rotation.** `pgque.maint_rotate_tables_step1` →
  `pgque.maint_rotate_tables_step2` (PgQ design requirement).

By contrast, `receive → process → ack` belongs in **one** transaction
when you want exactly-once effects on the same database (see the
[transactional pattern](examples.md#exactly-once-processing-transactional-pattern)).
The asymmetry: producer-to-consumer flow needs commit boundaries between
steps; consume-to-side-effect flow needs them merged.

For the shipped clients: Go (`pgxpool`) and TypeScript (`pg.Pool`) run
each call in its own implicit transaction, so the rule is satisfied
transparently. The Python client requires care — `pgque.connect(dsn)`
is **not** autocommit by default, so producers must commit explicitly
between `send` and the consumer side; the high-level Python `Consumer`
already handles this internally (autocommit + an explicit
`conn.transaction()` around `receive + dispatch + ack`). The footgun
in every driver is reaching for the underlying pool/connection
(`Client.Pool()`, `client.rawPool`, `client.conn`) to wrap `send` and
`receive` in one explicit transaction — the consumer side will not see
what the producer just sent.

## Three latencies

For the full explanation — producer latency, subscriber latency,
end-to-end delivery, tick-cadence trade-offs, and comparison with
UPDATE/DELETE-based designs — see [three-latencies.md](three-latencies.md).
