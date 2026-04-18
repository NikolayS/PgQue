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
Payload format is producer/consumer contract; PgQue does not interpret it.

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
