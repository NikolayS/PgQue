# PgQ: History and Concepts

Reference material distilled from Martin Pihlak's 2009 PgCon presentation
"Skytools: PgQ -- Queues and applications"
(https://www.pgcon.org/2009/schedule/attachments/91_pgq.pdf).

This document is a concept primer for PgQue contributors and a source of
quotable phrasing for README / SPEC / user-facing docs. All paraphrased
definitions originate with the PgQ authors at Skype; PgQue inherits them.

## History

- **2006** -- PgQ started at Skype, inspired by ideas from Slony.
- **2007** -- First application was Londiste replication. Open-sourced as part
  of the Skytools framework.
- **2009** -- Skytools 3.0 alpha introduced cooperative consumers and cascading.
- **2026** -- PgQue repackages the PgQ core (ISC, Marko Kreen / Skype
  Technologies OU) for PG14+ managed database environments.

## Core feature claims

From slide 5, lightly adapted:

- **Transactional.** Events are created transactionally, coupled with
  surrounding business logic.
- **Efficient.** Events are processed in batches, giving low per-event
  overhead.
- **Flexible.** No limits on the number of producers or consumers. Custom
  event formats.
- **Reliable.** Events live in PostgreSQL, inheriting write-ahead logging and
  crash recovery.
- **Easy to use.** Simple SQL interface.

## Use cases

- Asynchronous messaging
- Batch processing
- Replication (Londiste is the historical flagship)
- Distributed transactions (via event / batch tracking across databases)

## Glossary

These definitions are tighter than the ones currently in `SPECx.md` /
`SPEC.md` and should be reused verbatim in user-facing docs.

### Event

An atomic piece of data created by a producer. Physically, an event is one row
in a queue table.

**Guarantee:** PgQ guarantees that each event is seen *at least once*. It is
up to the consumer to ensure the event is processed *no more than once* when
exactly-once semantics matter.

### Batch

A group of events served to a consumer together. PgQ is designed for
efficiency and high throughput, so events are processed in batches rather than
one at a time. Batch size is tunable: larger batches for WAN consumers, smaller
batches for low-latency local processing.

Small batches incur higher per-event overhead; excessively large batches have
their own disadvantages (memory pressure, long processing windows, retry cost).

### Queue

A named stream of events, physically a set of tables in a PostgreSQL database.
The default is **3 rotating tables per queue**, which allows discarded events
to be purged by `TRUNCATE` rather than `DELETE` + `VACUUM`.

- An event is discarded when all registered consumers have processed it.
- Any number of producers can write to a queue.
- Any number of consumers can read from a queue.
- Multiple queues can live in the same database.

### Producer

Any application that places events into a queue. A producer can be written in
any language that can execute SQL against PostgreSQL.

Two common production patterns:

1. **Direct API.** Call `pgq.insert_event(queue, ev_type, ev_data)` (or
   `pgque.send()` in PgQue's modern API) from application code.
2. **Trigger-based capture.** Attach `pgq.logutriga` / `pgq.sqltriga` to a
   table to auto-enqueue change events -- the basis of trigger-based
   replication.

### Consumer

Any application that reads events from a queue. Can be written in any language
that can talk to PostgreSQL.

Key behaviors:

- Subscribes to a queue (explicitly in Skytools 3 / PgQue).
- Sees only events produced *after* the subscription.
- Obtains events one batch at a time.
- Must call a "finish batch" operation to advance its position.
- Can postpone an individual event for retry processing (e.g. a transient
  downstream failure).

### Ticker

A daemon that periodically creates **ticks** on each queue. A tick is a
position marker in the event stream; a batch is the set of events enqueued
between two consecutive ticks.

Responsibilities:

- Produce ticks (no ticks -> no batches -> no event delivery).
- Vacuum queue tables.
- Schedule retry events back onto the queue.
- Rotate queue tables.

**Operational warning:** "Pausing the ticker for an extended period will
produce a huge batch, consumers might not be able to cope with it. Keep the
ticker running!" -- Pihlak, 2009.

In PgQue, the ticker runs via `pg_cron` by default.

### Tick

A single position in the event stream, created by the ticker. Ticks delimit
batches.

### Cooperative consumer (sub-consumer)

A Skytools 3 concept: multiple sub-consumers share the workload of a single
logical consumer so events are not processed twice. Useful when one consumer
cannot keep up with the volume. PgQue's equivalent lives in the `pgque-api`
layer.

### Cascade (out of scope for pgque-core)

A set of database nodes that maintain an identical queue, with event and
batch numbers kept identical across nodes. Used historically for Londiste
replication failover. Not part of pgque-core; documented here for historical
context only.

## Event structure

Historical column layout of a queue row, still exposed by pgque-core:

| Column      | Type        | Purpose                                    |
|-------------|-------------|--------------------------------------------|
| `ev_id`     | `bigint`    | Monotonic event identifier                 |
| `ev_time`   | `timestamptz` | Enqueue time                             |
| `ev_txid`   | `xid8`      | Producing transaction (modernized in PgQue)|
| `ev_owner`  | `integer`   | Internal: assigned consumer on retry       |
| `ev_retry`  | `integer`   | Retry counter                              |
| `ev_type`   | `text`      | User-defined event type tag                |
| `ev_data`   | `text`      | User-defined payload                       |
| `ev_extra1` | `text`      | Convention: table name (for triggers)      |
| `ev_extra2` | `text`      | Convention: row backup / auxiliary data    |
| `ev_extra3` | `text`      | Free-form                                  |
| `ev_extra4` | `text`      | Free-form                                  |

"Field names hint at their intended usage" -- Pihlak, 2009. The content format
of `ev_data` / `ev_type` is agreed between producer and consumer; PgQ / PgQue
does not interpret it.

## Delivery semantics

PgQ provides **at-least-once** delivery. Strategies for achieving
exactly-once processing:

1. **Same-database processing.** Process the event in the same transaction
   that reads the batch. Rollback on failure replays the batch; commit
   atomically finishes the batch. No separate tracking needed.
2. **Cross-database processing.** Use batch / event tracking on the target
   database (`pgq_ext.is_batch_done` / `set_batch_done` in PgQ; the PgQue
   equivalent in `pgque-api`). Idempotency is guaranteed by skipping batches
   already marked done on the target before committing on the source.

The second pattern is PgQ's building block for asynchronous distributed
transactions.

## Consumer lifecycle (the contract)

1. `next_batch(queue, consumer)` -> batch id, or `NULL` if nothing is ready.
2. `get_batch_events(batch_id)` -> the event set (may be empty if the tick
   interval had no events).
3. Process the events. Optionally call `event_retry(batch_id, ev_id, sec)` to
   postpone an individual event.
4. `finish_batch(batch_id)`.
5. Commit.

A `NULL` return from step 1 means "sleep `loop_delay` and try again."

## Consumer status signals

Two numbers expose consumer health (`pgq.get_consumer_info`):

- **lag** -- age of the last finished batch. High lag means the consumer is
  falling behind.
- **last_seen** -- elapsed time since the consumer processed any batch. High
  last_seen means the consumer process is not running (or is stuck).

Both are actionable monitoring metrics and should feed Nagios / Prometheus /
CloudWatch alerts.

## Operational tuning knobs

These are **per-queue DB-level settings**, stored as columns on
`pgque.queue` (`queue_ticker_max_lag`, `queue_ticker_max_count`,
`queue_rotation_period`, ...). They are consulted by whatever process calls
`pgque.ticker()` -- in PgQue, that is `pg_cron`. They are *not* tied to the
historical `pgqd` / `pgqadm.py` daemons (which we do not ship); those tools
were just client-side front-ends that wrote the same DB columns.

- `ticker_max_lag` -- max wall time between ticks.
- `ticker_idle_period` -- tick interval when no events are arriving.
- `ticker_max_count` -- force a tick when this many events have accumulated.
  Effectively a batch-size cap.
- `rotation_period` -- how often to rotate queue tables. Trades disk space
  against retained event history.

Set via the `options` jsonb argument to `pgque.create_queue()` (see
`SPECx.md`), or updated later via the equivalent config function.

## Source notes

- All page references above are to the 2009 PgCon slides. Paraphrased text is
  marked as such; verbatim quotes are in double quotes and attributed to
  Pihlak, 2009.
- Original PgQ is ISC-licensed (Marko Kreen / Skype Technologies OU). Always
  preserve that notice when copying text into source files. See `NOTICE`.
