# PgQue semantics

PgQue is **batch-oriented**.

That is the single most important thing to understand before using it.

## Core model

PgQue wraps PgQ primitives into a friendlier API, but the underlying semantics
remain PgQ semantics:

- `pgque.receive(queue, consumer, n)` opens a PgQ batch
- the batch contains all events committed between two ticks
- `n` limits how many rows are returned to the caller
- `pgque.ack(batch_id)` finishes the **entire** batch
- `pgque.nack(...)` retries or dead-letters individual messages, then the batch
  is still acked normally

This is **not** an SQS-style per-message visibility-timeout queue.

## Why this matters

If you call:

```sql
select * from pgque.receive('orders', 'processor', 1);
```

and get one row back, then call:

```sql
select pgque.ack(batch_id);
```

you finish the whole underlying batch, not just that one returned row.

That behavior is intentional and inherited from PgQ's batch model.

## Retry flow

The normal failure flow is:

1. receive a batch
2. process messages
3. call `pgque.nack(batch_id, msg, retry_after, reason)` for messages that failed
4. call `pgque.ack(batch_id)` to finish the batch
5. maintenance moves retryable events back into the queue later
6. the retried event is delivered again with incremented `retry_count`

When `retry_count >= queue_max_retries`, `nack()` sends the event to the dead
letter queue instead of retrying it.

## Transactional consume-and-commit pattern

PgQue supports a transactional pattern where application writes and batch ack
happen in the same transaction.

That gives you a clean consume-and-commit flow inside Postgres, but you should
still describe it carefully. Avoid hand-wavy claims about universal
"exactly-once" delivery.

## Rotation blocking

An open batch can block rotation, because the consumer still depends on older
queue tables.

This is expected. Slow consumers must be monitored.

## Recommended mental model

Think of PgQue as:

- a zero-bloat, batch-oriented queue/event system for Postgres
- built for sustained load and managed Postgres compatibility
- not a drop-in clone of SQS, Redis queues, or `skip locked` job tables
