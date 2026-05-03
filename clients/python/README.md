# pgque-py

Python client for [PgQue](https://github.com/NikolayS/pgque) — the PgQ-based
universal PostgreSQL queue. Thin wrapper over `pgque-api` SQL functions:
`send`, `receive`, `ack`, `nack`, plus a polling `Consumer` with
`LISTEN`/`NOTIFY` wakeup.

## Install

```bash
pip install pgque
```

Requires Python 3.10+ and PostgreSQL 14+ with the PgQue schema installed
(`\i pgque.sql` — no extension required).

## Quickstart

```python
import pgque

with pgque.connect("postgresql://localhost/mydb") as client:
    # one-time setup (typically in a migration)
    client.conn.execute("select pgque.subscribe('orders', 'order_worker')")
    client.conn.commit()

    # producer: commit once to publish both calls atomically
    event_id = client.send("orders", {"order_id": 42}, type="order.created")
    batch_ids = client.send_batch("orders", "order.created", [
        {"order_id": 43},
        {"order_id": 44},
    ])
    client.conn.commit()
    print(event_id, batch_ids)

# consumer (separate process / thread)
consumer = pgque.Consumer(
    dsn="postgresql://localhost/mydb",
    queue="orders",
    name="order_worker",
)

@consumer.on("order.created")
def handle_order(msg: pgque.Message) -> None:
    print(f"got {msg.type}: {msg.payload}")

# Optional: catch-all handler for types with no specific handler.
# Without it, messages with unhandled types are nacked by default
# (sent to retry_queue, or to the dead-letter queue once
# queue_max_retries is exhausted). Register a "*" handler to take
# explicit control.
@consumer.on("*")
def handle_unknown(msg: pgque.Message) -> None:
    print(f"unhandled type {msg.type!r}: {msg.payload}")

consumer.start()  # blocks until SIGTERM / SIGINT
```

### Consumer options

`Consumer(..., max_messages=...)` controls the per-`receive` limit. By
default the consumer requests PostgreSQL's `int` maximum, so it drains
the whole PgQ batch before acknowledging it. If you lower this value
below the real batch size, `ack()` still finishes the batch and
unreturned rows are skipped.

### Handling unknown event types

By default the consumer **nacks** any message whose type has no
registered handler and no `"*"` catch-all. The message is retried (or
dead-lettered once `queue_max_retries` is exhausted) so unknown types
are never silently dropped.

To ack unknown types instead, pass `unknown_handler="ack"`:

```python
consumer = pgque.Consumer(
    dsn="postgresql://localhost/mydb",
    queue="orders",
    name="order_worker",
    unknown_handler="ack",  # log WARNING and ack; do not nack
)
```

## Tests

Integration tests require a running PostgreSQL with the PgQue schema
installed. Set `PGQUE_TEST_DSN` and run pytest:

```bash
PGQUE_TEST_DSN=postgresql://postgres:pgque_test@localhost/pgque_test \
    pytest clients/python/tests
```

Without `PGQUE_TEST_DSN`, the tests skip.

## More

- Schema install, full reference, tutorial:
  <https://github.com/NikolayS/pgque>
- Per-function SQL reference:
  <https://github.com/NikolayS/pgque/blob/main/docs/reference.md>
- Issues: <https://github.com/NikolayS/pgque/issues>

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
