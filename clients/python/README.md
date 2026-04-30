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

    # producer
    client.send("orders", {"order_id": 42}, type="order.created")
    client.conn.commit()

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
# By default, messages whose type has no registered handler are
# **nacked** (routed to retry_queue, then dead_letter). Pass
# `unknown_handler="ack"` to the Consumer to log + ack instead — useful
# when handler registration is intentionally an allow-list filter.
@consumer.on("*")
def handle_unknown(msg: pgque.Message) -> None:
    print(f"unhandled type {msg.type!r}: {msg.payload}")

consumer.start()  # blocks until SIGTERM / SIGINT
```

### Consumer configuration

Important defaults:

- `max_messages=500` — matches the default `pgque.ticker_max_count` so a
  single `receive` can drain a full batch. **Set `max_messages >=
  ticker_max_count`** to avoid leaving rows undelivered for the rest of
  the batch's lifetime.
- `unknown_handler="nack"` — unhandled event types are nacked (data-safe
  default). Pass `"ack"` to opt into the previous warn+ack behaviour.
- `poll_interval=30` — seconds between polls when LISTEN/NOTIFY does not
  fire.
- `retry_after=60` — seconds before a nacked message becomes available
  again.

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
