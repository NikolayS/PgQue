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

consumer.start()  # blocks until SIGTERM / SIGINT
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
