# pgque-py

Python client for [PgQue](https://github.com/NikolayS/pgque) — the PgQ-based
universal PostgreSQL queue. Thin wrapper over `pgque-api` SQL functions:
`send`, `send_batch`, `subscribe`, `unsubscribe`, `receive`, `ack`,
`nack`, `ticker`, `ticker_all`, `force_next_tick`, plus a polling
`Consumer` with `LISTEN`/`NOTIFY` wakeup.

## Install

```bash
pip install pgque-py
```

Requires Python 3.10+ and PostgreSQL 14+ with the PgQue schema installed
(`\i pgque.sql` — no extension required).

## Database permissions

The connecting database role needs `pgque_reader` to consume (`receive`, `ack`, `nack`, `subscribe`, `unsubscribe`) and `pgque_writer` to produce (`send`, `send_batch`). The two are **siblings** — neither inherits the other. An app that both produces and consumes (the typical case for code using this client) must be granted **both** roles:

```sql
grant pgque_reader to your_app_user;
grant pgque_writer to your_app_user;
```

See [`docs/reference.md` — Roles and grants](../../docs/reference.md#roles-and-grants) for the full role table.

## Quickstart

```python
import pgque

with pgque.connect("postgresql://localhost/mydb") as client:
    # one-time setup (typically in a migration)
    client.subscribe("orders", "order_worker")
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

`Consumer(..., max_messages=...)` controls the per-`receive` limit.
The default is PostgreSQL's `int` maximum, so the consumer requests
the whole PgQ batch before acknowledging it. `ack()` finishes the
entire underlying PgQ batch, including rows beyond `max_messages`;
only lower this value when it is at least as large as the queue's
worst-case batch size, otherwise rows past the limit are silently
skipped by the batch ack.

### Handling unknown event types

By default the consumer **nacks** any message whose type has no
registered handler and no `"*"` catch-all. The message is retried (or
dead-lettered once `queue_max_retries` is exhausted) so unknown types
are never silently dropped.

To ack unknown types instead, pass `unknown_handler_policy="ack"`:

```python
consumer = pgque.Consumer(
    dsn="postgresql://localhost/mydb",
    queue="orders",
    name="order_worker",
    unknown_handler_policy="ack",  # log WARNING and ack; do not nack
)
```

## Experimental: cooperative consumers

> **Experimental in PgQue 0.2.** Function names, edge-case behavior, and
> client API shape may change before this feature is marked stable. Do
> not use this as the only processing path for critical workloads
> without idempotent handlers and stale-worker takeover tests.

Cooperative consumers let several worker processes share **one logical
consumer**. Each batch is handed to exactly one subconsumer; the main
row owns the group cursor, member rows own active batches. See
[`docs/reference.md` — Cooperative consumers / subconsumers](../../docs/reference.md#cooperative-consumers--subconsumers)
for the SQL surface.

Two-worker example (each worker holds its own connection / process):

```python
import pgque

# worker-1
c1 = pgque.Consumer(
    dsn="postgresql://localhost/mydb",
    queue="orders",
    name="order_worker",
    subconsumer="worker-1",
    dead_interval="5 minutes",  # optional: take over a stale sibling
)

@c1.on("order.created")
def handle(msg):
    process(msg)

c1.start()  # in a second process: subconsumer="worker-2"
```

`Consumer(subconsumer=...)` switches the poll loop to
`receive_coop` and auto-registers the `coop_main` + `coop_member` rows
on the first call. `dead_interval` is only valid in cooperative mode;
passing it without `subconsumer` raises `ValueError`.

The low-level methods on `PgqueClient` are also available for direct
use:

```python
client.subscribe_subconsumer("orders", "order_worker", "worker-1")
msgs = client.receive_coop(
    "orders", "order_worker", "worker-1",
    max_messages=100, dead_interval="5 minutes",
)
client.ack(msgs[0].batch_id)
client.touch_subconsumer("orders", "order_worker", "worker-1")
client.unsubscribe_subconsumer(
    "orders", "order_worker", "worker-1", batch_handling=1,
)
```

`unsubscribe_subconsumer(..., batch_handling=0)` (the default) raises if
the subconsumer holds an active batch; pass `batch_handling=1` to route
active messages through retry/DLQ before removal.

A runnable two-worker demo lives at
[`bench/coop_demo.py`](bench/coop_demo.py); run it against any pgque
database with `PGQUE_TEST_DSN` set.

## Manual ticking

For tests, demos, or manual operation without `pg_cron`, use
`client.force_next_tick(queue)` to force the **next** ticker call to
materialize a tick. It does not insert the tick itself:

```python
client.force_next_tick("orders")
client.ticker("orders")
client.conn.commit()
```

`client.force_tick(queue)` remains as a deprecated compatibility alias.

## Transactions

`send` → ticker → `receive` must each run in its own committed transaction (PgQue is snapshot-based). `pgque.connect(dsn)` is non-autocommit by default — commit between produce and consumer. The `Consumer` is autocommit + explicit `conn.transaction()` around `receive + dispatch + ack`.

Don't wrap `send` and `receive` in one explicit tx; same for `maint_retry_events` + `ticker`. See [snapshot rule](https://github.com/NikolayS/pgque/blob/main/docs/pgq-concepts.md#snapshot-rule).


## Tests

Integration tests require a running PostgreSQL with the PgQue schema
installed. Set `PGQUE_TEST_DSN` and run pytest:

```bash
PGQUE_TEST_DSN=postgresql://postgres:pgque_test@localhost/pgque_test \
    pytest clients/python/tests
```

Without `PGQUE_TEST_DSN`, the tests skip.

## Distribution

The PyPI distribution is `pgque-py`; the import package is `pgque`:

```python
import pgque
```

See [RELEASE.md](RELEASE.md) for publishing steps.

## More

- Schema install, full reference, tutorial:
  <https://github.com/NikolayS/pgque>
- Per-function SQL reference:
  <https://github.com/NikolayS/pgque/blob/main/docs/reference.md>
- Issues: <https://github.com/NikolayS/pgque/issues>

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
