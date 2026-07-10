# pgque

Ruby client for [PgQue](https://github.com/NikolayS/pgque) — the PgQ-based
universal PostgreSQL queue. Thin wrapper over `pgque-api` SQL functions:
`send`, `send_batch`, `subscribe`, `unsubscribe`, `receive`, `ack`,
`nack`, `ticker`, `ticker_all`, `force_next_tick`, plus a polling
`Consumer` with `LISTEN`/`NOTIFY` wakeup.

## Install

```bash
gem install pgque --pre
```

`--pre` is required while v0.3.0 is in release-candidate; the latest
published version is `0.3.0.rc.1`. Pin the exact version if you prefer:

```ruby
gem "pgque", "0.3.0.rc.1"
```

Requires Ruby 3.1+ and PostgreSQL 14+ with the PgQue schema installed
(`\i pgque.sql` — no extension required).

## Database permissions

The connecting database role needs `pgque_reader` to consume (`receive`,
`ack`, `nack`, `subscribe`, `unsubscribe`) and `pgque_writer` to produce
(`send`, `send_batch`). The two are **siblings** — neither inherits the
other. An app that both produces and consumes (the typical case for code
using this client) must be granted **both** roles:

```sql
grant pgque_reader to your_app_user;
grant pgque_writer to your_app_user;
```

See [`docs/reference.md` — Roles and grants](../../docs/reference.md#roles-and-grants)
for the full role table.

## Quickstart

```ruby
require "pgque"

Pgque.connect("postgresql://localhost/mydb") do |client|
  # one-time setup (typically in a migration)
  client.conn.exec("select pgque.create_queue('orders')")
  client.subscribe("orders", "order_worker")

  # producer
  event_id  = client.send("orders", { "order_id" => 42 }, type: "order.created")
  batch_ids = client.send_batch("orders", "order.created", [
    { "order_id" => 43 },
    { "order_id" => 44 },
  ])
  puts "#{event_id} #{batch_ids.inspect}"
end

# consumer (separate process / thread)
consumer = Pgque::Consumer.new(
  "postgresql://localhost/mydb",
  queue: "orders",
  name: "order_worker",
)

consumer.on("order.created") { |msg| process_order(msg.payload) }

# Optional: catch-all handler for types with no specific handler.
# Without it, messages with unhandled types are nacked by default
# (sent to retry_queue, or to the dead-letter queue once
# queue_max_retries is exhausted). Register a "*" handler to take
# explicit control.
consumer.on("*") { |msg| log_unhandled(msg.type, msg.payload) }

consumer.start  # blocks until SIGTERM / SIGINT
```

The consumer only sees events after `pgque.ticker()` has materialized
a batch. With `pg_cron` available, run `select pgque.start();` once
to schedule the default 10 ticks/sec. Without `pg_cron`, drive
ticking from your application or an external scheduler — see the
project [Installation](https://github.com/NikolayS/pgque#installation)
section for both paths.

### Consumer options

`Consumer.new(..., max_messages: ...)` controls the per-`receive` limit.
The default is PostgreSQL's `int` maximum, so the consumer requests
the whole PgQ batch before acknowledging it. `ack` finishes the
entire underlying PgQ batch, including rows beyond `max_messages`;
only lower this value when it is at least as large as the queue's
worst-case batch size, otherwise rows past the limit are silently
skipped by the batch ack.

Other options: `poll_interval:` (seconds between polls when no
`LISTEN`/`NOTIFY` arrives, default 30), `retry_after:` (seconds before
nacked messages are retried, default 60), and `logger:` (a `Logger`
instance; the default targets `$stderr` at `FATAL`, so the consumer is
effectively silent unless you set `PGQUE_LOG_LEVEL=warn` or pass your
own).

After finishing a non-empty batch, the consumer polls again immediately
until the existing backlog is empty. `LISTEN`/`NOTIFY` is only a wakeup
optimization: notifications sent before a consumer connected cannot be
replayed, so waiting after every batch would otherwise drain an existing
backlog at only one batch per `poll_interval`.

Connection, `LISTEN`, receive, and ack errors do not terminate `start`.
The consumer closes the failed connection, waits `poll_interval` in
stop-aware slices, reconnects, re-subscribes to notifications, and resumes.
Set `PGQUE_LOG_LEVEL=error` (or pass `logger:`) to make these retry events
visible. Permanent configuration errors such as a missing queue or consumer
also retry until `stop`, SIGTERM, or SIGINT.

### Handling unknown event types

By default the consumer **nacks** any message whose type has no
registered handler and no `"*"` catch-all. The message is retried (or
dead-lettered once `queue_max_retries` is exhausted) so unknown types
are never silently dropped.

To ack unknown types instead, pass `unknown_handler_policy: "ack"`:

```ruby
consumer = Pgque::Consumer.new(
  "postgresql://localhost/mydb",
  queue: "orders",
  name: "order_worker",
  unknown_handler_policy: "ack",  # log WARNING and ack; do not nack
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

```ruby
require "pgque"

# worker-1
c1 = Pgque::Consumer.new(
  "postgresql://localhost/mydb",
  queue: "orders",
  name: "order_worker",
  subconsumer: "worker-1",
  dead_interval: "5 minutes",  # optional: take over a stale sibling
)

c1.on("order.created") { |msg| process(msg) }

c1.start  # in a second process: subconsumer: "worker-2"
```

`Consumer.new(subconsumer: ...)` switches the poll loop to
`receive_coop` and uses the cooperative cursor. `dead_interval:` is
only valid in cooperative mode; passing it without `subconsumer:`
raises `ArgumentError`.

The low-level methods on `Pgque::Client` are also available for direct
use:

```ruby
client.subscribe_subconsumer("orders", "order_worker", "worker-1")
msgs = client.receive_coop(
  "orders", "order_worker", "worker-1",
  max_messages: 100, dead_interval: "5 minutes",
)
client.ack(msgs[0].batch_id)
client.touch_subconsumer("orders", "order_worker", "worker-1")
client.unsubscribe_subconsumer(
  "orders", "order_worker", "worker-1", batch_handling: 1,
)
```

`unsubscribe_subconsumer(..., batch_handling: 0)` (the default) raises
if the subconsumer holds an active batch; pass `batch_handling: 1` to
route active messages through retry/DLQ before removal.

## Manual ticking

For tests, demos, or manual operation without `pg_cron`, use
`client.force_next_tick(queue)` to force the **next** ticker call to
materialize a tick. It does not insert the tick itself:

```ruby
client.force_next_tick("orders")
client.ticker("orders")
```

`client.ticker_all` runs the global ticker across all eligible queues
and returns the number of queues that received a tick.

## Transactions

`send` → ticker → `receive` must each run in its own committed
transaction (PgQue is snapshot-based). Ruby's `pg` gem runs each
statement in its own implicit transaction by default — the equivalent
of psycopg's `autocommit=True` — so the snippets above already commit
between phases without any explicit `BEGIN`/`COMMIT`.

To group several statements into one transaction (for example, to
publish a batch atomically with surrounding bookkeeping), wrap them in
a `transaction` block on the underlying `PG::Connection`:

```ruby
client.conn.transaction do
  client.send("orders", { "order_id" => 42 })
  bookkeeping(client.conn)
end  # commits here; raises rollback the whole block
```

Don't wrap `send` and `receive` in one explicit transaction; same for
`maint_retry_events` + `ticker`. See the
[snapshot rule](https://github.com/NikolayS/pgque/blob/main/docs/pgq-concepts.md#snapshot-rule).
The built-in `Pgque::Consumer` already wraps `receive` + dispatch +
`ack` in a single `conn.transaction` per poll, so handler code does
not need to manage that.

## A note on `Pgque::Client#send`

The producer method is called `send` to mirror the SQL surface
(`pgque.send(queue, payload)`) and the Python/TS clients. That name
shadows Ruby's `Object#send`, which is widely used for reflective
method invocation. This means `client.send(:close)` calls the SQL
`send`, **not** the `close` method.

Two well-known Ruby escape hatches restore reflective dispatch on a
`Pgque::Client` instance:

```ruby
client.__send__(:close)        # canonical "always works" form
client.public_send(:close)     # safer: respects visibility
```

Use `__send__` or `public_send` whenever you need to call a method on
a `Pgque::Client` by name. The Pgque API itself never calls these
internally.

## Tests

Integration tests require a running PostgreSQL with the PgQue schema
installed. Set `PGQUE_TEST_DSN` and run rake:

```bash
PGQUE_TEST_DSN=postgresql://postgres:pgque_test@localhost/pgque_test \
    bundle exec rake test
```

Without `PGQUE_TEST_DSN`, the tests skip.

## Distribution

The RubyGems distribution is `pgque`; require it as:

```ruby
require "pgque"
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
