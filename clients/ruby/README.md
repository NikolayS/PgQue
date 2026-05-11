# pgque

Ruby client for [PgQue](https://github.com/NikolayS/pgque) — the PgQ-based
universal PostgreSQL queue. Thin wrapper over `pgque-api` SQL functions:
`send`, `receive`, `ack`, `nack`, `force_next_tick`, plus a polling
`Consumer` with `LISTEN`/`NOTIFY` wakeup.

## Install

```bash
gem install pgque --pre
```

`--pre` is required while v0.2.0 is in release-candidate; the latest
published version is `0.2.0.rc.1`. Pin the exact version if you prefer:

```ruby
gem "pgque", "0.2.0.rc.1"
```

Requires Ruby 3.1+ and PostgreSQL 14+ with the PgQue schema installed
(`\i pgque.sql` — no extension required).

## Database permissions

The connecting database role needs `pgque_reader` to consume (`receive`,
`ack`, `nack`) and `pgque_writer` to produce (`send`, `send_batch`). The
two are **siblings** — neither inherits the other. An app that both
produces and consumes must be granted **both** roles:

```sql
grant pgque_reader to your_app_user;
grant pgque_writer to your_app_user;
```

See [`docs/reference.md` — Roles and grants](../../docs/reference.md#roles-and-grants).

## Quickstart

Run the one-time setup once (typically in a migration), then produce
and consume from any process:

```ruby
require "pgque"

Pgque.connect("postgresql://localhost/mydb") do |client|
  # one-time setup
  client.conn.exec("select pgque.create_queue('orders')")
  client.conn.exec("select pgque.subscribe('orders', 'order_worker')")

  # produce
  client.send("orders", { "order_id" => 42 }, type: "order.created")
end

# consume (separate process)
consumer = Pgque::Consumer.new(
  "postgresql://localhost/mydb",
  queue: "orders",
  name: "order_worker",
)
consumer.on("order.created") { |msg| process_order(msg.payload) }
consumer.start  # blocks until SIGTERM / SIGINT
```

The consumer only sees events after `pgque.ticker()` has materialized
a batch. With `pg_cron` available, run `select pgque.start();` once
to schedule the default 10 ticks/sec. Without `pg_cron`, drive
ticking from your application or an external scheduler — see the
project [Installation](https://github.com/NikolayS/pgque#installation)
section for both paths.

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

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
