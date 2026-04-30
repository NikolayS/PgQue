# pgque-go

Go client for [PgQue](https://github.com/NikolayS/pgque) — the PgQ-based
universal PostgreSQL queue. A thin, idiomatic wrapper over the
`pgque-api` SQL functions: `send`, `receive`, `ack`, `nack`.

## Install

```bash
go get github.com/NikolayS/pgque/clients/go
```

Requires Go 1.21+ and PostgreSQL 14+ with the PgQue schema installed
(`\i pgque.sql` — no extension required).

## Quickstart

```go
package main

import (
    "context"
    "log"

    pgque "github.com/NikolayS/pgque/clients/go"
)

func main() {
    ctx := context.Background()

    client, err := pgque.Connect(ctx, "postgres://user:pass@localhost/mydb")
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    // One-time queue + consumer setup (run once, e.g. in a migration):
    //   select pgque.create_queue('orders');
    //   select pgque.register_consumer('orders', 'order_worker');

    // Producer side
    _, err = client.Send(ctx, "orders", pgque.Event{
        Type:    "order.created",
        Payload: map[string]any{"order_id": 42},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Consumer side
    consumer := client.NewConsumer("orders", "order_worker",
        // pgque.WithPollInterval(30 * time.Second),
        // pgque.WithMaxMessages(500),                      // default
        // pgque.WithUnknownHandlerPolicy(pgque.NackUnknown), // default
    )
    consumer.Handle("order.created", func(ctx context.Context, msg pgque.Message) error {
        log.Printf("got %s: %s", msg.Type, msg.Payload)
        return nil
    })
    if err := consumer.Start(ctx); err != nil {
        log.Fatal(err)
    }
}
```

### Consumer configuration

| Option | Default | Notes |
|---|---|---|
| `WithPollInterval(d)` | `30s` | Time between polls when LISTEN/NOTIFY is silent. |
| `WithMaxMessages(n)` | `500` | Max messages requested per `pgque.receive`. **Keep `>= queue_ticker_max_count`** (default 500) so a single Receive drains the batch. |
| `WithUnknownHandlerPolicy(p)` | `NackUnknown` | `NackUnknown` (data-safe default) routes unhandled types to retry/DLQ. `AckUnknown` logs + acks instead. |

### Per-message Nack options

`Client.Nack` accepts variadic options:

```go
client.Nack(ctx, batchID, msg,
    pgque.WithRetryAfter(5 * time.Minute),
    pgque.WithReason("payment.declined"),
)
```

Defaults: `retry_after = 60s`, `reason = NULL`.

### Batch send

```go
ids, err := client.SendBatch(ctx, "orders", "order.created",
    []any{
        map[string]any{"id": 1},
        map[string]any{"id": 2},
        map[string]any{"id": 3},
    })
```

Wraps `pgque.send_batch(text, text, jsonb[])` 1:1.

## Tests

The integration tests require a running PostgreSQL with the PgQue schema
installed. Set `PGQUE_TEST_DSN` to point at it:

```bash
PGQUE_TEST_DSN=postgres://postgres:pgque_test@localhost/pgque_test \
  go test ./...
```

Without `PGQUE_TEST_DSN`, the tests skip.

## More

- Schema install, full reference, tutorial:
  <https://github.com/NikolayS/pgque>
- Per-function SQL reference:
  <https://github.com/NikolayS/pgque/blob/main/docs/reference.md>
- Issues / discussion: <https://github.com/NikolayS/pgque/issues>

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
