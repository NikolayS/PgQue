# pgque-go

Go client for [PgQue](https://github.com/NikolayS/pgque) — the PgQ-based
universal PostgreSQL queue. A thin, idiomatic wrapper over the
`pgque-api` SQL functions: `send`, `receive`, `ack`, `nack`.

## Install

After the first Go client release:

```bash
go get github.com/NikolayS/pgque-go@latest
```

Requires Go 1.21+ and PostgreSQL 14+ with the PgQue schema installed
(`\i pgque.sql` — no extension required).

## Quickstart

```go
package main

import (
    "context"
    "log"

    pgque "github.com/NikolayS/pgque-go"
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

    // Producer side -- single event
    _, err = client.Send(ctx, "orders", pgque.Event{
        Type:    "order.created",
        Payload: map[string]any{"order_id": 42},
    })
    if err != nil {
        log.Fatal(err)
    }

    // Producer side -- batch (one type, many payloads)
    ids, err := client.SendBatch(ctx, "orders", "order.created", []any{
        map[string]any{"order_id": 43},
        map[string]any{"order_id": 44},
    })
    if err != nil {
        log.Fatal(err)
    }
    log.Printf("published batch event IDs: %v", ids)

    // Consumer side
    consumer := client.NewConsumer("orders", "order_worker",
        pgque.WithUnknownHandlerPolicy(pgque.NackUnknown), // also the default
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

## Consumer options

| Option                                  | Default        | Notes                                                                 |
| --------------------------------------- | -------------- | --------------------------------------------------------------------- |
| `WithPollInterval(d time.Duration)`     | `30s`          | Idle backoff between polls when the queue is empty.                   |
| `WithMaxMessages(n int)`                | `math.MaxInt32` | Per-Receive limit. The default requests the whole PgQ batch before `Ack`. If you lower it below the real batch size, `Ack` still finishes the batch and unreturned rows are skipped. |
| `WithUnknownHandlerPolicy(p)`           | `NackUnknown`  | `AckUnknown` logs and skips messages with no registered handler.      |

## Nack options

`Client.Nack` takes optional, variadic `NackOption`s:

```go
err := client.Nack(ctx, batchID, msg,
    pgque.WithRetryAfter(5*time.Minute), // override 60s default
    pgque.WithReason("payment-declined"), // recorded on the dead_letter row
)
```

Calls without options preserve the historical defaults: 60-second retry
delay, NULL reason.

## At-least-once contract

If a per-message Nack call fails, the Consumer leaves the batch unacked
so PgQue redelivers it on the next Receive. Acking a batch whose Nack
failed would silently drop the failure information — the Go consumer
prefers redelivery and lets the at-least-once retry path do its job.

## Typed errors

Client methods wrap PostgreSQL-side failures so callers can route on
recoverable conditions with `errors.Is`:

```go
_, err := client.Send(ctx, "orders", pgque.Event{Type: "x", Payload: nil})
switch {
case errors.Is(err, pgque.ErrQueueNotFound):
    // create the queue, retry
case errors.Is(err, pgque.ErrConsumerNotFound):
    // re-register the consumer
case errors.Is(err, pgque.ErrBatchNotFound):
    // batch already finished — usually safe to ignore
case errors.Is(err, pgque.ErrConnection):
    // pool closed, network drop, bad DSN
case err != nil:
    // generic SQL error — extract SQLSTATE if needed
    var sqlErr *pgque.SqlError
    if errors.As(err, &sqlErr) {
        log.Printf("pgque %s failed: %s [SQLSTATE %s]",
            sqlErr.Op, sqlErr.Err, sqlErr.SQLSTATE)
    }
}
```

`context.Canceled` and `context.DeadlineExceeded` pass through unwrapped,
so `errors.Is(err, context.Canceled)` continues to work.

The same typed surface is exposed by the Python client (`PgqueQueueNotFound`,
`PgqueConsumerNotFound`, `PgqueBatchNotFound`, `PgqueConnectionError`) and
TypeScript client (`PgqueQueueNotFoundError`, `PgqueConsumerNotFoundError`,
`PgqueSqlError`).

## Tests

The integration tests require a running PostgreSQL with the PgQue schema
installed. Set `PGQUE_TEST_DSN` to point at it:

```bash
PGQUE_TEST_DSN=postgres://postgres:pgque_test@localhost/pgque_test \
  go test ./...
```

Without `PGQUE_TEST_DSN`, the tests skip.

## Distribution

This client is published as the Go module
`github.com/NikolayS/pgque-go`. Source lives in this monorepo under
`clients/go`; releases sync that subtree to the mirror repository and use
normal Go module tags such as `vX.Y.Z`.

See [RELEASE.md](RELEASE.md) for publishing steps.

## More

- Schema install, full reference, tutorial:
  <https://github.com/NikolayS/pgque>
- Per-function SQL reference:
  <https://github.com/NikolayS/pgque/blob/main/docs/reference.md>
- Issues / discussion: <https://github.com/NikolayS/pgque/issues>

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
