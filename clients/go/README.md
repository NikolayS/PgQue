# pgque-go

Go client for [PgQue](https://github.com/NikolayS/pgque) — the PgQ-based
universal PostgreSQL queue. A thin, idiomatic wrapper over the
`pgque-api` SQL functions: `send`, `send_batch`, `subscribe`,
`unsubscribe`, `receive`, `ack`, `nack`, `ticker`, `ticker_all`, and
`force_next_tick`.

## Install

```bash
go get github.com/NikolayS/pgque-go@v0.2.0
```

The module is mirrored from `clients/go/` of the parent repo to the public [`NikolayS/pgque-go`](https://github.com/NikolayS/pgque-go) module repo.

Requires Go 1.21+ and PostgreSQL 14+ with the PgQue schema installed
(`\i pgque.sql` — no extension required).

## Database permissions

The connecting database role needs `pgque_reader` to consume (`receive`, `ack`, `nack`, `subscribe`, `unsubscribe`) and `pgque_writer` to produce (`send`, `send_batch`). The two are **siblings** — neither inherits the other. An app that both produces and consumes (the typical case for code using this client) must be granted **both** roles:

```sql
grant pgque_reader to your_app_user;
grant pgque_writer to your_app_user;
```

See [`docs/reference.md` — Roles and grants](../../docs/reference.md#roles-and-grants) for the full role table.

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
    if _, err := client.Subscribe(ctx, "orders", "order_worker"); err != nil {
        log.Fatal(err)
    }

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
| `WithRetryAfter(d time.Duration)`       | `60s`          | Retry delay for Consumer-issued `Nack` calls on handler failure or unknown type. |

## Manual ticking

For tests, demos, or manual operation without `pg_cron`, use
`Client.ForceNextTick(ctx, queue)` to force the **next** `pgque.ticker()` call
to materialize a tick. It does not insert the tick itself:

```go
_, err := client.ForceNextTick(ctx, "orders")
if err != nil {
    log.Fatal(err)
}
_, err = client.Ticker(ctx, "orders")
```

`Client.ForceTick(ctx, queue)` remains as a deprecated compatibility alias.

## Nack options

`Client.Nack` takes a `NackOptions` struct. Pointer fields default to the
SQL-side defaults when nil (60-second retry delay, NULL reason):

```go
err := client.Nack(ctx, batchID, msg, pgque.NackOptions{
    RetryAfter: ptr(5 * time.Minute),
    Reason:     ptr("payment-declined"),
})
```

Calls without options preserve the historical defaults: 60-second retry
delay, NULL reason.

## Ack rowcount

`Client.Ack` returns `(int64, error)`. The `int64` is the row-count from
`pgque.finish_batch`:

- `1` — batch was active and has been finished (normal success).
- `0` — no active batch was finished: the `batchID` was not found, was already
  finished (stale/double ack), or belongs to a different consumer. This is not a
  SQL error — the `error` return is nil. Log it at warn level if you see it.

```go
n, err := client.Ack(ctx, batchID)
if err != nil {
    log.Printf("ack SQL error: %v", err)
} else if n == 0 {
    log.Printf("ack returned 0 — stale or double ack for batch %d", batchID)
}
```

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
    var sqlErr *pgque.SQLError
    if errors.As(err, &sqlErr) {
        log.Printf("pgque %s failed: %s [SQLSTATE %s]",
            sqlErr.Op, sqlErr.Err, sqlErr.SQLSTATE)
    }
}
```

`context.Canceled` and `context.DeadlineExceeded` are preserved through
the chain, so `errors.Is(err, context.Canceled)` continues to work.

The same typed surface is exposed by the Python client (`PgqueQueueNotFound`,
`PgqueConsumerNotFound`, `PgqueBatchNotFound`, `PgqueConnectionError`) and
TypeScript client (`PgqueQueueNotFoundError`, `PgqueConsumerNotFoundError`,
`PgqueSqlError`). Go uses the standard acronym-uppercase convention
(`SQLError` rather than `SqlError`).

## Experimental: cooperative consumers

**Experimental in PgQue 0.2.** Function names, edge-case behavior, and
client API shape may change before this feature is marked stable. Do
not use this as the only processing path for critical workloads
without idempotent handlers and stale-worker takeover tests.

Cooperative consumers let several workers share one logical consumer
cursor. Each worker registers as a *subconsumer*; each cooperative
batch is allocated to one subconsumer at a time. Pass
`pgque.WithSubconsumer(name)` to the high-level `Consumer` to opt in:

```go
worker := client.NewConsumer("orders", "order_workers",
    pgque.WithSubconsumer("worker-1"),
    pgque.WithDeadInterval(2*time.Minute), // optional: steal stale batches
)
worker.Handle("order.created", func(ctx context.Context, m pgque.Message) error {
    return processOrder(ctx, m)
})
go worker.Start(ctx)

// On a separate process / goroutine:
worker2 := client.NewConsumer("orders", "order_workers",
    pgque.WithSubconsumer("worker-2"))
worker2.Handle("order.created", processOrderHandler)
go worker2.Start(ctx)
```

`receive_coop` auto-registers the cooperative main and the subconsumer
on first poll, so an explicit `SubscribeSubconsumer` is not required.
Heartbeats are not auto-emitted; call `client.TouchSubconsumer(...)`
manually if you want to advertise liveness ahead of a dead-interval
takeover by another worker.

Low-level API (matches the SQL one-for-one):

| Method | Wraps |
| --- | --- |
| `Client.SubscribeSubconsumer(ctx, q, c, sc)` | `pgque.subscribe_subconsumer` |
| `Client.UnsubscribeSubconsumer(ctx, q, c, sc, opts...)` | `pgque.unsubscribe_subconsumer` |
| `Client.ReceiveCoop(ctx, q, c, sc, opts...)` | `pgque.receive_coop` |
| `Client.TouchSubconsumer(ctx, q, c, sc)` | `pgque.touch_subconsumer` |

Options:

| Option | Notes |
| --- | --- |
| `WithSubconsumer(name)` | High-level `Consumer`: enables coop mode. |
| `WithDeadInterval(d)` | High-level `Consumer`: passes a takeover window to `ReceiveCoop`. |
| `WithCoopMaxMessages(n)` | `ReceiveCoop` per-call row cap (default 100). |
| `WithCoopDeadInterval(d)` | `ReceiveCoop` takeover window for one call. |
| `WithBatchHandlingRetry()` | `UnsubscribeSubconsumer`: route active batch through retry/DLQ instead of erroring. |

Runnable demo: [`clients/go/bench/coop_demo`](bench/coop_demo) — two
workers under one logical consumer printing per-message dispatch lines
and a final disjoint-delivery summary.

## Transactions

`Send` → ticker → `Receive` must each run in its own committed transaction (PgQue is snapshot-based). `pgxpool` satisfies this transparently — every `Send`/`Receive`/`Ack` is its own implicit tx, and the `Consumer` is pool-level.

The footgun is `Client.Pool()`: calling `pgque.send` inside your own `pgx.Tx` is fine for transactional enqueue, but the consumer must run after `tx.Commit()`. Don't wrap `pgque.send` and `pgque.receive` in one shared `pgx.Tx`; same for `pgque.maint_retry_events` + `pgque.ticker`. See [snapshot rule](https://github.com/NikolayS/pgque/blob/main/docs/concepts.md#the-snapshot-rule).

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
