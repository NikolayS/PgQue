# PgQue clients

PgQue ships three first-party clients. They are thin wrappers over `pgque.*`
SQL primitives. The matrix below tracks the public client API on current
`main`.

## Current parity matrix

| Capability | Python | Go | TypeScript |
| --- | :---: | :---: | :---: |
| `connect` / `close` | ✓ | ✓ | ✓ |
| Raw SQL escape hatch | ✓ (`conn`) | ✓ (`Pool()`) | ✓ (`rawPool`) |
| PgQue-classified errors | ✓ | ✗ | ✓ |
| Lossless PostgreSQL `bigint` IDs | ✓ (`int`) | ✓ (`int64`) | ✓ (`bigint`) |
| `send` | ✓ | ✓ | ✓ |
| `send_batch` / `SendBatch` / `sendBatch` | ✓ | ✓ | ✓ |
| `receive` | ✓ | ✓ | ✓ |
| `ack` returns SQL rowcount (0 stale, 1 success) | ✓ (int) | ✓ (int64) | ✓ (number) |
| `nack` | ✓ | ✓ | ✓ |
| `force_next_tick` / `ForceNextTick` / `forceNextTick` | ✓ | ✓ | ✓ |
| `nack` retry delay + reason options | ✓ | ✗ | ✓ |
| High-level `Consumer` | ✓ | ✓ | ✓ |
| Consumer wakeup model | polling + optional LISTEN/NOTIFY wakeup | polling | polling |
| `Consumer` poll interval option | ✓ | ✓ | ✓ |
| `Consumer` max-messages option | ✓ | ✗ | ✓ |
| `Consumer` retry delay option | ✓ | ✗ | ✗ |
| Unknown-type behavior avoids silent ack | ✗ | ✓ | ✓ |
| Configurable unknown-type policy | ✗ | ✗ | ✗ |
| `subscribe` / `unsubscribe` wrappers | ✗ | ✗ | ✓ |
| Cooperative consumers (experimental) [^coop] | ✗ | ✗ | ✓ |

Legend: ✓ supported by the client API on `main`; ✗ not exposed as a
first-class client API. Lower-level SQL primitives remain available through raw
connection/pool escape hatches. TypeScript currently exposes an extra
convenience wrapper for `ticker`; Python and Go can call it via raw SQL.

[^coop]: Experimental. TypeScript exposes `subscribeSubconsumer`,
    `unsubscribeSubconsumer`, `receiveCoop`, `touchSubconsumer`, and a
    `subconsumer` / `deadInterval` option on `newConsumer`. Function names
    and edge-case behavior may change before the feature is marked stable.
