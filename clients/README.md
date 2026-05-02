# PgQue clients

PgQue ships three first-party clients. They are thin wrappers over `pgque.*`
SQL primitives. The matrix below tracks the public client API on current
`main`.

## Current parity matrix

| Capability | Python | Go | TypeScript |
| --- | :---: | :---: | :---: |
| `connect` / `close` | ✓ | ✓ | ✓ |
| `send` | ✓ | ✓ | ✓ |
| `send_batch` / `SendBatch` / `sendBatch` | ✓ | ✓ | ✓ |
| `receive` | ✓ | ✓ | ✓ |
| `ack` | ✓ | ✓ | ✓ |
| `nack` | ✓ | ✓ | ✓ |
| `nack` retry delay + reason options | ✓ | ✗ | ✓ |
| High-level `Consumer` | ✓ | ✓ | ✓ |
| `Consumer` max-messages option | ✓ | ✗ | ✓ |
| Unknown-type behavior avoids silent ack | ✗ | ✓ | ✓ |
| Configurable unknown-type policy | ✗ | ✗ | ✗ |
| `subscribe` / `unsubscribe` wrappers | ✗ | ✗ | ✓ |
| `ticker(queue?)` / `force_tick(queue)` wrappers | ✗ | ✗ | ✓ |

Legend: ✓ supported by the client API on `main`; ✗ not exposed as a
first-class client API. Callers can still use raw SQL through the underlying
connection/pool for SQL primitives that do not yet have wrappers.

See #146 for the cross-driver audit umbrella.
