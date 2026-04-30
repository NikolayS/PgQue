# PgQue clients

PgQue ships three first-party clients. They are thin wrappers over `pgque.*`
SQL primitives. The matrix below tracks parity.

## v0.2.0 parity matrix

Status as of v0.2.0 (assumes PRs #153, #154, #155 merge as planned). Cells
marked "main" are already on `main`; the rest land via the in-flight
per-driver PRs that ship together in v0.2.0.

| Capability                                   | Python                  | Go                          | TypeScript                  |
| -------------------------------------------- | :---------------------: | :-------------------------: | :-------------------------: |
| `connect` / `close`                          | ✓                       | ✓                           | ✓                           |
| `send`                                       | ✓                       | ✓                           | ✓                           |
| `send_batch`                                 | ✓                       | ✓ (via #153)                | ✗ deferred to v0.2.1 (#138) |
| `receive`                                    | ✓                       | ✓                           | ✓                           |
| `ack`                                        | ✓                       | ✓                           | ✓                           |
| `nack` with `retry_after` + `reason`         | ✓                       | ✓ (via #153 `NackOption`s)  | ✓                           |
| `Consumer` `max_messages` option             | ✓                       | ✓ (via #153 `WithMaxMessages`) | ✓ (via #154)             |
| `Consumer` `unknown_handler` policy          | ✓ (via #155)            | ✓ (via #153 `WithUnknownHandlerPolicy`) | ✓ (via #154)    |
| `subscribe` / `unsubscribe`                  | ✗ deferred to v0.2.1 (#138) | ✗ deferred to v0.2.1 (#138) | ✓                       |
| `ticker(queue?)` / `force_tick(queue)`       | ✗ deferred to v0.2.1 (#138) | ✗ deferred to v0.2.1 (#138) | ✓                       |

Legend: ✓ ships in v0.2.0, ✗ deferred (see linked issue), N/A not applicable
to this driver.

See #146 for the cross-driver audit umbrella.
