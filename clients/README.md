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

## v0.2.1 follow-ups

Deferred audit items, not blocking the v0.2.0 parity-must-have set:

- #138 — remaining wrappers (`subscribe` / `unsubscribe` / `ticker` /
  `force_tick`) for Python and Go; TypeScript `send_batch`
- #140 — typed errors taxonomy aligned across drivers
- #141 — split `send_text` and `send_json` (or define a single `send` shape)
- #143 — handling of NULL event `type` and NULL `payload`
- #145 — TypeScript `bigint` parser for `event_id` / `batch_id`
- #147 — TypeScript `LISTEN` / `NOTIFY` integration
- #148 — `ack` no-op result semantics
- #150 — transactional consumer mode
- #151 — TypeScript ticker return values

## Related issues

- #146 — clients v0.2.0 umbrella
- #144 — cross-driver API parity matrix (this doc closes the docs portion)
