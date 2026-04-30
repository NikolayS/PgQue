# PgQue clients

PgQue ships three first-party clients. They are thin wrappers over `pgque.*`
SQL primitives. The matrix below tracks parity.

## v0.2.0 parity matrix

| Capability                                  | Python | Go  | TypeScript |
| ------------------------------------------- | :----: | :-: | :--------: |
| `connect` / `close`                         |   ✓    |  ✓  |     ✓      |
| `send`                                      |   ✓    |  ✓  |     ✓      |
| `send_batch`                                |   ✓    | TBD |    TBD     |
| `receive`                                   |   ✓    |  ✓  |     ✓      |
| `ack`                                       |   ✓    |  ✓  |     ✓      |
| `nack` with `retry_after` + `reason`        |   ✓    | TBD |     ✓      |
| `Consumer` with `max_messages`              |   ✓    | TBD |     ✓      |
| `Consumer` `unknown_handler` policy         |  TBD   | TBD |    TBD     |

Legend: ✓ shipped, TBD planned for v0.2.0, N/A not applicable to this driver.

Cells reflect what is on `main` today. Per-driver PRs are in flight to fill
the TBD rows for v0.2.0 parity.

## v0.2.1 follow-ups

Deferred audit items, not blocking the v0.2.0 parity-must-have set:

- #138 — remaining wrappers (`subscribe` / `unsubscribe` / `ticker` /
  `force_tick`) across all three drivers
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
