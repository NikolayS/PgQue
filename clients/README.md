# PgQue clients

Driver libraries for [PgQue](https://github.com/NikolayS/pgque) — the
PgQ-based universal PostgreSQL queue. Each client is a thin 1:1 wrapper
over the `pgque-api` SQL surface (`send`, `receive`, `ack`, `nack`,
`subscribe`, `unsubscribe`, plus `ticker` / `force_tick` for tests) with
an idiomatic high-level `Consumer` poll loop layered on top.

| Driver | Path | Status |
|---|---|---|
| Python | [`python/`](./python/) | v0.2.0 |
| Go | [`go/`](./go/) | v0.2.0 |
| TypeScript | [`typescript/`](./typescript/) | v0.2.0 |

## Cross-driver capability matrix (v0.2.0)

This table captures the runtime contract every driver must honour. The
SQL surface is shared; the differences below are language-idiom shaping
(e.g. variadic options vs. keyword args), never differences in semantics
against the underlying `pgque.*` functions.

| Capability | Python | Go | TypeScript |
|---|---|---|---|
| `send(queue, payload, type=...)` -> `id` | yes | yes (`Send`) | yes |
| `send_batch(queue, type, payloads)` -> `[]id` | yes | yes (`SendBatch`) | deferred (#141) |
| `receive(queue, consumer, max)` | yes | yes | yes |
| `ack(batch_id)` | yes | yes | yes |
| `nack(batch_id, msg, retry_after, reason)` | yes (kwargs) | yes (variadic options) | yes (opts object) |
| `subscribe(queue, consumer)` | yes | deferred (#138) | yes |
| `unsubscribe(queue, consumer)` | yes | deferred (#138) | yes |
| `ticker()` / `ticker(queue)` | via raw SQL | deferred (#138) | yes (overload) |
| `force_tick(queue)` | via raw SQL | deferred (#138) | yes |
| Consumer: dispatch by type | yes | yes | yes |
| Consumer: nack on handler error | yes | yes | yes |
| Consumer: nack-default for unknown types | **yes** | **yes** | **yes** |
| Consumer: opt-in ack-on-unknown | `unknown_handler="ack"` | `WithUnknownHandlerPolicy(AckUnknown)` | `unknownHandlerPolicy: 'ack'` |
| Consumer: skip batch ack if any nack fails | yes (tx rollback) | yes (`nackFailed` flag) | yes (`nackFailed` flag) |
| Consumer: default `max_messages` | `500` | `500` | `500` |
| Top-level `undefined`/`None` payload -> JSON null | yes | yes | yes |

The data-safety contract is fixed: by default no driver may silently
drop a message because of a missing handler or a Nack failure. The
`ack`-policy opt-in (and, in Python, registering a `*` catch-all) is the
only way to discard unhandled types on purpose.

## Deferred to v0.2.1+

The following items are tracked for a follow-up release:

- #138 (Go): `Subscribe`, `Unsubscribe`, `Ticker`, `ForceTick` wrappers
- #140: typed errors for `consumer not found` / DLQ paths in all drivers
- #141 (TS): `send_text` / `send_json` convenience wrappers
- #143: NULL `type` / NULL `payload` round-trips
- #145 (TS): scoped `bigint` parser instead of process-global mutation
- #147 (TS): native `LISTEN` wakeup for the consumer
- #148: `ack` no-op vs. real-finish result code surfaced by drivers
- #150: transactional consumer (single-tx `receive`+handler+ack pattern)
- #151 (TS): expose `pgque.ticker()` return value (events ticked count)

See the GitHub issues for design notes.

## License

Apache-2.0. Copyright 2026 Nikolay Samokhvalov.
