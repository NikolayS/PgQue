# PgQue clients

PgQue ships three first-party clients. They are thin wrappers over `pgque.*`
SQL primitives. The matrix below tracks the public client API on current
`main`.

## Release quality rules

These rules are part of the client release process. Do not cut a final client
release until they are satisfied or explicitly documented as intentionally
out-of-scope for that release.

### API parity is required

First-party clients must expose the same public PgQue capabilities in idiomatic
language shape. If Python exposes a normal consumer primitive, Go and
TypeScript should expose it too; if TypeScript has a ticker helper, Python and
Go should have equivalent helpers. Drift is acceptable only when it is a
conscious product decision and recorded here or in the release issue.

Implementation details may differ. For example, Python can use LISTEN/NOTIFY as
a wakeup optimization while Go and TypeScript poll, but the public behavior and
failure semantics must remain equivalent.

Before a final release:

- update the parity matrix below;
- remove every accidental `✗` or convert it into a documented, intentional
  exception;
- verify each new or changed public method has tests in every affected client;
- keep experimental features, especially cooperative consumers, clearly marked
  experimental in all client READMEs.

### Benchmarks must be current before final

Do not quote old client performance numbers in release notes or docs. Before a
final release, rerun the producer benchmarks against current SQL and client
code, especially after changes to `send_batch`, payload encoding, transaction
handling, or driver setup.

Benchmark entry points:

- Python: `clients/python/bench_producer.py`
- Go: `clients/go/producer_benchmark_test.go`
- TypeScript: `clients/typescript/src/producer_bench.ts`

If benchmark results are published, update the chart and state the environment
used for the run. Stale charts are worse than no charts; they give users false
confidence, which is how benchmarks become marketing-shaped lies.

### Testing gates

For any client release candidate or final release:

- run the repo CI client jobs for all first-party clients;
- run package build / pack smoke tests for each ecosystem;
- verify install from the real package index for published RCs before final;
- run at least one clean smoke install for final packages:
  - `pip install pgque-py`
  - `npm install pgque`
  - `go get github.com/NikolayS/pgque-go@vX.Y.Z`
- verify pkg.go.dev renders Go docs and recognizes the license;
- verify npm dist-tags: prereleases use `rc`/`next`, final uses `latest`;
- verify PyPI/TestPyPI and npm/GitHub release automation before relying on it.

### Release documentation

Release prep PRs must update user-facing install commands and remove stale
prerelease wording. When publishing a final release, docs must stop telling
users to install `--pre`, `@rc`, or an `-rc` Go tag.

## Current parity matrix

| Capability | Python | Go | TypeScript |
| --- | :---: | :---: | :---: |
| `connect` / `close` | ✓ | ✓ | ✓ |
| Raw SQL escape hatch | ✓ (`conn`) | ✓ (`Pool()`) | ✓ (`rawPool`) |
| PgQue-classified errors | ✓ | ✓ | ✓ |
| Lossless PostgreSQL `bigint` IDs | ✓ (`int`) | ✓ (`int64`) | ✓ (`bigint`) |
| `send` | ✓ | ✓ | ✓ |
| `send_batch` / `SendBatch` / `sendBatch` | ✓ | ✓ | ✓ |
| `receive` | ✓ | ✓ | ✓ |
| `ack` returns SQL rowcount (0 stale, 1 success) | ✓ (int) | ✓ (int64) | ✓ (number) |
| `nack` | ✓ | ✓ | ✓ |
| `ticker` / `Ticker` / `ticker`, `ticker_all` / `TickerAll` / `tickerAll` | ✓ | ✗ | ✓ |
| `force_next_tick` / `ForceNextTick` / `forceNextTick` | ✓ | ✓ | ✓ |
| `nack` retry delay + reason options | ✓ | ✓ | ✓ |
| High-level `Consumer` | ✓ | ✓ | ✓ |
| Consumer wakeup model | polling + optional LISTEN/NOTIFY wakeup | polling | polling |
| `Consumer` poll interval option | ✓ | ✓ | ✓ |
| `Consumer` max-messages option | ✓ | ✓ | ✓ |
| `Consumer` retry delay option | ✓ | ✗ | ✗ |
| Unknown-type behavior avoids silent ack | ✓ | ✓ | ✓ |
| Configurable unknown-type policy | ✓ | ✓ | ✓ |
| `subscribe` / `unsubscribe` wrappers | ✓ | ✗ | ✓ |
| Cooperative consumers (experimental) [^coop] | ✓ | ✓ | ✓ |

Legend: ✓ supported by the client API on `main`; ✗ not exposed as a
first-class client API. Lower-level SQL primitives remain available through raw
connection/pool escape hatches. Python and TypeScript currently expose ticker
convenience wrappers; Go can call ticker functions via raw SQL until parity is
complete.

[^coop]: Experimental. Each supporting client exposes
    `subscribe_subconsumer` / `unsubscribe_subconsumer` / `receive_coop` /
    `touch_subconsumer` (idiomatic case per language) and a `subconsumer` /
    `dead_interval` option on the high-level consumer. Function names and
    edge-case behavior may change before the feature is marked stable. See
    each client's README for details.
