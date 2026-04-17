# Benchmarks

Preliminary results on a laptop (Apple Silicon, 10 cores, 24 GiB RAM,
PostgreSQL 18.3, `synchronous_commit=off` per-session). Full methodology:
[NikolayS/pgq#1](https://github.com/NikolayS/pgq/issues/1).

| Scenario | Throughput | Per core |
|---|---|---|
| PL/pgSQL single insert/TX, ~100 B, 16 clients | **85,836 ev/s** | ~8.6k ev/s |
| PL/pgSQL batched 100k/TX, ~100 B | 80,515 ev/s | ~8.1k ev/s |
| PL/pgSQL batched 100k/TX, ~2 KiB | 48,899 ev/s (91.5 MiB/s) | ~4.9k ev/s |
| Consumer read rate, 100k batch, ~100 B | ~2.4M ev/s | ~240k ev/s |
| Consumer read rate, 100k batch, ~2 KiB | ~305k ev/s (568 MiB/s) | ~30.5k ev/s |

Key takeaways:

- **Zero bloat under sustained load** — a 30-minute sustained test showed
  zero dead tuple growth in event tables.
- **Batching matters** — throughput jumps hard when you stop doing one tiny
  transaction per event.
- **Consumer side is not the bottleneck** — reads are much faster than writes.
- **You keep Postgres guarantees** — transactional semantics, WAL durability
  options, backups, replication, SQL introspection.

> `synchronous_commit=off` can be set per session or per transaction for
> queue-heavy workloads if that trade-off makes sense for your system.

These numbers are from a single laptop and are preliminary; server-class
results will be posted as they become available.
