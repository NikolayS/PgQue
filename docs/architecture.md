# PgQue architecture

PgQue is a productization of PgQ for modern Postgres deployments.

## What it is

PgQue is **not** a reimplementation of a queue from scratch.

It takes PgQ's proven architecture and makes it usable in environments where
custom C extensions and external daemons are not acceptable.

## Core architectural ideas

### 1. Snapshot-based batch isolation

Each batch contains exactly the events committed between two ticks.

This gives PgQue a clean batch boundary and avoids the claim/delete lifecycle
used by most Postgres-native queues.

### 2. TRUNCATE-based table rotation

PgQue uses three rotating event tables per queue.

Instead of deleting consumed rows one by one, it rotates tables and clears old
ones with `TRUNCATE`. That means:

- no dead tuples on the hot event path
- no sustained VACUUM pressure from normal consumption
- stable behavior under load

### 3. Multiple independent consumers

Each consumer tracks its own position. One queue can feed many consumers.

## Why this differs from most Postgres queues

Most Postgres-native queue systems rely on some version of:

- `for update skip locked`
- row state changes
- deletes or updates on completion

That works, but under sustained load it creates heap and index churn, dead
tuples, and growing dependence on VACUUM keeping up.

PgQue trades lower-latency single-message ergonomics for a better long-run
storage model.

## Managed Postgres compatibility

PgQue is designed for major managed Postgres platforms including:

- RDS
- Aurora
- AlloyDB
- Cloud SQL
- Supabase
- Neon
- Crunchy Bridge

`pg_cron` is recommended. If it is unavailable or unsuitable, run
`pgque.ticker()` and `pgque.maint()` from an external scheduler.

## Project structure

PgQue has two layers:

- **pgque-core** — transformed and modernized PgQ internals
- **pgque-api** — convenience API (`send`, `receive`, `ack`, `nack`, DLQ,
  delayed delivery, observability)

That distinction matters. The core is proven; the API layer is where semantic
clarity matters most.
