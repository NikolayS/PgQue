# PgQue comparison notes

This document keeps the long-form positioning material out of the README.

## Short version

PgQue is for teams that care about:

- managed Postgres compatibility
- language-agnostic SQL API
- sustained-load behavior
- avoiding MVCC churn on the hot queue path

It is not for teams that primarily need:

- sub-10ms dispatch latency
- ecosystem-specific DX above all else
- workflow orchestration instead of queue semantics

## PgQue vs PGMQ

PGMQ has a more familiar per-message mental model.

But it still lives in the `skip locked` / row lifecycle family, which means
heap churn, index churn, and VACUUM dependence under sustained load.

PgQue's core argument is that TRUNCATE-based rotation is a better long-run
storage model for a queue inside Postgres.

## PgQue vs River

River is excellent for Go-native job processing and app-integrated job flows.

PgQue is more language-agnostic and more focused on queue/event architecture
than on Go-first developer experience.

## PgQue vs pg-boss / graphile-worker / Oban / solid_queue

These systems are good productized job queues, but they still live in the same
broad family of row claiming and row lifecycle management.

PgQue is different because its main value is **structural immunity on the hot
path** to the dead-tuple treadmill.

## PgQue vs workflow engines

PgQue is not a workflow engine.

If you need long-running, branching, durable execution, look at systems such as
Temporal, Restate, Inngest, Hatchet, or other workflow platforms.

If you need a queue with good Postgres operational behavior, PgQue is the right
shape.
