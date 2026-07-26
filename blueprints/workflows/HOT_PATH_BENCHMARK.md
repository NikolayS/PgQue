# PgQue durable workflows hot-path benchmark

Status: design for the first verification harness. Do not treat this as a
durable-workflow implementation.

## Question

Can PgQue preserve its batching amortization when workflow progress is modeled
as continuation events?

The claim under test is narrow:

> A consumer can take N workflow step-events, append N successor step-events,
> and advance the PgQue subscription once, without producing one dead tuple per
> workflow transition.

This benchmark intentionally excludes `await`, `join`, timers, cancellation,
signals, SDKs, and real workflow semantics. Those are later tests. This one
tests the hot-path tuple-churn model before building anything expensive.

## Hypothesis

For await-light continuation-passing workflows:

- event tables rotate or truncate and keep `n_dead_tup` flat;
- the subscription table sees roughly one update per batch, not per step;
- throughput is dominated by successor inserts plus one batch ack;
- a mutable `workflow_status` baseline accumulates dead tuples proportional to
  transition count.

If this does not hold clearly, the event-sourced durable-workflow thesis should
be killed or narrowed before implementation.

## Workloads

### A. Mutable status baseline

One row per workflow instance:

```sql
create table workflow_status (
  workflow_id uuid primary key,
  step_seq bigint not null,
  payload jsonb not null default '{}'::jsonb
);
```

Each transition performs:

```sql
update workflow_status
set step_seq = step_seq + 1
where workflow_id = $1;
```

This is the DBOS/absurd-shaped hot path: one mutable workflow row touched per
transition.

### B. PgQue continuation hot path

Each workflow step is a PgQue event. The event payload carries only:

- `workflow_id`;
- `step_seq`;
- `step_name`;
- tiny continuation state.

Each batch performs:

```sql
begin;
  select pgque.next_batch(..., max_events := :batch_size) as batch_id;

  -- get N step-events from the batch
  -- append N successor events with step_seq + 1
  -- no workflow_status update, no wf_live update

  select pgque.finish_batch(batch_id);
commit;
```

Expected hot path:

- N inserts into PgQue's rotating event tables;
- one subscription update per batch;
- no per-workflow mutable-position update.

### C. Continuation + dedup

Same as B, plus one append into a short-horizon dedup table keyed by
`(workflow_id, step_seq)`.

Purpose: isolate whether dedup destroys the hot-path advantage.

### D. Continuation + high-resolution `wf_live`

Same as B, plus:

```sql
update wf_live
set step_seq = step_seq + 1,
    updated_at = clock_timestamp()
where workflow_id = $1;
```

Purpose: prove that exact per-step liveness is an opt-in tax. If this workload
behaves like A, `wf_live` must remain off the correctness path and off by
default.

## Metrics

Sample at fixed intervals:

- transitions/sec;
- batch size and batch latency p50/p95/p99;
- `pg_stat_user_tables.n_live_tup` and `n_dead_tup` for:
  - PgQue event tables;
  - PgQue subscription table;
  - mutable baseline table;
  - dedup table;
  - `wf_live`;
- `pg_total_relation_size()` and `pg_indexes_size()` for the same tables;
- WAL bytes/sec from `pg_stat_wal`;
- autovacuum activity from `pg_stat_user_tables`;
- CPU, disk write MiB/sec, and IOPS.

## Acceptance criteria

The event-sourced hot path is viable only if workload B shows:

- dead tuples in event tables remain flat through sustained load;
- subscription dead tuples grow with batch count, not transition count;
- table size is bounded by the active rotation window;
- throughput is materially better than A under the same transition rate and
  hardware;
- no hidden table becomes the real per-step update bottleneck.

Workload C is acceptable only if dedup overhead is bounded and does not turn the
design back into a status-row system.

Workload D is expected to pay per-step update cost. That cost must be documented
as the price of high-resolution observability, not hidden in the main claim.

## Kill criteria

Stop the durable-workflow hot-path thesis if:

- B does not beat A clearly on dead-tuple growth and sustained throughput;
- B still produces dead tuples proportional to transition count outside the
  subscription cursor;
- C collapses throughput or creates unbounded bloat;
- the only way to get useful observability requires D by default.

## Benchmark environment

Use the existing PgQue benchmark discipline:

- dedicated VM only, never the agent/home VM;
- same Postgres version, configuration, storage, and sampling cadence for all
  workloads;
- record hardware, Postgres settings, and exact commit SHA;
- export raw CSVs and charts with the PR.

The initial target should match the existing PgQue benchmark hardware before
claiming any public number.

## Relation to full workflows

Passing this benchmark proves only one thing: the simple step-to-step
continuation model can preserve PgQue's batching amortization.

It does not prove:

- `awaitEvent` correctness;
- fan-out/join correctness;
- timer behavior;
- cancellation/signal semantics;
- external side-effect idempotency;
- operator UX.

Those need separate tests. This benchmark exists first because if the hot path
does not work, the rest is academic.
