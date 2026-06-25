# Partition-keys reproduction

A runnable spike for the partition-keys design
(`blueprints/partition-keys/SPEC.md`), aligned to the **two cases** from the
original thread. It installs pgque on a throwaway Postgres, drives each case
with concurrent producers/workers, **measures**, and **checks the guarantees
empirically** — so we test the design instead of arguing about it.

TypeScript on **bun** + `pg` (node-postgres), matching `clients/typescript/`.
Everything consumer/producer-side is a thin recipe in `schema.sql`; the engine
is unchanged.

## The two cases (and what each one actually needs)

These are **two different features**, not two tiers of one. Case 2 is the
partition-keys feature; Case 1 is a *plain-queue* recipe that doesn't use
partition slots at all.

**Case 1 — migrations: producer idempotency + consumer mutual exclusion**
(`--tier a`, `--tier hazard`). On a **plain queue** (no slots, no fixed N).
Fabrizio's ask has **two distinct guarantees**, *complementary layers*:

| Layer | Guarantee | Mechanism | Prevents |
|-------|-----------|-----------|----------|
| **L1 producer** | idempotency | TTL dedup window (`demo.send_idem`) | duplicate **insert** (log bloat, no-op enqueues) |
| **L2 consumer** | one job at a time per key | cooperative consumers + per-key `pg_try_advisory_xact_lock` | duplicate **work** |

L1 is the producer-side TTL dedup (the SQS/NATS model — its own send-layer
feature, spec'd in `blueprints/idempotency/`). L2 is mutual exclusion on the
consume side. **They are not substitutes** — L1 keeps the log small; L2
guarantees correctness even if a duplicate slips in. Neither needs the
partition-keys (slot) feature: the "key" here is a *lock key*, not a partition.

> **Key-scope footgun (`--tier hazard`).** A TTL dedup keyed on the *entity*
> (`migrate:tenant`) silently drops a *needed* job when the desired effect
> changes inside the window — ship migration v1, then v2 within the TTL, and v2
> is suppressed as a "duplicate". The idempotency key must represent the desired
> **effect**, not just the entity: `migrate:tenant:vN`. The guardrail test
> reproduces both.

**Case 2 — lifecycle events: ordered per key** (`--tier b`). *This* is the
partition-keys feature. Ordered per tenant, parallel across tenants: N
hash-routed slot subscriptions filtering via `get_batch_cursor` `extra_where`.
Checks G1 (per-key FIFO + single-slot affinity) + exactly-once, and measures
read amplification (~N) — the honest cost, plus rotation-pin risk (the slowest
slot lowers the rotation floor; see SPEC R7).

## Run it on a fresh Linux VM

```bash
# Debian/Ubuntu VM, from inside the pgque repo:
sudo bash blueprints/partition-keys/repro/run.sh
```

`run.sh` installs Postgres + bun, creates `pgque_repro` and a peer-auth role,
installs `sql/pgque.sql` + `schema.sql`, `bun install`s, then runs Case 1
(both with and without producer dedup) and Case 2, each ending in `PASS`/`FAIL`
invariant lines.

### Run pieces directly / change scale

```bash
cd blueprints/partition-keys/repro
export PGHOST=/var/run/postgresql PGDATABASE=pgque_repro PGUSER=$(id -un)

# Case 1 with producer dedup ON, 8 producers racing 5000 tenants:
bun driver.ts --tier a --tenants 5000 --producers 8 --dups 4 --dedup-ttl 60 --workers 16

# Case 2, 16 slots:
bun driver.ts --tier b --tenants 1000 --events-per-tenant 30 --slots 16

# Dedup key-scope guardrail (entity-only vs effect-scoped key):
bun driver.ts --tier hazard --tenants 500 --dedup-ttl 300
```

Knobs (env for `run.sh`, flags for `driver.ts`):

- Case 1: `A_TENANTS`/`--tenants`, `A_PRODUCERS`/`--producers` (concurrent
  producers racing the same tenants), `A_DUPS`/`--dups` (attempts each),
  `A_DEDUP_TTL`/`--dedup-ttl` (seconds; `0` = producer dedup off),
  `A_WORKERS`/`--workers`, `A_WORK_MS`/`--work-ms`.
- Case 2: `B_TENANTS`/`--tenants`, `B_EPT`/`--events-per-tenant`,
  `B_SLOTS`/`--slots` (= N = worker count, static assignment).

## What the reports show

**Case 1**
- `[L1 producer] attempts` vs `INSERTED` — with dedup ON, thousands of
  concurrent "migrate tenant T" attempts collapse to one insert per tenant
  (the literal idempotency ask). With dedup OFF, all are inserted.
- `[L2 consumer] jobs RUN` vs `ack-dropped` — exactly one run per tenant either
  way; residual duplicates are dropped at consume.
- Invariants: producer dedup → inserted == distinct tenants; no two workers ran
  the same key with overlapping windows; every tenant migrated exactly once.

**Case 2**
- `read amplification` = measured `scanned/delivered`, ~`N` (every slot scans
  the full window) — the cost of strict ordering, quantified.
- Invariants: each key on exactly one slot (affinity), non-decreasing `ev_id`
  per key (FIFO), nothing lost or duplicated.

## Caveats (it's a spike)

- One-shot: produce-then-drain, not steady-state. Throughput is indicative, not
  a tuned benchmark; producer throughput is per-row round trips, not bulk.
- Case 1 processes a whole cooperative batch in one transaction (the per-key
  advisory lock is `xact`-scoped), so the visible processing window equals the
  batch — fine for checking exclusion, not for latency.
- Case 2 uses static `worker k → slot k`; the dynamic claim variants
  (advisory / `SKIP LOCKED`) from SPEC §15 are not exercised here.
- The L1 TTL table is GC'd by expiry; a real install would tie the window to
  rotation (see the idempotency design note).
