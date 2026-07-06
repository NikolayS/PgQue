# Partition-keys read-amplification benchmark

Exercises the pgque v0.8 **partition keys** feature (`sql/pgque-api/partition_keys.sql`)
at a Supabase-Storage-like scale — the read-amplification scenario in
`blueprints/partition-keys/SPEC.md` §14 (S4). It uses **only the real installed
pgque API** (`send` keyed / `subscribe_slot` / `claim_slot` /
`receive_partitioned` / `ack_partitioned` / `release_slot` and the
`pgque.partition_slot_status` view) — no demo schema.

## What it measures

The slot mechanism is N independent slot consumers, each scanning the full
event stream and filtering server-side to its hash class. That gives two
properties this bench quantifies:

- **R2 — read amplification ~N×.** Each produced event is scanned by all N
  slots, so buffers touched per produced event scale ~linearly with N. Measured
  from `pg_stat_statements` (under `track = top`, the event-table scan buffers
  roll up into the top-level `receive_partitioned` call), at **N=16 vs N=32**.
- **R7 — a stalled slot pins rotation for the whole queue.** One slot's worker
  is SIGSTOPped mid-run; its subscription cursor freezes, so the engine cannot
  drop old `event_N_M` tables. The bench tracks per-slot lag growth, the
  rotation floor (queue table count), and the catch-up slope after resume.

## Target profile (Fabrizio: >400M events/day)

- **Producer:** 5,000 ev/s sustained keyed `send`, Zipfian tenant skew (s=1.1)
  over 2,000 tenants, ~200-byte JSON payload — see `producer.sql`. Rate-limited
  pgbench (`-R 5000 -c 16 -j 8`).
- **Consumers:** N slot workers (one per slot) on the lease loop — `slot_worker.ts`
  (bun + node-postgres). Each worker claims its slot, sticky-drains it in
  batches of 500, acks, releases at drain, re-polls after 200 ms idle. One ack
  log line per batch: `ts,worker,slot,events,max_ev_id`.
- **Ticker:** `pk_ticker.py` — `pgque.ticker()` every 250 ms, `pgque.maint()`
  every 60 s.

## Phases

`run_bench.sh` runs, all output under `/tmp/bench/pk/<phase>/`:

1. **steady-16** (30 min): `bench_q`, consumer `w16`, 16 slots, 5k ev/s.
2. **steady-32** (30 min): fresh `bench_q32`, consumer `w32`, 32 slots, 5k ev/s.
3. **stalled-16** (15 min): reuse `bench_q`; slot 7's worker is SIGSTOPped at
   minute 2 and resumed at minute 10.

Each phase runs, at 5 s / 10 s / 30 s cadences: `slot_status_sampler.sh`
(per-slot lease + lag from `partition_slot_status`, and queue-level throughput
+ table count from `get_queue_info`), `sys_metrics_sampler.py`,
`pg_stat_statements_snapshot.py`, and `bloat_sampler.py`. `pg_stat_statements`
is reset at each phase start and snapshotted at the boundary for the read-amp
measurement.

Then `summarize.py` parses the CSVs into `summary.md` — producer/consume
throughput, per-slot pending percentiles, CPU/mem, the N-scaling read-amp
table, the stalled-slot timeline, and a headline table to paste into a PR.

## Running

On a fresh Hetzner CCX43 (16 dedicated cores, 64 GiB, local NVMe, Ubuntu 24.04),
Postgres 18 from PGDG:

```bash
# from the operator machine: ship the repo, then bootstrap
rsync -a --exclude .git ./ root@VM:/root/pgque/
ssh root@VM 'bash /root/pgque/benchmark/partition-keys/setup_vm.sh'

# on the VM: install driver deps and run the full ~90 min of measured phases
ssh root@VM 'cd /root/pgque/benchmark/partition-keys && bun install && \
  PGUSER=postgres DEVICE=sda bash run_bench.sh'
```

`DEVICE` is the block device name in `/proc/diskstats` (Hetzner cloud volumes
usually show as `sda`; adjust if the NVMe is named differently).

### Knobs (env)

`RATE` (5000), `N_SLOTS` (16; steady-32 uses 2×), `DURATION_MIN` (30),
`STALL_MIN` (15), `STALL_ON_MIN`/`STALL_OFF_MIN`/`STALL_SLOT`, `ROTATION`
(`30 seconds`), `BATCH` (500), `TTL_S` (30), `PGB_C`/`PGB_J`, `PHASES`
(`1,2,3`), `OUT` (`/tmp/bench/pk`), and libpq `PGHOST`/`PGUSER`/`PGDATABASE`.

### Smoke run

A 60-second micro-run of phase 1 at 200 ev/s with 4 slots — just env vars:

```bash
PGHOST=/tmp PGUSER="$(id -un)" PGDATABASE=bench \
  RATE=200 N_SLOTS=4 DURATION_MIN=1 PHASES=1 DEVICE=none \
  bash run_bench.sh
```

## Files

| File | Role |
|---|---|
| `setup_vm.sh` | VM bootstrap: PG18 + tuning + bun + pgque install |
| `producer.sql` | pgbench keyed-send script (`@QUEUE@` rendered at runtime) |
| `slot_worker.ts` | bun slot-worker lease-loop driver |
| `package.json` | driver deps (`pg`) |
| `pk_ticker.py` | ticker + maint loop |
| `slot_status_sampler.sh` | per-slot lease/lag + queue-rate sampler |
| `run_bench.sh` | phase orchestrator |
| `summarize.py` | CSV → markdown report |
