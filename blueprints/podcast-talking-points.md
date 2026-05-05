# PgQue podcast talking points (45 min)

Internal prep, not user-facing docs. Goal: a host with a Postgres-savvy
audience can run a 45-minute conversation from this without prep on their end.

## One-line pitch

> PgQ — the Skype-era Postgres queue that ran messaging for hundreds of
> millions of users — repackaged as one SQL file. No C extension, no
> daemon, runs on RDS / Aurora / Cloud SQL / Supabase / Neon out of the
> box. Zero bloat, by construction.

## The hook (open with one of these)

1. **The death spiral.** "Every Postgres queue tutorial uses `SKIP LOCKED`.
   That works in a demo. Brandur at Heroku watched it pile up a 60k backlog
   in an hour. PlanetScale watched it die at 800 jobs/sec. River has an
   open issue on autovacuum starvation. We've been collectively rebuilding
   the same broken queue for a decade — and there has been a working
   alternative inside Postgres since 2007 that almost nobody uses."
2. **Skype's queue.** "The largest VoIP service of the 2000s ran its
   messaging on a Postgres queue called PgQ. It's still maintained. Almost
   nobody outside that lineage uses it, because installing it requires a C
   extension and a daemon — and your managed Postgres provider won't let
   you install either. So we rebuilt the engine in pure SQL."
3. **The anti-extension.** "What if a Postgres extension wasn't an
   extension? No `.control` file, no `shared_preload_libraries`, no
   restart, no provider approval. Just `\i pgque.sql` and you have a
   queue. That's the whole shape of this project."

## 45-minute arc

| Block | Minutes | Topic |
|---|---|---|
| 1 | 0–5 | Cold open + one-line pitch |
| 2 | 5–12 | The bloat problem (why every Postgres queue gets sick) |
| 3 | 12–22 | PgQ's actual architecture (snapshot batching + rotation) |
| 4 | 22–30 | The "anti-extension" repackage (what PgQue adds) |
| 5 | 30–37 | Trade-offs and when NOT to use it |
| 6 | 37–42 | Roadmap + ecosystem (clients, OTel, scheduled delivery) |
| 7 | 42–45 | Where to start, where to ask questions |

## Block 2 — Why every Postgres queue eventually gets sick (5–12)

The story arc here is: pattern → failure mode → receipts → why mitigations
don't fix the root cause.

- **The pattern.** A row per job. `SELECT … FOR UPDATE SKIP LOCKED` to
  claim. `UPDATE` to mark in-flight. `DELETE` on completion. Every blog
  post since ~2014.
- **The failure mode.** Every UPDATE and DELETE creates a dead tuple.
  Autovacuum reclaims dead tuples *only past the xmin horizon*. Anything
  that holds back the horizon — a long-running transaction, an idle-in-
  transaction connection, a lagging logical replication slot, a physical
  standby with `hot_standby_feedback=on` — pins the dead tuples in place.
  The queue table grows. Index scans get slower. The queue gets slower.
  Now the queue is the bottleneck. Now consumers fall behind. Now the
  backlog grows. Now the dead tuples grow faster. Death spiral.
- **Receipts.** Brandur (Heroku, 2015): 60k backlog in an hour.
  PlanetScale (2026): collapse at 800 jobs/sec when an OLAP query held
  xmin back. River issue #59: autovacuum starvation. Oban Pro shipped
  table partitioning to mitigate it. PGMQ ships aggressive autovacuum
  settings. These are mitigations of an architectural problem.
- **Why the mitigations don't fix it.** Partitioning bounds the blast
  radius but doesn't remove dead tuples. Tuned autovacuum still loses
  to a held xmin. The root cause is *the design*: claim-mark-delete
  produces dead tuples on the hot path.

Listener takeaway: **the bloat tax is not a tuning problem. It's a
design problem.**

## Block 3 — What PgQ actually does differently (12–22)

The most important block. Two ideas. Don't rush them.

### Idea 1: snapshot-based batching

Instead of "claim a row," PgQ uses Postgres's MVCC snapshot itself as the
cursor.

- A **tick** records `pg_current_snapshot()` — the set of transaction IDs
  visible right now.
- A **batch** is "all events in the gap between the last tick's snapshot
  and this tick's snapshot." That's a `WHERE xmin BETWEEN …` filter; no
  row claiming, no `SKIP LOCKED`, no UPDATE.
- Each consumer keeps a **cursor** — which tick it has last `ack`'d. Two
  consumers on the same queue see the same events independently.

So a "batch" is a virtual range over a shared event log, not a stash of
rows that have been mutated. That's why fan-out is free here — it's
literally the same scan with a different cursor.

### Idea 2: TRUNCATE-based rotation

The events table never gets DELETEd from. PgQ writes events into one of
**three rotating tables**. When the oldest table is older than every
consumer's cursor, it gets `TRUNCATE`d in one shot and reused.

- TRUNCATE produces zero dead tuples. The file is unlinked.
- No autovacuum dependency on the hot path.
- The xmin horizon can pin a snapshot for hours and the events table
  doesn't bloat — the dead tuples were never there to begin with.

Soundbite: **"the events table is a ring buffer, not a worklist."**

### Soundbites you can lift

- "Postgres already has snapshots. PgQ uses them as the queue."
- "The queue is a virtual cursor over a shared log. Not a worklist."
- "Autovacuum cannot fail to reclaim what was never written."
- "It's closer to Kafka topics than to RabbitMQ queues — inside Postgres."

## Block 4 — What PgQue adds on top (22–30)

PgQ has been around since 2007 and remains maintained. The product
question is: why don't more people use it? Answer: **packaging.**

- Standard PgQ ships as a **C extension** (`pgq`) plus an **external
  daemon** (`pgqd`) that drives the ticker. Most managed Postgres
  providers — RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon — do not
  let you install arbitrary C extensions or run sidecar daemons against
  your database. So 95% of the Postgres market has been excluded from
  PgQ for over a decade.

PgQue's shape:

1. **pgque-core** — mechanical repackage of PgQ. Renamed schema,
   modernized to PG14+ idioms (`xid8`, `pg_snapshot`,
   `pg_current_xact_id()` instead of the deprecated `txid_current()`),
   `SECURITY DEFINER` everywhere with pinned `search_path`, single-file
   install. ~4,000 lines of proven PL/pgSQL.
2. **pgque-api** — modern convenience layer on top: `send`, `receive`,
   `ack`, `nack`, dead-letter queue, batch send. Every API function must
   reduce cleanly to a PgQ primitive — that's a hard design rule. If
   `send()` can't be explained as "calls `insert_event` with these
   args," it's too complex.
3. **The pg_cron sub-second tick trick.** pg_cron's minimum schedule is
   1 second. PgQue's default tick rate is 100 ms. The ticker job loops
   inside a single 1-second pg_cron slot, calling `ticker()` and
   `commit`ing between iterations — each tick gets its own transaction
   so held-xmin is bounded by the tick period, not the slot. Tunable
   from 1 ms to 1000 ms; allowed values are exact divisors of 1000.

Demo-worthy moment: **`\i sql/pgque.sql` in any psql session and you have
a queue.** That's the whole installation. No reboot, no preload, no
provider ticket.

## Block 5 — Trade-offs and when NOT to use it (30–37)

This block is where you earn the audience. Don't oversell.

### The latency story

"Queue latency" is three numbers, not one.

| # | Name | PgQue | Comment |
|---|---|---|---|
| 1 | Producer (`send` → durable) | sub-ms | WAL flush bound |
| 2 | Subscriber (`next_batch` over a built batch) | sub-ms | snapshot SELECT |
| 3 | End-to-end (`send` → consumer visibility) | ≈ tick period (default 100 ms) | tunable, NOT load-dependent |

The big property: **end-to-end latency does not grow with load.** Under
pressure, batch size grows up to `queue_ticker_max_count`; e2e doesn't.
That's the opposite of `SKIP LOCKED` queues, where drain rate is
`batch_size / poll_interval` — when producers outrun that, queue depth
grows *and* e2e grows with it.

### When PgQue is wrong

Be honest about this:

- **You need single-digit-ms dispatch latency.** Default e2e is ~50 ms
  median. You can drive it down to ~5 ms with a 10 ms tick period, but
  if your SLA is "1 ms p99 from publish to handler," use Redis Streams
  or a dedicated broker.
- **You want a job queue framework, not an event log.** Per-job
  priorities, retries with exponential backoff per job class, cron
  scheduling, unique jobs, deep ecosystem hooks — that's Oban / River /
  graphile-worker / Sidekiq territory. PgQue is a shared event log with
  per-consumer cursors. Different shape.
- **You don't have any of the bloat conditions.** If your queue does
  300 jobs/day and you've never seen a vacuum problem, `SKIP LOCKED` is
  fine. PgQue earns its keep at sustained load and under MVCC pressure.

Soundbite: **"PgQue is for when your queue is supposed to be boring."**

### The honest fan-out caveat

Fan-out here means "N consumers each independently see every event."
That's the Kafka shape. If you want competing-consumers (one job goes to
exactly one of N workers), PgQue gives you that with a single registered
consumer and N workers polling — but a job framework will be a more
ergonomic fit.

## Block 6 — Roadmap and ecosystem (37–42)

What ships today:

- PG14–18 support, including 19devel.
- Pure SQL/PL/pgSQL install. Optional `pg_tle` packaging if you want
  `CREATE EXTENSION` semantics.
- pg_cron sub-second ticking, tunable at runtime.
- `send` / `receive` / `ack` / `nack` / `send_batch` API.
- Dead-letter queue after retry limit.
- Three role split: `pgque_reader`, `pgque_writer`, `pgque_admin` —
  siblings, not inherited; produce-and-consume apps need both grants.
- First-party clients: Python (psycopg 3), Go (pgx/v5), TypeScript
  (node-postgres).

What's next, in roughly the order it'll matter:

- **LISTEN/NOTIFY consumer wakeups** — drives e2e to single-digit-ms
  without raising the tick rate. Likely the highest-leverage open item.
- **Scheduled delivery (`send_at`)** — cron-shaped jobs without a
  separate scheduler.
- **OpenTelemetry / Prometheus exporters** — observability is the
  product gap right now; queue depth, lag, batch size by consumer.
- **Migration guides from PGMQ / pg-boss / Oban / River.**
- **Admin CLI.** Operators want one binary, not psql snippets.

## Block 7 — Closing (42–45)

- "If you're shipping anything with a queue inside Postgres, the
  question worth asking is: *what happens to my queue when xmin gets
  pinned for an hour?* If you don't know, that's a fire drill waiting
  to happen. PgQue is the answer that says: nothing happens. The queue
  keeps working. That's the whole pitch."
- Where to go: `github.com/NikolayS/pgque`. README walks the install,
  the comparison table, and a quick start. Tutorial in `docs/tutorial.md`.
- Status disclaimer: PgQ is decade-proven; the PgQue **packaging and API
  layer** are early-stage. Treat installs as one-way for now while
  upgrade paths are tightened.

## Q&A bank — likely host questions

Have crisp answers ready.

**"Isn't this just another in-database queue?"**
> The architecture is different in a way that matters. SKIP LOCKED
> queues bloat under sustained load — that's a property of the design,
> not of the implementations. PgQue uses snapshot batching and TRUNCATE
> rotation: zero dead tuples on the hot path, by construction. It's the
> only Postgres queue with that property other than upstream PgQ.

**"Why didn't anyone use PgQ if it's so good?"**
> Two reasons. One: it required a C extension and a daemon, neither of
> which run on managed Postgres — that's most of the market. Two: PgQ's
> API is low-level. `insert_event` / `next_batch` / `finish_batch` is
> Postgres-internals shaped, not application-shaped. PgQue fixes both:
> pure SQL install, modern `send` / `receive` / `ack` API on top.

**"Why not Kafka?"**
> Different operational profile. Kafka is the right answer if you've
> already operationalized Kafka, or if you need cross-datacenter
> replication, or if you're at scale where Postgres write throughput is
> the bottleneck. PgQue is the right answer if your data already lives
> in Postgres, you want transactional enqueue with your business writes,
> and you don't want to operate another distributed system.

**"How does this compare to PGMQ?"**
> PGMQ is the SKIP LOCKED design done well. It works, and it ships with
> tuned autovacuum settings to mitigate the bloat. It's a good fit for
> small-to-medium workloads. PgQue's snapshot-batching design avoids the
> bloat class entirely, and it has native fan-out (independent consumer
> cursors on a shared event log) — PGMQ doesn't. Different shape, not a
> drop-in replacement either way.

**"What about logical replication / CDC?"**
> Complementary. Use logical decoding when you want to expose database
> changes outside the database. Use PgQue when you want event semantics
> *inside* the database — transactional enqueue with your business
> writes, multiple independent consumers, retries, DLQ. They compose:
> CDC into PgQue is a reasonable pipeline.

**"What's the one thing you wish more people understood about Postgres
queues?"**
> The bloat tax isn't a tuning problem; it's a design problem. Every
> blog post telling you to add a partial index or tune autovacuum is
> treating the symptom. The cause is that the queue is a worklist of
> mutated rows. Make it a virtual cursor over an append-only log
> instead, and the problem disappears. PgQ figured that out in 2007.

## Demo flow (if there's screen-share)

Three minutes, four commands.

```sql
\i sql/pgque.sql                                          -- install, single transaction
select pgque.start();                                     -- pg_cron ticker + maint
select pgque.create_queue('orders');
select pgque.subscribe('orders', 'processor');

select pgque.send('orders', '{"order_id": 42}'::jsonb);   -- produce
select * from pgque.receive('orders', 'processor', 100);  -- consume
select pgque.ack(:batch_id);                              -- ack
```

Then show `select * from pgque.status();` and the three rotating event
tables (`pgq.event_<n>_0`, `_1`, `_2`) with `\dt+ pgq.event_*` so the
audience sees the rotation pattern in the file system.

## Stats sheet (have these in front of you)

- Skype era: PgQ ran ~hundreds of queues in production at Skype scale,
  decade+ in production.
- ~4,028 lines of proven PL/pgSQL inherited; ~1,500 lines of new
  PgQue-API layer.
- Default tick rate: 10 ticks/sec (100 ms). Tunable 1 ms – 1000 ms.
- WAL: ~280 bytes per *materialized* tick per queue. ~240 MiB/day at
  10 continuously-materialized ticks/sec; idle queues back off.
- Preliminary bench: ~86k events/sec batched insert; ~2.4M events/sec
  primitive batch read; zero dead-tuple growth across a 30-minute
  sustained test with a deliberately-blocked xmin horizon.
- Supports PG 14, 15, 16, 17, 18 in CI.

## Things to NOT say

- Don't claim "Postgres-native Kafka." It's an event log shape, not
  Kafka's durability/throughput envelope. Set expectations honestly.
- Don't trash SKIP LOCKED queues categorically. They work fine for
  small workloads and have great ergonomics. The argument is about
  sustained load and MVCC pressure, not about correctness.
- Don't promise dates on roadmap items.
