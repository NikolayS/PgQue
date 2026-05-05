# benchmark/tick-rate

Producer→consumer end-to-end latency under different `pgque.set_tick_period_ms()`
settings, plus a held-xmin variant that demonstrates how `pgque.tick` and
`pgque.subscription` UPDATEs degrade under blocked vacuum.

Backs PR #204 / issue #69.

## What it measures

**Latency** = `consumer_recv_ts - producer_send_ts`, both server-side
`clock_timestamp()` (no client clock skew). The producer stamps the timestamp
into the JSON payload at `pgque.send()`. The consumer reads the payload, takes
its own `clock_timestamp()` at `pgque.receive()` time, and subtracts.

This is **end-to-end delivery latency** (latency #3 in
[docs/three-latencies.md](../../docs/three-latencies.md)) — the gap that the
tick rate actually controls.

## Run

Requires:

- Postgres 14+ with `pg_cron` available, `cron.use_background_workers = on`
  (TCP-auth-free), `cron.database_name` pointing at the bench database.
- `python3` with `psycopg` (`pip install 'psycopg[binary]'`).
- The schema installed via `\i sql/pgque.sql` and `pgque.start()` already
  invoked.

```sh
# Idle sweep (a few minutes total).
python3 benchmark/tick-rate/bench.py \
    --dsn "host=/var/run/postgresql dbname=pgque_bench user=postgres" \
    --scenario idle \
    --periods 1000,100,10,1 \
    --duration-s 30 \
    --rate-eps 100

# Held-xmin orchestration (1 min baseline + 5 min held-xmin run).
bash benchmark/tick-rate/run_held_xmin.sh
```

## Results — local sandbox

Single-laptop, Postgres 16, default settings, `pg_cron` 1.6.2 with
`use_background_workers = on`.

### Idle sweep (30 s per cell, 100 ev/s producer)

| `tick_period_ms` | rate_eps |  sent | recvd |   p50 |   p95 |    p99 |    max |  mean |
|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| 1000 |   100 |  3000 |  3000 | 503.32 | 953.87 | 993.89 | 1004.16 | 503.32 |
|  100 |   100 |  3000 |  3000 |  52.62 |  98.86 | 103.48 |  105.28 |  52.74 |
|   10 |   100 |  3000 |  3000 |   8.05 | 263.53 | 863.50 | 1013.39 |  41.86 |
|    1 |   100 |  3000 |  3000 |   3.26 | 161.67 | 460.47 |  547.91 |  22.07 |

Reads:

- `tick_period_ms = 100` (the default) — clean: median ~52 ms, max ~105 ms,
  exactly tracks "wait for next tick, mean ≈ period/2".
- `tick_period_ms = 10` and `tick_period_ms = 1` — median improves to single
  digits, but the **tail blows up** to ~1 s. Suspected cause: at sub-10 ms
  periods the procedure can't actually complete its inner iterations within
  one pg_cron 1-second slot, so the next slot can land on a still-running
  worker and effectively skip a tick window. Drives both the p99 and the max
  up to ~1 slot length.
- The "0.1 ticks/sec" / `tick_period_ms = 10000` cell originally requested in
  the PR comment — **not measurable**. `ticker_loop` clamps the internal
  period at 1 s (the pg_cron slot length), so any value above 1000 collapses
  to "one tick per slot" = `tick_period_ms = 1000`. The setter now rejects
  values > 1000 and any value that is not an exact divisor of the 1000 ms slot.

### Held-xmin (default `tick_period_ms = 100`, 1000 ev/s)

A separate session runs `BEGIN ISOLATION LEVEL REPEATABLE READ; ...
pg_sleep(...);` for the duration of the bench, holding the cluster xmin floor
and preventing autovacuum from reclaiming dead tuples on `pgque.tick` and
`pgque.subscription`.

|        condition  | duration |  sent  | recvd |   p50 |   p95 |    p99 |    max |  mean |
|---|---|---|---|---|---|---|---|---|
| baseline (no held-xmin) |   60 s | 60 000 | 60 000 |  52.60 |  98.96 | 103.40 | 143.76 |  52.75 |
| held-xmin (RR tx open)  |  300 s | 300 000 | 300 000 |  53.83 | 100.18 | 104.73 | 235.56 |  54.03 |

Reads:

- p50 / p95 / p99 are essentially unchanged. Median e2e stays at ~53 ms; p99
  at ~104 ms.
- Worst-case (max) roughly **doubles**: 144 ms → 236 ms.
- This is much milder than the upstream-PgQ baseline reported in the R7 90-min
  bench (peak dead tuples > 21k upstream vs. ≤ 1000 with PR #62's metadata
  rotation). Two reasons the picture looks calm here:
  - Test duration is 5 min, not 30–90 min. Metadata bloat scales with held
    duration; longer runs would visibly degrade the tail further.
  - Tick rate is 10/sec, not 100/sec or 1000/sec. The metadata-table churn at
    higher rates would also amplify the effect.

The takeaway is **not** "held-xmin is fine, ignore #62". It's that under a
moderate (5-min, 10-tick/sec) blocked-vacuum window, the tail degrades but the
median is robust — consistent with PgQue's design goal of stable latency
under load. The ceiling on this stability is what PR #62's rotation fix
extends.

## Methodology notes

- The producer sends one event per `pgque.send()` call (no batching). Single
  Python thread sustains 1000 ev/s on a local socket.
- The consumer polls `pgque.receive(queue, consumer, 1000)` in a tight loop
  with a 1 ms sleep between empty receives. No LISTEN/NOTIFY wakeup — the
  point is to measure the tick-driven path.
- The drain budget at run end is `max(2 s, 3 × tick_period_ms)` so slow ticks
  don't truncate trailing events.
- For the idle sweep, queue config is `ticker_max_count = 1`,
  `ticker_max_lag = 0 s`, `ticker_idle_period = 0 s` so every tick fires
  regardless of event volume — isolating the effect of `tick_period_ms`.
