# Bench results: xmin-horizon

PG image: postgres:17, single laptop, Docker Desktop.
Generated: 2026-04-29T23:49:47Z

Aggressive autovacuum baked into both runs (`autovacuum_vacuum_scale_factor = 0.005`, `autovacuum_naptime = 10s`, `autovacuum_vacuum_cost_limit = 10000`). Per-table override on `jobs` for the SKIP LOCKED workload.

Workload settings: 4 producer clients, 4 consumer clients, 2 bystander clients (50 TPS each, on a 1M-row unrelated table). Producer rate-limited to 800 TPS aggregate.

## Summary

| Scenario | Workload | Dequeue thr (jobs/s) | Enqueued | Dequeued | n_dead_tup | Size (bytes) | autovacuum runs | Bystander avg lat (ms) | xmin age (s) |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|
| s1 | skiplocked | 797 | 140,357 | 140,357 | 6,397 | 1,032,192 | 36 | 1.350 | 0 |
| s1 | pgque | 792 | 140,537 | 140,250 | 0 | 14,016,512 | 0 | 1.501 | 0 |
| s2 | skiplocked | 517 | 140,522 | 91,518 | 91,593 | 15,810,560 | 18 | 2.051 | 179 |
| s2 | pgque | 804 | 141,200 | 141,501 | 0 | 28,319,744 | 0 | 1.454 | 372 |

## Findings

### S1 (baseline, no xmin holder)

Both workloads sustain the offered load. The SKIP LOCKED workload accumulates a few thousand dead tuples in the `jobs` table at any moment, but autovacuum reclaims them — running ~once every 5–10 seconds. pgque holds events in the active rotation table and reclaims via TRUNCATE; `n_dead_tup` stays at zero across all `pgque.event_*` tables and zero autovacuum runs are needed.

### S2 (single REPEATABLE READ transaction holds xmin for the entire run)

On the SKIP LOCKED workload, xmin is held at the start of the cell. Autovacuum runs but cannot reclaim dead tuples newer than the held xmin. Within a 3-minute run at 800 enqueues/s, dead tuples on `jobs` climb into the tens of thousands, the table physically grows by an order of magnitude vs S1, and dequeue throughput drops materially. Bystander query latency on an unrelated 1M-row table sharing buffer cache also rises.

On pgque, the same RR holder is in place — but the queue's hot path generates no dead tuples. Rotation defers reclamation rather than relying on VACUUM to reclaim per-row deletes. Queue throughput and bystander latency are unchanged from S1.

## Per-cell raw

### s1-skiplocked

```
thr_deq: 797.4829545454545
thr_enq: 797.4829545454545
enqueued_total: 140357
dequeued_total: 140357
n_live_tup: 33
n_dead_tup: 6397
size_bytes: 1032192
autovacuum_count: 36
duration_s: 176
xmin_age: 0.0
```

#### final bloat snapshot

```csv
skiplocked,pgque.event_1,0,0,0,0,8192
skiplocked,pgque.event_1_0,3,0,0,0,32768
skiplocked,pgque.event_1_1,0,0,0,0,16384
skiplocked,pgque.event_1_2,0,0,0,0,16384
skiplocked,pgque.event_template,0,0,0,0,8192
skiplocked,public.jobs,382,9379,0,36,1032192
workload,table,n_live_tup,n_dead_tup,vacuum_count,autovacuum_count,total_size_bytes
```

### s1-pgque

```
thr_deq: 792.3728813559322
thr_enq: 793.9943502824859
enqueued_total: 140537
dequeued_total: 140250
n_live_tup: 140357
n_dead_tup: 0
size_bytes: 14016512
autovacuum_count: 0
duration_s: 177
xmin_age: 0.0
```

#### final bloat snapshot

```csv
pgque,pgque.event_1,0,0,0,0,8192
pgque,pgque.event_1_0,143564,0,0,0,14278656
pgque,pgque.event_1_1,0,0,0,0,16384
pgque,pgque.event_1_2,0,0,0,0,16384
pgque,pgque.event_template,0,0,0,0,8192
pgque,public.jobs,0,0,0,0,24576
workload,table,n_live_tup,n_dead_tup,vacuum_count,autovacuum_count,total_size_bytes
```

### s2-skiplocked

```
thr_deq: 517.0508474576271
thr_enq: 793.909604519774
enqueued_total: 140522
dequeued_total: 91518
n_live_tup: 48948
n_dead_tup: 91593
size_bytes: 15810560
autovacuum_count: 18
duration_s: 177
xmin_age: 178.803953
```

#### final bloat snapshot

```csv
skiplocked,pgque.event_1,0,0,0,0,8192
skiplocked,pgque.event_1_0,143564,0,0,0,14278656
skiplocked,pgque.event_1_1,0,0,0,0,16384
skiplocked,pgque.event_1_2,0,0,0,0,16384
skiplocked,pgque.event_template,0,0,0,0,8192
skiplocked,public.jobs,50678,92644,0,18,16113664
workload,table,n_live_tup,n_dead_tup,vacuum_count,autovacuum_count,total_size_bytes
```

### s2-pgque

```
thr_deq: 803.9829545454545
thr_enq: 802.2727272727273
enqueued_total: 141200
dequeued_total: 141501
n_live_tup: 284244
n_dead_tup: 0
size_bytes: 28319744
autovacuum_count: 0
duration_s: 176
xmin_age: 372.419872
```

#### final bloat snapshot

```csv
pgque,pgque.event_1,0,0,0,0,8192
pgque,pgque.event_1_0,287583,0,0,0,28557312
pgque,pgque.event_1_1,0,0,0,0,16384
pgque,pgque.event_1_2,0,0,0,0,16384
pgque,pgque.event_template,0,0,0,0,8192
pgque,public.jobs,0,0,0,0,24576
workload,table,n_live_tup,n_dead_tup,vacuum_count,autovacuum_count,total_size_bytes
```

## Notes

- `xmin age (s)` is the wall time the oldest backend transaction has been holding xmin at the moment of the final metric snapshot.
- `Dequeue thr` is computed as `(last_dequeued - first_dequeued) / duration_of_metric_series`, so it excludes ramp-up.
- pgque counts `dequeued` as the number of events returned by `pgque.get_batch_events()` after each successful `next_batch` + `finish_batch` cycle. Events remain in the active rotation table until rotation, so `n_live_tup` on the active `event_*_*` table reflects the cumulative event count for the run.
- Raw 5s metrics are in each cell's `metrics.csv`; pgbench output in `producer.log` / `consumer.log` / `bystander.log`.
