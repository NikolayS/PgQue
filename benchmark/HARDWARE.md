# Hardware — VM sizing and microbench baselines

## VM: AWS i4i.2xlarge (us-east-2)

| Spec | Value |
|---|---|
| vCPU | 8 (Intel Ice Lake Xeon 8375C, 2.9 GHz base / 3.5 GHz turbo) |
| RAM | 64 GB |
| NVMe instance store | 1 × 1.75 TB (physical attach, NVMe) |
| Network | Up to 12 Gbps |
| EBS (root) | 8 GB gp3 (Ubuntu 24.04 AMI default) |
| Spot price (us-east-2, 2026-04) | ~$0.20–0.30 / hour |
| On-demand price | $0.686 / hour |

See [OPS_GOTCHAS.md §1](OPS_GOTCHAS.md) — the NVMe instance store is **not** auto-mounted on Ubuntu 24.04 boot.

## Expected microbench baselines

Run via [tooling/microbench.sh](tooling/microbench.sh). Expected order-of-magnitude numbers:

| Probe | Expected |
|---|---|
| sysbench cpu (1-thread events/sec) | ~25 k |
| sysbench memory bandwidth (1-thread) | ~15 GiB/sec |
| fio 4 k randwrite, QD=32, direct=1, on NVMe | ~300 k IOPS |
| fio 4 k randwrite, bandwidth | ~1.2 GiB/sec |
| fio 4 k randwrite, 99 p latency | ~100 µs |
| fio 4 k randread, QD=32, on NVMe | ~400 k IOPS |

*Actual R7 microbench numbers to be filled in from the R7 microbench pass.*

## Postgres tuning (shared across all 7 VMs)

Applied by each VM's bootstrap (see `install/install_*.sh`):

```
shared_preload_libraries = 'pg_stat_statements,pg_cron'
cron.database_name = 'bench'

shared_buffers = 4GB
effective_cache_size = 12GB

synchronous_commit = off
wal_level = minimal
wal_compression = lz4
max_wal_size = 16GB
checkpoint_completion_target = 0.9

bgwriter_delay = 50ms
bgwriter_lru_maxpages = 400
bgwriter_lru_multiplier = 4.0

random_page_cost = 1.1
effective_io_concurrency = 200
max_connections = 200

max_wal_senders = 0

autovacuum_vacuum_scale_factor  = 0.01
autovacuum_analyze_scale_factor = 0.01
autovacuum_vacuum_cost_delay    = 2ms

jit = off
listen_addresses = 'localhost'
```

`synchronous_commit=off` is deliberate — queue workloads are almost always idempotent at the application layer, and the WAL-flush path is the dominant cost for low-latency producers. It's the only PG knob we touch that materially changes safety posture.

`jit=off` because 5-s JIT warmups on `DO` blocks dominated our first-transaction latency in R4.

`autovacuum_*_scale_factor=0.01` is aggressive on purpose — we want autovacuum attempting to clean every 1 % dead-tuple ratio so the held-xmin phase of the bench exposes the *inability* to vacuum, not a lazy autovacuum schedule.
