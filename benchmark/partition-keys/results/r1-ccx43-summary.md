# Partition-keys read-amplification benchmark

Supabase-Storage profile: keyed producer + N slot workers on the real
pgque v0.8 lease API. Maps to SPEC R2 (read amplification ~N x) and R7
(a stalled slot pins rotation for the whole queue).

## Phase `steady-16`  (N=16, target 5000 ev/s, 30 min)

- Producer: **5,000 ev/s** achieved, 8,996,791 events sent
- Consumers: **4,999 ev/s** end-to-end, 8,998,023 events acked
- CPU: 25% avg / 26% peak | RAM used: 3.5 / 5.4 GiB
- Pending events across slots: p50 0, p99 1,320, max 1,332
- Read-amp (receive_partitioned): 39,458,647 buffers (39,458,647 hit / 0 read) over 388,732 calls = **4.39 buffers/event**
- Per-slot consumed: { w0:288,446, w1:303,241, w2:267,539, w3:896,810, w4:499,753, w5:257,939, w6:530,210, w7:327,033, w8:787,695, w9:1,843,421, w10:845,659, w11:862,379, w12:283,429, w13:353,890, w14:298,668, w15:351,911 }

**Bloat & dead tuples**

- Metadata tables by peak dead tuples (end size):
    - `pgque.subscription`: peak 7,586 dead (end 7,572 dead, 704.00 KiB)
    - `pgque.tick`: peak 1,608 dead (end 0 dead, 888.00 KiB)
    - `pgque.partition_slot`: peak 557 dead (end 363 dead, 96.00 KiB)
    - `pgque.partition_consumer`: peak 35 dead (end 35 dead, 32.00 KiB)
    - `pgque.consumer`: peak 20 dead (end 20 dead, 48.00 KiB)
- `pgque.partition_slot` (v0.8 lease, 2 UPDATEs/batch): peak **557** dead, end 363 dead, 96.00 KiB — HOT updates should keep this tiny
- `pgque.tick` peak dead tuples: 1,608 (rotation-metadata churn)
- `pgque.subscription` peak dead tuples: 7,586 (rotation-metadata churn)
- Event tables: 4 present at phase start -> 4 at end (rotation dropping old `event_N_M`)
- Event-table bytes: start 56.00 KiB, end 3.57 GiB, peak 3.57 GiB
- Event-table peak dead tuples: 0 (append-only — expect ~0; nonzero means retries)
- Total DB footprint: end 3.57 GiB, peak 3.57 GiB

## Phase `steady-32`  (N=32, target 5000 ev/s, 30 min)

- Producer: **5,001 ev/s** achieved, 8,997,460 events sent
- Consumers: **5,000 ev/s** end-to-end, 9,000,840 events acked
- CPU: 30% avg / 31% peak | RAM used: 7.3 / 9.3 GiB
- Pending events across slots: p50 0, p99 1,318, max 1,337
- Read-amp (receive_partitioned): 63,166,134 buffers (63,166,134 hit / 0 read) over 775,996 calls = **7.02 buffers/event**
- Per-slot consumed: { w0:145,553, w1:142,039, w2:185,695, w3:451,545, w4:95,106, w5:116,060, w6:196,430, w7:166,868, w8:331,859, w9:1,707,562, w10:118,269, w11:782,539, w12:121,190, w13:219,230, w14:122,795, w15:211,793, w16:143,608, w17:160,798, w18:82,634, w19:447,463, w20:403,448, w21:141,426, w22:335,823, w23:159,336, w24:453,227, w25:136,178, w26:728,446, w27:80,067, w28:161,406, w29:135,008, w30:176,196, w31:141,243 }

**Bloat & dead tuples**

- Metadata tables by peak dead tuples (end size):
    - `pgque.subscription`: peak 9,502 dead (end 1,878 dead, 1.26 MiB)
    - `pgque.partition_slot`: peak 704 dead (end 578 dead, 96.00 KiB)
    - `pgque.partition_consumer`: peak 66 dead (end 0 dead, 64.00 KiB)
    - `pgque.consumer`: peak 20 dead (end 20 dead, 48.00 KiB)
    - `pgque.queue`: peak 8 dead (end 8 dead, 48.00 KiB)
- `pgque.partition_slot` (v0.8 lease, 2 UPDATEs/batch): peak **704** dead, end 578 dead, 96.00 KiB — HOT updates should keep this tiny
- `pgque.tick` peak dead tuples: 0 (rotation-metadata churn)
- `pgque.subscription` peak dead tuples: 9,502 (rotation-metadata churn)
- Event tables: 8 present at phase start -> 8 at end (rotation dropping old `event_N_M`)
- Event-table bytes: start 3.62 GiB, end 7.19 GiB, peak 7.19 GiB
- Event-table peak dead tuples: 0 (append-only — expect ~0; nonzero means retries)
- Total DB footprint: end 7.19 GiB, peak 7.19 GiB

## Phase `stalled-16`  (N=16, target 5000 ev/s, 15 min)

- Producer: **4,997 ev/s** achieved, 4,495,477 events sent
- Consumers: **4,996 ev/s** end-to-end, 4,496,137 events acked
- CPU: 25% avg / 30% peak | RAM used: 8.9 / 9.0 GiB
- Pending events across slots: p50 0, p99 1,665,413, max 2,386,485
- Read-amp (receive_partitioned): 13,361,585 buffers (13,361,585 hit / 0 read) over 189,979 calls = **2.97 buffers/event**
- Per-slot consumed: { w0:143,742, w1:150,971, w2:133,571, w3:449,575, w4:249,337, w5:128,238, w6:265,385, w7:163,156, w8:392,808, w9:921,401, w10:423,363, w11:431,755, w12:141,882, w13:176,922, w14:148,272, w15:175,759 }

**Bloat & dead tuples**

- Metadata tables by peak dead tuples (end size):
    - `pgque.subscription`: peak 8,912 dead (end 1,672 dead, 1.05 MiB)
    - `pgque.tick`: peak 7,205 dead (end 0 dead, 1.70 MiB)
    - `pgque.partition_slot`: peak 640 dead (end 329 dead, 96.00 KiB)
    - `pgque.consumer`: peak 36 dead (end 36 dead, 48.00 KiB)
    - `pgque.partition_consumer`: peak 16 dead (end 16 dead, 64.00 KiB)
- `pgque.partition_slot` (v0.8 lease, 2 UPDATEs/batch): peak **640** dead, end 329 dead, 96.00 KiB — HOT updates should keep this tiny
- `pgque.tick` peak dead tuples: 7,205 (rotation-metadata churn)
- `pgque.subscription` peak dead tuples: 8,912 (rotation-metadata churn)
- Event tables: 8 present at phase start -> 8 at end (rotation dropping old `event_N_M`)
- Event-table bytes: start 3.62 GiB, end 5.38 GiB, peak 5.38 GiB
- Event-table peak dead tuples: 0 (append-only — expect ~0; nonzero means retries)
- Total DB footprint: end 5.38 GiB, peak 5.38 GiB
- R7 pin/release — event-table bytes: stall start 3.62 GiB -> stall end 5.38 GiB -> phase end 5.38 GiB (grows while the stalled cursor pins rotation, drops after resume)

## Read amplification: N-scaling (SPEC R2)

| N | buffers/event | read/event | ratio vs smallest N |
|--:|--:|--:|--:|
| 16 | 4.39 | 0.00 | 1.00x |
| 32 | 7.02 | 0.00 | 1.60x |

Every slot scans the full stream and filters server-side, so buffers touched per produced event scale ~linearly with N. Observed 32/16 = 1.60x (ideal 2x).

## Stalled-slot: rotation pinning (SPEC R7)

Stalled slot (by peak lag): **slot 7**.
- Lease-expiry stall window: 896s (no live owner)
- Pending events: baseline 0 -> peak 2,386,485
- Lag growth during stall: **2,663 events/s**
- Catch-up after resume: did not return to baseline within the phase
- Rotation floor: queue held 3..3 event tables (the stalled slot pins the drop floor)

## Headline

| Phase | N | Producer ev/s | Consume ev/s | Pending p99 | Buffers/event | CPU peak | Peak dead tup (meta) | Event tbl GiB end |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| steady-16 | 16 | 5,000 | 4,999 | 1,320 | 4.39 | 26% | 7,586 | 3.57 |
| steady-32 | 32 | 5,001 | 5,000 | 1,318 | 7.02 | 31% | 9,502 | 7.19 |
| stalled-16 | 16 | 4,997 | 4,996 | 1,665,413 | 2.97 | 30% | 8,912 | 5.38 |

