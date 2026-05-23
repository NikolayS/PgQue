# subconsumer scaling demo

Focused benchmark + visualization harness for the `250 ms / message` story.

One consumer fetches a PgQue batch, then hands message processing to an in-process
pool of **subconsumers**. Each subconsumer sleeps for a fixed amount of time per
message to emulate a transactional email API call — think Resend or SendGrid.

The point is simple: when downstream work costs ~250 ms per message, a single
worker tops out at ~4 messages / second. More subconsumers drain the same
backlog faster.

## What it measures

- **Throughput** — events consumed per second, parsed from `NOTICE:`-style logs
- **Backlog** — `preloaded_messages - cumulative_consumed`
- **Drain time** — wall time from consumer start until the backlog reaches zero
- **Scaling efficiency** — observed throughput vs. the ideal `workers / sleep_s`

## Files

- `run.py` — orchestrates the whole demo across worker counts
- `run.sh` — tiny shell wrapper around `run.py`
- `consumer_pool.py` — single PgQue consumer + thread pool of subconsumers
- `chart_throughput.py` — timeline chart of events / second
- `chart_backlog.py` — timeline chart of messages remaining
- `summary_table.py` — markdown summary table
- `gif_subconsumer_scaling.py` — animated two-panel GIF
- `backlog_race.py` — the current single-axis backlog-drain graphic used in the main README
- `single_message_latency.py` — static per-message-latency chart used in the main README
- `scaling_linearity.py` — static throughput-vs-subconsumers chart (alternative view)
- `completion_latency.py` — alternative completion-time-by-queue-position chart

## Quick start

```bash
cd benchmark/subconsumer-scaling
python3 run.py \
  --dsn postgres://postgres@127.0.0.1:5432/bench \
  --message-count 1000 \
  --sleep-ms 250 \
  --workers 1,2,4,8,16
```

Outputs land under `/tmp/bench_subc/` by default:

```text
/tmp/bench_subc/
  01-workers/
  02-workers/
  04-workers/
  08-workers/
  16-workers/
  throughput.png
  backlog.png
  scaling.gif
  scaling_hero.png
  summary.md
```

## Queue flow

For each worker count:

1. drop + recreate a fresh queue
2. create an initial tick
3. subscribe one main consumer
4. preload all messages with `pgque.send_batch()`
5. `force_next_tick()` + `ticker()` once, so the backlog becomes visible in one batch
6. run one consumer process with an internal thread pool of `N` subconsumers
7. parse `NOTICE: ev ts=<epoch_s> n=1` lines into `events_consumed_per_sec.csv`
8. derive backlog and summaries

## Why one forced tick

This demo is about **consumer-side scaling**, not producer cadence. Everything is
preloaded before one forced tick, so every worker-count run drains the same
backlog from the same starting line.


## Recommended assets for the main README

- `docs/images/backlog_race.gif` — one-axis backlog-drain race
- `docs/images/single_message_latency.png` — static per-message-latency chart on a plain linear x-axis
