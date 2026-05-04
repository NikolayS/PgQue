#!/usr/bin/env python3
"""
tick-rate latency bench.

Measures producer→consumer end-to-end latency for pgque under different
ticker_loop tick periods.

Latency definition:
    latency_ms = consumer_recv_ts - producer_send_ts
where both timestamps are server-side `clock_timestamp()` (no clock skew).

The producer stamps `clock_timestamp()` into the event payload at INSERT
time. The consumer reads the payload, computes the delta, and records it.

Two scenarios:
  --scenario idle        : light producer, sweep tick_period_ms.
  --scenario held-xmin   : a long-running RR transaction holds xmin while
                           a 1000 ev/s producer runs. Demonstrates how
                           tick/subscription metadata UPDATEs degrade
                           latency under blocked vacuum.

Usage:
    python3 bench.py --dsn "host=localhost dbname=pgque_bench user=postgres" \
                     --scenario idle \
                     --periods 10000,1000,100,10,1 \
                     --duration-s 30 \
                     --rate-eps 100

    python3 bench.py --dsn "..." \
                     --scenario held-xmin \
                     --tick-period-ms 100 \
                     --duration-s 300 \
                     --rate-eps 1000
"""
from __future__ import annotations

import argparse
import json
import statistics
import sys
import threading
import time
from contextlib import closing

import psycopg


def percentile(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    k = int(round((p / 100.0) * (len(xs) - 1)))
    return xs[k]


def fmt_ms(x):
    return "n/a" if x != x else f"{x:.2f}"  # NaN check


def setup(dsn, queue, consumer):
    with closing(psycopg.connect(dsn, autocommit=True)) as cn, cn.cursor() as cur:
        cur.execute("select 1 from pgque.queue where queue_name = %s", (queue,))
        if not cur.fetchone():
            cur.execute("select pgque.create_queue(%s)", (queue,))
        # Aggressive ticking even when idle: max_lag = 0 means any tick fires.
        cur.execute("select pgque.set_queue_config(%s, 'ticker_max_lag', '0 seconds')", (queue,))
        cur.execute("select pgque.set_queue_config(%s, 'ticker_idle_period', '0 seconds')", (queue,))
        cur.execute("select pgque.set_queue_config(%s, 'ticker_max_count', '1')", (queue,))
        # Subscribe (idempotent — re-subscribe after cleanup)
        cur.execute("select 1 from pgque.subscription s join pgque.queue q on q.queue_id=s.sub_queue "
                    "where q.queue_name=%s and s.sub_consumer=(select co_id from pgque.consumer where co_name=%s)", (queue, consumer))
        if not cur.fetchone():
            cur.execute("select pgque.subscribe(%s, %s)", (queue, consumer))


def teardown(dsn, queue, consumer):
    with closing(psycopg.connect(dsn, autocommit=True)) as cn, cn.cursor() as cur:
        try:
            cur.execute("select pgque.unsubscribe(%s, %s)", (queue, consumer))
        except Exception:
            pass
        try:
            cur.execute("select pgque.drop_queue(%s)", (queue,))
        except Exception:
            pass


def producer_thread(dsn, queue, rate_eps, duration_s, stop_evt, stats):
    """Send events at target rate. Each payload carries server-side send_ts."""
    interval = 1.0 / rate_eps
    sent = 0
    start = time.monotonic()
    next_send = start
    with closing(psycopg.connect(dsn, autocommit=True)) as cn, cn.cursor() as cur:
        while not stop_evt.is_set() and (time.monotonic() - start) < duration_s:
            now = time.monotonic()
            if now < next_send:
                time.sleep(min(next_send - now, 0.001))
                continue
            # Server-side timestamp goes into the payload.
            try:
                cur.execute(
                    "select pgque.send(%s, jsonb_build_object('seq', %s::int, "
                    "'send_ts', extract(epoch from clock_timestamp())))",
                    (queue, sent),
                )
                sent += 1
            except Exception as e:
                stats.setdefault("errors", []).append(f"producer: {e}")
            next_send += interval
    stats["sent"] = sent
    stats["duration_s"] = time.monotonic() - start


def consumer_thread(dsn, queue, consumer, stop_evt, stats, drain_extra_s=2.0):
    """Continuously receive + ack, computing latency = recv_ts - send_ts per event."""
    latencies_ms = []
    received = 0
    last_msg_at = time.monotonic()
    with closing(psycopg.connect(dsn, autocommit=True)) as cn, cn.cursor() as cur:
        while True:
            cur.execute(
                "select extract(epoch from clock_timestamp()), payload, batch_id "
                "from pgque.receive(%s, %s, 1000)",
                (queue, consumer),
            )
            rows = cur.fetchall()
            if rows:
                last_msg_at = time.monotonic()
                bid = rows[0][2]
                for recv_epoch, payload, _ in rows:
                    pl = payload if isinstance(payload, dict) else json.loads(payload)
                    latency_ms = (float(recv_epoch) - float(pl["send_ts"])) * 1000.0
                    latencies_ms.append(latency_ms)
                    received += 1
                cur.execute("select pgque.ack(%s)", (bid,))
            else:
                # No batch yet — sleep briefly to avoid busy-looping.
                if stop_evt.is_set() and (time.monotonic() - last_msg_at) > drain_extra_s:
                    break
                time.sleep(0.001)
    stats["received"] = received
    stats["latencies_ms"] = latencies_ms


def run_one(dsn, queue, consumer, tick_period_ms, rate_eps, duration_s):
    """Run one (tick_period_ms, rate_eps, duration_s) cell."""
    # Set the rate.
    with closing(psycopg.connect(dsn, autocommit=True)) as cn, cn.cursor() as cur:
        cur.execute("select pgque.set_tick_period_ms(%s)", (tick_period_ms,))
    # Wait one full pg_cron slot so the new rate kicks in.
    time.sleep(1.2)

    stop_evt = threading.Event()
    p_stats, c_stats = {}, {}
    # drain budget: at least one full tick period plus 2 s slack so slow
    # ticks don't truncate trailing events.
    drain_extra_s = max(2.0, (tick_period_ms / 1000.0) * 3.0)
    pt = threading.Thread(target=producer_thread,
                          args=(dsn, queue, rate_eps, duration_s, stop_evt, p_stats),
                          daemon=True)
    ct = threading.Thread(target=consumer_thread,
                          args=(dsn, queue, consumer, stop_evt, c_stats, drain_extra_s),
                          daemon=True)
    ct.start()
    pt.start()
    pt.join()
    stop_evt.set()
    ct.join(timeout=duration_s + drain_extra_s + 10)

    lat = c_stats.get("latencies_ms", [])
    return {
        "tick_period_ms": tick_period_ms,
        "rate_eps": rate_eps,
        "duration_s": p_stats.get("duration_s", duration_s),
        "sent": p_stats.get("sent", 0),
        "received": c_stats.get("received", 0),
        "p50_ms": percentile(lat, 50),
        "p95_ms": percentile(lat, 95),
        "p99_ms": percentile(lat, 99),
        "max_ms": max(lat) if lat else float("nan"),
        "mean_ms": statistics.fmean(lat) if lat else float("nan"),
    }


def print_table(rows, scenario):
    header = (
        f"\n=== Scenario: {scenario} ===\n"
        "| tick_period_ms | rate_eps |  sent  | recvd |   p50 |   p95 |   p99 |   max |  mean |\n"
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
    )
    print(header)
    for r in rows:
        print(
            f"| {r['tick_period_ms']:>5} | {r['rate_eps']:>5} | "
            f"{r['sent']:>5} | {r['received']:>5} | "
            f"{fmt_ms(r['p50_ms']):>5} | {fmt_ms(r['p95_ms']):>5} | "
            f"{fmt_ms(r['p99_ms']):>5} | {fmt_ms(r['max_ms']):>5} | "
            f"{fmt_ms(r['mean_ms']):>5} |"
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dsn", required=True)
    p.add_argument("--scenario", choices=("idle", "held-xmin"), default="idle")
    p.add_argument("--periods", default="10000,1000,100,10,1",
                   help="comma-separated tick_period_ms values (idle scenario only)")
    p.add_argument("--tick-period-ms", type=int, default=100,
                   help="single tick_period_ms (held-xmin scenario)")
    p.add_argument("--duration-s", type=float, default=30.0)
    p.add_argument("--rate-eps", type=int, default=100)
    p.add_argument("--queue", default="bench_tick")
    p.add_argument("--consumer", default="bench_consumer")
    p.add_argument("--out-json", default=None)
    args = p.parse_args()

    setup(args.dsn, args.queue, args.consumer)

    rows = []
    try:
        if args.scenario == "idle":
            for tp in [int(x) for x in args.periods.split(",")]:
                print(f"running tick_period_ms={tp}, rate={args.rate_eps} ev/s, "
                      f"duration={args.duration_s}s ...", flush=True)
                rows.append(run_one(args.dsn, args.queue, args.consumer,
                                    tp, args.rate_eps, args.duration_s))
        else:
            print(f"running held-xmin scenario, tick_period_ms={args.tick_period_ms}, "
                  f"rate={args.rate_eps} ev/s, duration={args.duration_s}s ...",
                  flush=True)
            rows.append(run_one(args.dsn, args.queue, args.consumer,
                                args.tick_period_ms, args.rate_eps,
                                args.duration_s))
    finally:
        teardown(args.dsn, args.queue, args.consumer)

    print_table(rows, args.scenario)
    if args.out_json:
        with open(args.out_json, "w") as f:
            json.dump({"scenario": args.scenario, "rows": rows}, f, indent=2,
                      default=lambda o: None if o != o else o)
        print(f"\nwrote {args.out_json}")


if __name__ == "__main__":
    sys.exit(main())
