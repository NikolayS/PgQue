#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from threading import Lock

import psycopg


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Drain one PgQue batch with a subconsumer thread pool")
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--queue", required=True)
    ap.add_argument("--consumer", required=True)
    ap.add_argument("--workers", type=int, required=True)
    ap.add_argument("--sleep-ms", type=float, required=True)
    ap.add_argument("--max-return", type=int, required=True)
    ap.add_argument("--expected-messages", type=int, required=True)
    ap.add_argument("--summary-json", required=True)
    ap.add_argument("--startup-timeout", type=float, default=10.0)
    ap.add_argument("--idle-poll-ms", type=float, default=50.0)
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    summary_path = Path(args.summary_json)
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    sleep_s = args.sleep_ms / 1000.0
    print_lock = Lock()
    processed = 0
    batches = 0
    empty_polls = 0
    t0 = time.time()

    def log_notice(n: int = 1) -> None:
        with print_lock:
            print(f"NOTICE: ev ts={int(time.time())} n={n}", flush=True)

    def process_one(_msg: tuple) -> None:
        time.sleep(sleep_s)
        log_notice(1)

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        startup_deadline = time.time() + args.startup_timeout
        while True:
            rows = conn.execute(
                "select * from pgque.receive(%s, %s, %s)",
                (args.queue, args.consumer, args.max_return),
            ).fetchall()

            if rows:
                batch_id = rows[0][1]
                batches += 1
                print(
                    f"batch {batch_id}: fetched {len(rows)} messages with {args.workers} subconsumers",
                    flush=True,
                )
                with ThreadPoolExecutor(max_workers=args.workers) as pool:
                    futures = [pool.submit(process_one, row) for row in rows]
                    for fut in as_completed(futures):
                        fut.result()
                        processed += 1
                acked = conn.execute("select pgque.ack(%s)", (batch_id,)).fetchone()[0]
                print(f"acked batch {batch_id}: ack={acked}", flush=True)
                if processed >= args.expected_messages:
                    break
                continue

            empty_polls += 1
            if processed >= args.expected_messages:
                break
            if time.time() > startup_deadline and processed == 0:
                raise RuntimeError(
                    f"no batch became visible within {args.startup_timeout:.1f}s"
                )
            time.sleep(args.idle_poll_ms / 1000.0)

    wall_s = time.time() - t0
    ideal_eps = args.workers / sleep_s if sleep_s > 0 else 0.0
    avg_eps = processed / wall_s if wall_s > 0 else 0.0
    summary = {
        "queue": args.queue,
        "consumer": args.consumer,
        "workers": args.workers,
        "sleep_ms": args.sleep_ms,
        "expected_messages": args.expected_messages,
        "processed_messages": processed,
        "batches": batches,
        "empty_polls": empty_polls,
        "wall_s": wall_s,
        "avg_ev_s": avg_eps,
        "ideal_ev_s": ideal_eps,
        "efficiency": (avg_eps / ideal_eps) if ideal_eps > 0 else None,
        "hostname": os.uname().nodename,
    }
    summary_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(
        f"done: processed {processed} messages in {wall_s:.3f}s "
        f"({avg_eps:.2f} ev/s avg, ideal {ideal_eps:.2f})",
        flush=True,
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        raise
