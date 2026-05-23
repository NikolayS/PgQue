#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import subprocess
import sys
from pathlib import Path

import psycopg

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
PARSE_EVENTS = ROOT / "tooling" / "parse_events_consumed.py"
BACKLOG_CHART = HERE / "chart_backlog.py"
THROUGHPUT_CHART = HERE / "chart_throughput.py"
SUMMARY_TABLE = HERE / "summary_table.py"
GIF_SCRIPT = HERE / "gif_subconsumer_scaling.py"


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Run the PgQue subconsumer scaling demo")
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--root", default="/tmp/bench_subc")
    ap.add_argument("--queue-prefix", default="subc_demo")
    ap.add_argument("--consumer", default="main_consumer")
    ap.add_argument("--message-count", type=int, default=1000)
    ap.add_argument("--sleep-ms", type=float, default=250.0)
    ap.add_argument("--workers", default="1,2,4,8,16")
    ap.add_argument("--batch-size", type=int, default=200)
    ap.add_argument("--startup-timeout", type=float, default=10.0)
    ap.add_argument("--skip-viz", action="store_true")
    return ap.parse_args()


def worker_list(text: str) -> list[int]:
    vals = []
    for part in text.split(","):
        part = part.strip()
        if not part:
            continue
        vals.append(int(part))
    if not vals:
        raise ValueError("workers list is empty")
    return vals


def payload_chunks(total: int, chunk_size: int) -> list[list[str]]:
    chunks: list[list[str]] = []
    for start in range(0, total, chunk_size):
        stop = min(start + chunk_size, total)
        chunk = [json.dumps({"msg_id": i + 1}) for i in range(start, stop)]
        chunks.append(chunk)
    return chunks


def drop_queue_if_exists(conn: psycopg.Connection, queue: str) -> None:
    exists = conn.execute(
        "select exists(select 1 from pgque.queue where queue_name = %s)",
        (queue,),
    ).fetchone()[0]
    if exists:
        conn.execute("select pgque.drop_queue(%s, true)", (queue,))


def prep_queue(conn: psycopg.Connection, queue: str, consumer: str, message_count: int, batch_size: int) -> None:
    drop_queue_if_exists(conn, queue)
    conn.execute("select pgque.create_queue(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))
    conn.execute("select pgque.subscribe(%s, %s)", (queue, consumer))
    for chunk in payload_chunks(message_count, batch_size):
        conn.execute("select pgque.send_batch(%s, %s::text[])", (queue, chunk))
    conn.execute("select pgque.force_tick(%s)", (queue,))
    conn.execute("select pgque.ticker(%s)", (queue,))


def derive_backlog(run_dir: Path, message_count: int) -> None:
    src = run_dir / "events_consumed_per_sec.csv"
    dst = run_dir / "backlog_per_sec.csv"
    cumulative = 0
    rows: list[tuple[int, int, int]] = []
    with src.open() as f:
        r = csv.DictReader(f)
        for row in r:
            sec = int(row["second_since_start"])
            ev = int(row["events_consumed"])
            cumulative += ev
            backlog = max(0, message_count - cumulative)
            rows.append((sec, cumulative, backlog))
    with dst.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["second_since_start", "cumulative_consumed", "messages_remaining"])
        for row in rows:
            w.writerow(row)


def run_one(root: Path, dsn: str, queue_prefix: str, consumer: str, message_count: int,
            sleep_ms: float, batch_size: int, workers: int, startup_timeout: float) -> Path:
    run_dir = root / f"{workers:02d}-workers"
    if run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True)
    queue = f"{queue_prefix}_{workers:02d}"

    with psycopg.connect(dsn, autocommit=True) as conn:
        prep_queue(conn, queue, consumer, message_count, batch_size)

    consumer_log = run_dir / "consumer.log"
    consumer_summary = run_dir / "consumer_summary.json"
    producer_log = run_dir / "producer.log"
    producer_summary = {
        "preloaded_messages": message_count,
        "sleep_ms": sleep_ms,
        "workers": workers,
        "queue": queue,
    }
    producer_log.write_text(
        f"preloaded {message_count} messages into {queue}\n"
        f"workers={workers} sleep_ms={sleep_ms}\n"
    )
    (run_dir / "producer_summary.json").write_text(json.dumps(producer_summary, indent=2) + "\n")

    cmd = [
        sys.executable,
        str(HERE / "consumer_pool.py"),
        "--dsn", dsn,
        "--queue", queue,
        "--consumer", consumer,
        "--workers", str(workers),
        "--sleep-ms", str(sleep_ms),
        "--max-return", str(message_count),
        "--expected-messages", str(message_count),
        "--summary-json", str(consumer_summary),
        "--startup-timeout", str(startup_timeout),
    ]
    with consumer_log.open("w") as f:
        subprocess.run(cmd, check=True, stdout=f, stderr=subprocess.STDOUT)

    subprocess.run(
        [
            sys.executable,
            str(PARSE_EVENTS),
            "--bench-dir", str(run_dir),
            "--bucket", "1",
            "--system", f"subc-{workers}",
        ],
        check=True,
    )
    derive_backlog(run_dir, message_count)

    meta = json.loads(consumer_summary.read_text())
    meta.update({
        "queue": queue,
        "message_count": message_count,
        "sleep_ms": sleep_ms,
        "expected_ev_s": workers / (sleep_ms / 1000.0),
    })
    (run_dir / "run_meta.json").write_text(json.dumps(meta, indent=2) + "\n")

    with psycopg.connect(dsn, autocommit=True) as conn:
        drop_queue_if_exists(conn, queue)

    return run_dir


def main() -> int:
    args = parse_args()
    root = Path(args.root)
    root.mkdir(parents=True, exist_ok=True)
    workers = worker_list(args.workers)

    for w in workers:
        print(f"=== subconsumer scaling: {w} workers ===")
        run_one(
            root=root,
            dsn=args.dsn,
            queue_prefix=args.queue_prefix,
            consumer=args.consumer,
            message_count=args.message_count,
            sleep_ms=args.sleep_ms,
            batch_size=args.batch_size,
            workers=w,
            startup_timeout=args.startup_timeout,
        )

    if not args.skip_viz:
        subprocess.run([sys.executable, str(THROUGHPUT_CHART), "--root", str(root)], check=True)
        subprocess.run([sys.executable, str(BACKLOG_CHART), "--root", str(root)], check=True)
        summary = subprocess.check_output([sys.executable, str(SUMMARY_TABLE), "--root", str(root)], text=True)
        (root / "summary.md").write_text(summary)
        print(summary)
        subprocess.run([sys.executable, str(GIF_SCRIPT), "--root", str(root)], check=True)

    print(f"done: outputs under {root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
