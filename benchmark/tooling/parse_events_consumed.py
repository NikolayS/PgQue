#!/usr/bin/env python3
"""parse_events_consumed.py — turn NOTICE-instrumented consumer.log into
an honest events-delivered-per-second time series.

Inputs (by convention, --bench-dir):
  consumer.log   : pgbench stdout+stderr merged; contains lines like
                   "NOTICE:  ev ts=<epoch_s> n=<events>"
                   (and the usual pgbench progress + banner lines)
  producer.log   : pgbench producer output — used only to derive run t0
                   when no NOTICE is present, or for sanity cross-checks.
  pgss.csv       : optional; queried at end of run; used for sanity check.
  bloat.csv      : optional; used by the bench-script post-processor only.

Outputs (to --out-dir or --bench-dir):
  events_consumed_per_sec.csv   columns: second_since_start,events_consumed
  events_consumed_summary.txt   human summary

Method:
  - Primary: parse NOTICE lines. Group by second_since_start=(ts - t0).
  - Fallback: if zero NOTICE lines found, emit a single row with total=0
    and log a warning (caller should switch to pgss sampling).
  - t0: earliest NOTICE ts; or first timestamp in producer_agg log if present.

Robust to: missing files, truncated runs, interleaved stderr from
multiple pgbench clients, oversized files (streamed line-by-line).
"""
from __future__ import annotations
import argparse, csv, os, re, sys
from collections import defaultdict
from pathlib import Path

NOTICE_RE = re.compile(r"NOTICE:\s+ev\s+ts=(\d+)\s+n=(\d+)")
# pgbench aggregate log format: "start_ts.usec n_tx lat sqlat ..."
# but our producer --aggregate-interval=10 --log writes one row per interval.
AGG_FIRST_TS_RE = re.compile(r"^(\d{10})(?:\.\d+)?\s")


def parse_notice_log(path: Path) -> list[tuple[int, int]]:
    """Return list of (ts_epoch_s, n_events) tuples from NOTICE lines."""
    out: list[tuple[int, int]] = []
    if not path.is_file():
        return out
    with path.open("r", errors="replace") as f:
        for line in f:
            m = NOTICE_RE.search(line)
            if m:
                out.append((int(m.group(1)), int(m.group(2))))
    return out


def find_t0_from_producer(bench_dir: Path) -> int | None:
    """Find earliest timestamp from producer_agg.<pid> file (pgbench aggregate log)."""
    candidates = sorted(bench_dir.glob("producer_agg.*"))
    for c in candidates:
        try:
            with c.open("r", errors="replace") as f:
                for line in f:
                    m = AGG_FIRST_TS_RE.match(line)
                    if m:
                        return int(m.group(1))
        except OSError:
            continue
    return None


def bucket(events: list[tuple[int, int]], t0: int, width_s: int) -> dict[int, int]:
    """Return {second_since_start_bucket_start: total_events}."""
    hist: dict[int, int] = defaultdict(int)
    for ts, n in events:
        s = ts - t0
        if s < 0:
            s = 0
        bucket_start = (s // width_s) * width_s
        hist[bucket_start] += n
    return hist


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bench-dir", default="/tmp/bench",
                    help="Directory with consumer.log, producer.log, etc.")
    ap.add_argument("--out-dir", default=None,
                    help="Where to write events_consumed_per_sec.csv (default: --bench-dir)")
    ap.add_argument("--bucket", type=int, default=1,
                    help="Bucket width in seconds (1 or 10 typical).")
    ap.add_argument("--system", default=None,
                    help="Informational: system name; recorded in summary.")
    args = ap.parse_args()

    bench_dir = Path(args.bench_dir)
    out_dir = Path(args.out_dir) if args.out_dir else bench_dir
    out_dir.mkdir(parents=True, exist_ok=True)

    consumer_log = bench_dir / "consumer.log"
    events = parse_notice_log(consumer_log)

    out_csv = out_dir / "events_consumed_per_sec.csv"
    summary_txt = out_dir / "events_consumed_summary.txt"

    if not events:
        # No NOTICE lines — emit empty CSV and a warning summary.
        with out_csv.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["second_since_start", "events_consumed"])
        with summary_txt.open("w") as f:
            f.write(
                f"system={args.system or 'unknown'}\n"
                f"bench_dir={bench_dir}\n"
                f"method=NOTICE\n"
                f"notice_lines_found=0\n"
                "WARNING: no NOTICE events_consumed data — verify consumer.sql\n"
                "         uses NOTICE instrumentation. Fallback: use\n"
                "         pg_stat_statements snapshot (pgss_timeseries.csv)\n"
                "         or compute via producer_rate - queue_bloat_delta.\n"
            )
        print(f"WARN: no NOTICE events in {consumer_log}; wrote empty {out_csv}",
              file=sys.stderr)
        return 0

    # t0: prefer producer_agg earliest ts; else first NOTICE ts.
    t0 = find_t0_from_producer(bench_dir)
    if t0 is None:
        t0 = min(e[0] for e in events)

    hist = bucket(events, t0, args.bucket)

    # Write CSV, sorted.
    sec_keys = sorted(hist.keys())
    max_sec = sec_keys[-1] if sec_keys else 0

    with out_csv.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["second_since_start", "events_consumed"])
        # Emit every bucket from 0..max, filling zeros for empty buckets —
        # this produces a proper TS the plotter can consume without gaps.
        for s in range(0, max_sec + args.bucket, args.bucket):
            w.writerow([s, hist.get(s, 0)])

    total_events = sum(n for _, n in events)
    total_calls = len(events)
    # Calls with n>0 (a.k.a. useful polls)
    useful_calls = sum(1 for _, n in events if n > 0)
    run_s = max_sec + args.bucket
    avg_eps = total_events / run_s if run_s > 0 else 0.0

    with summary_txt.open("w") as f:
        f.write(
            f"system={args.system or 'unknown'}\n"
            f"bench_dir={bench_dir}\n"
            f"method=NOTICE\n"
            f"t0_epoch_s={t0}\n"
            f"bucket_s={args.bucket}\n"
            f"total_notice_lines={total_calls}\n"
            f"useful_calls_n_gt_0={useful_calls}\n"
            f"total_events_consumed={total_events}\n"
            f"approx_run_duration_s={run_s}\n"
            f"avg_events_per_sec={avg_eps:.1f}\n"
            f"hit_rate_useful_over_total={useful_calls}/{total_calls}"
            f" = {useful_calls / total_calls:.4f}\n"
            f"csv={out_csv}\n"
        )
    print(f"OK: wrote {out_csv} and {summary_txt}")
    print(f"     events={total_events} over ~{run_s}s -> {avg_eps:.1f} ev/s"
          f" ({useful_calls}/{total_calls} useful polls)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
