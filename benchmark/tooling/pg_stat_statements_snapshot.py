#!/usr/bin/env python3
"""pg_stat_statements_snapshot.py — take periodic pg_stat_statements
snapshots. For each snapshot, record (ts, queryid, query_head, calls, rows,
total_exec_time_ms). Output appended as CSV — the downstream analyzer
diffs consecutive snapshots per queryid to obtain per-interval rates.

Used as a cross-check for the NOTICE-based instrumentation, and as a
fallback for systems where the DO-block wrapper makes statement-level
rows invisible (pgque, pgq, pgmq).
"""
from __future__ import annotations
import argparse, csv, sys, time
from datetime import datetime, timezone
import psycopg2

DSN_DEFAULT = "host=127.0.0.1 dbname=bench user=postgres"

SAMPLE_SQL = """
SELECT
    queryid::text,
    left(regexp_replace(query, '\\s+', ' ', 'g'), 200) AS query_head,
    calls,
    rows,
    total_exec_time::bigint AS total_exec_time_ms
FROM pg_stat_statements
WHERE query ~ ANY (ARRAY[
    'next_batch', 'pgque\\.ticker', 'pgq\\.ticker',
    'pgmq\\.read', 'pgmq\\.delete',
    'river_job', 'que_jobs', 'pgboss\\.job'
])
ORDER BY calls DESC
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=DSN_DEFAULT)
    ap.add_argument("--interval", type=int, default=10)
    ap.add_argument("--duration", type=int, default=5400)
    ap.add_argument("--out", default="/tmp/bench/pgss_timeseries.csv")
    args = ap.parse_args()

    conn = psycopg2.connect(args.dsn)
    conn.autocommit = True
    cur = conn.cursor()

    f = open(args.out, "w", newline="")
    w = csv.writer(f)
    w.writerow(["ts", "queryid", "query_head", "calls", "rows", "total_exec_time_ms"])
    f.flush()

    t_end = time.monotonic() + args.duration
    try:
        while time.monotonic() < t_end:
            ts = datetime.now(timezone.utc).isoformat()
            try:
                cur.execute(SAMPLE_SQL)
                rows = cur.fetchall()
                for r in rows:
                    w.writerow([ts, *r])
                f.flush()
            except Exception as e:  # noqa: BLE001
                print(f"# pgss snap err: {e}", file=sys.stderr, flush=True)
            time.sleep(args.interval)
    finally:
        f.close()
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
