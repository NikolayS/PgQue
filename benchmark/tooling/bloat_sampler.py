#!/usr/bin/env python3
"""bloat_sampler.py — sample queue table bloat every N seconds.
For pgboss, also samples all partitions of pgboss.job.
Output CSV: ts,table,n_live_tup,n_dead_tup,heap_bytes,toast_bytes,index_bytes,total_bytes
"""
import argparse, time, sys
from datetime import datetime, timezone
import psycopg2

DSN = "host=127.0.0.1 dbname=bench user=postgres"

# Per-system base queries; pgboss gets special treatment (partitions)
TABLES_QUERY = {
    "pgque": """
        SELECT schemaname||'.'||relname FROM pg_stat_user_tables
        WHERE schemaname = 'pgque' ORDER BY 1
    """,
    "pgq": """
        SELECT schemaname||'.'||relname FROM pg_stat_user_tables
        WHERE schemaname = 'pgq' ORDER BY 1
    """,
    "pgmq": """
        SELECT schemaname||'.'||relname FROM pg_stat_user_tables
        WHERE schemaname = 'pgmq' AND relname LIKE 'q_%' ORDER BY 1
    """,
    "river":  "SELECT 'public.river_job'",
    "que":    "SELECT 'public.que_jobs'",
    # pgboss: include parent + all partitions of pgboss.job + archive
    "pgboss": """
        SELECT n.nspname||'.'||c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'pgboss'
          AND (c.relname IN ('job','archive')
               OR c.relispartition
               OR c.relkind = 'p')
        ORDER BY 1
    """,
    "pgmq-partitioned": """
        SELECT n.nspname||'.'||c.relname
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'pgmq'
          AND (c.relname LIKE 'q_%' OR c.relname LIKE 'a_%')
          AND (c.relispartition OR c.relkind = 'p' OR c.relkind = 'r')
        ORDER BY 1
    """,
}

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--system", required=True)
    ap.add_argument("--interval", type=int, default=30)
    ap.add_argument("--duration", type=int, default=1800)
    args = ap.parse_args()

    conn = psycopg2.connect(DSN); conn.autocommit = True
    cur = conn.cursor()

    print("ts,table,n_live_tup,n_dead_tup,heap_bytes,toast_bytes,index_bytes,total_bytes", flush=True)
    t_end = time.monotonic() + args.duration
    while time.monotonic() < t_end:
        ts = datetime.now(timezone.utc).isoformat()
        try:
            cur.execute(TABLES_QUERY[args.system])
            tables = [r[0] for r in cur.fetchall()]
            for tbl in tables:
                cur.execute("""
                    SELECT
                        COALESCE((SELECT n_live_tup FROM pg_stat_user_tables
                                  WHERE schemaname||'.'||relname = %s), 0),
                        COALESCE((SELECT n_dead_tup FROM pg_stat_user_tables
                                  WHERE schemaname||'.'||relname = %s), 0),
                        COALESCE(pg_relation_size(%s::regclass), 0),
                        COALESCE(pg_table_size(%s::regclass) - pg_relation_size(%s::regclass), 0),
                        COALESCE(pg_indexes_size(%s::regclass), 0),
                        COALESCE(pg_total_relation_size(%s::regclass), 0)
                """, (tbl, tbl, tbl, tbl, tbl, tbl, tbl))
                row = cur.fetchone()
                print(f"{ts},{tbl},{row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]}", flush=True)
        except Exception as e:
            print(f"# err: {e}", file=sys.stderr, flush=True)
        time.sleep(args.interval)

    conn.close()

if __name__ == "__main__":
    main()
