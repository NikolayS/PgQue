#!/usr/bin/env python3
"""pk_ticker.py -- tight ticker + maintenance loop for the partition-keys bench.

Calls pgque.ticker() every TICK_MS (default 250 ms) so slot cursors advance
promptly, and pgque.maint() every MAINT_S (default 60 s) so event tables rotate
and vacuum on the pgque cadence. Persistent autocommit connection, mirroring
tooling/pgq_ticker_daemon.py.

Env: PGHOST/PGDATABASE/PGUSER (libpq), TICK_MS, MAINT_S, RUN_S (0 = forever).
"""
import os
import signal
import sys
import time

import psycopg2

DSN = (
    f"host={os.environ.get('PGHOST', '127.0.0.1')} "
    f"dbname={os.environ.get('PGDATABASE', 'bench')} "
    f"user={os.environ.get('PGUSER', 'postgres')} "
    "application_name=pk_ticker"
)
TICK_S = float(os.environ.get("TICK_MS", "250")) / 1000.0
MAINT_S = float(os.environ.get("MAINT_S", "60"))
RUN_S = float(os.environ.get("RUN_S", "0"))

conn = psycopg2.connect(DSN)
conn.autocommit = True
cur = conn.cursor()


def shutdown(signum, frame):
    try:
        conn.close()
    except Exception:
        pass
    sys.exit(0)


signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

print(f"pk_ticker: tick={TICK_S}s maint={MAINT_S}s run={RUN_S or 'forever'}", flush=True)
t_start = time.monotonic()
last_maint = 0.0
while True:
    now = time.monotonic()
    if RUN_S and now - t_start >= RUN_S:
        break
    try:
        cur.execute("select pgque.ticker()")
        if now - last_maint >= MAINT_S:
            cur.execute("select pgque.maint()")
            last_maint = now
    except Exception as e:  # noqa: BLE001
        print(f"pk_ticker err: {e}", file=sys.stderr, flush=True)
        time.sleep(1)
    time.sleep(TICK_S)

conn.close()
