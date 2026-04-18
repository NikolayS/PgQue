#!/usr/bin/env python3
"""pgq_ticker_daemon.py — tight loop calling pgq.ticker + pgq.maint_operations.
Replaces the 1-min pg_cron invocation to match pgque's inline cadence.
Run as: sudo -u postgres python3 /root/pgq_ticker_daemon.py
"""
import time, signal, sys
import psycopg2

DSN = "host=127.0.0.1 dbname=bench user=postgres application_name=pgq_ticker"
conn = psycopg2.connect(DSN); conn.autocommit = True
cur = conn.cursor()

def shutdown(signum, frame):
    try: conn.close()
    except: pass
    sys.exit(0)
signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)

print("pgq_ticker_daemon: starting loop", flush=True)
last_maint = 0
while True:
    try:
        cur.execute("SELECT pgq.ticker()")
        # Run maint_operations() periodically (every 5s) — each op in its own implicit tx
        now = time.time()
        if now - last_maint >= 5:
            cur.execute("SELECT func_name, func_arg FROM pgq.maint_operations()")
            for fn, arg in cur.fetchall():
                try:
                    if arg is None:
                        cur.execute(f"SELECT {fn}()")
                    else:
                        cur.execute(f"SELECT {fn}(%s)", (arg,))
                except Exception as me:
                    # Some operations are expected to fail (e.g. NOWAIT lock) — ignore
                    pass
            last_maint = now
    except Exception as e:
        print(f"ticker err: {e}", file=sys.stderr, flush=True)
        time.sleep(1)
    time.sleep(1)  # 1 Hz ticker
