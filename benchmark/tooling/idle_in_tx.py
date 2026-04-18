#!/usr/bin/env python3
"""idle_in_tx.py — open a REPEATABLE READ transaction and hold xmin forever.
Brandur's recipe for reproducing the death spiral. Kill with SIGTERM.
"""
import time, signal, sys
import psycopg2

DSN = "host=127.0.0.1 dbname=bench user=postgres application_name=idle_in_tx"

conn = psycopg2.connect(DSN)
conn.autocommit = False
cur = conn.cursor()
cur.execute("BEGIN ISOLATION LEVEL REPEATABLE READ")
cur.execute("SELECT 1")
print(f"idle_in_tx: holding xmin via pid=", conn.get_backend_pid(), flush=True)

def shutdown(signum, frame):
    try:
        conn.rollback()
    except: pass
    sys.exit(0)

signal.signal(signal.SIGTERM, shutdown)
signal.signal(signal.SIGINT, shutdown)
while True:
    time.sleep(60)
    # Keep the transaction alive with a trivial query
    try:
        cur.execute("SELECT 1")
    except: shutdown(None, None)
