#!/usr/bin/env python3
"""Smoke-test benchmark setup and delivery against an installed PgQue database.

Requires psycopg and a disposable database with sql/pgque.sql installed.
This checks compatibility, not latency or pg_cron scheduling.
"""
import argparse
from contextlib import closing
from datetime import timedelta
import uuid

import psycopg

import bench


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dsn", required=True)
    args = parser.parse_args()
    queue = "tick_setup_" + uuid.uuid4().hex
    consumer = "smoke"
    try:
        bench.setup(args.dsn, queue, consumer)
        with closing(psycopg.connect(args.dsn, autocommit=True)) as cn:
            with cn.cursor() as cur:
                cur.execute(
                    "select queue_ticker_max_count, queue_ticker_max_lag, "
                    "queue_ticker_idle_period from pgque.queue where queue_name=%s",
                    (queue,),
                )
                count, lag, idle = cur.fetchone()
                if count != 1 or not all(
                    timedelta(0) < interval < timedelta(milliseconds=1)
                    for interval in (lag, idle)
                ):
                    raise RuntimeError("setup must support every scheduler period, including 1 ms")
                for payload in ("first", "second"):
                    cur.execute("select pgque.send(%s, %s::text)", (queue, payload))
                # The configured triggers must admit this committed batch
                # without force_next_tick or a scheduler extension.
                cur.execute("select pgque.ticker(%s)", (queue,))
                if cur.fetchone()[0] is None:
                    raise RuntimeError("configured ticker did not materialize a tick")
                cur.execute("select payload,batch_id from pgque.receive(%s,%s,2)", (queue, consumer))
                rows = cur.fetchall()
                if sorted(row[0] for row in rows) != ["first", "second"]:
                    raise RuntimeError(f"incomplete delivery: {rows!r}")
                if len({row[1] for row in rows}) != 1:
                    raise RuntimeError("expected one complete batch")
                cur.execute("select pgque.ack(%s)", (rows[0][1],))
                if cur.fetchone()[0] != 1:
                    raise RuntimeError("ack did not finish the batch")
    finally:
        # Do not use bench.teardown here: it intentionally suppresses errors.
        with closing(psycopg.connect(args.dsn, autocommit=True)) as cn:
            with cn.cursor() as cur:
                cur.execute("select 1 from pgque.queue where queue_name=%s", (queue,))
                if cur.fetchone():
                    cur.execute("select pgque.drop_queue(%s,true)", (queue,))
                cur.execute("select count(*) from pgque.queue where queue_name=%s", (queue,))
                if cur.fetchone()[0] != 0:
                    raise RuntimeError("test queue cleanup failed")
    print("PASS: benchmark setup, tick, complete delivery, ack, and cleanup")


if __name__ == "__main__":
    main()
