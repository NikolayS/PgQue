#!/usr/bin/env python3
"""consumer_awa.py -- run 4 awa workers on the 'bench' queue for <duration> seconds.
Emits NOTICE-style ev-rate lines (1 / sec) so parse_events_consumed.py can produce
events_consumed_per_sec.csv exactly like the other systems.
"""
import asyncio, os, time, sys
from dataclasses import dataclass
import awa

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://postgres@127.0.0.1:5432/bench")
DURATION = int(os.environ.get("DURATION", "3000"))
WORKERS  = int(os.environ.get("WORKERS", "4"))

@dataclass
class BenchJob:
    payload: str

# Use a module-level counter so the handler can update it lock-free under asyncio.
consumed = 0

async def main() -> None:
    global consumed
    client = awa.AsyncClient(DATABASE_URL)

    @client.task(BenchJob, queue="bench")
    async def _handle(job):
        global consumed
        consumed += 1
        # No-op handler. Just count.

    await client.start([("bench", WORKERS)])

    deadline = time.monotonic() + DURATION
    last = consumed
    last_t = time.monotonic()
    while time.monotonic() < deadline:
        await asyncio.sleep(1.0)
        now = time.monotonic()
        delta = consumed - last
        # NOTICE format matches what parse_events_consumed.py expects from pgbench.
        print(f"NOTICE:  ev ts={int(time.time())} n={delta}", flush=True)
        last = consumed
        last_t = now

    print(f"done: consumed {consumed} jobs in {DURATION}s", flush=True)
    await client.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
