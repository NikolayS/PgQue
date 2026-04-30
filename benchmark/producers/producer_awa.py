#!/usr/bin/env python3
"""producer_awa.py -- enqueue jobs into awa at -R <rate> per second for <duration> seconds.
Mirrors pgbench's -R 2000 -T 3000 contract used for the other systems.
"""
import asyncio, os, time, sys, signal
from dataclasses import dataclass
import awa

DATABASE_URL = os.environ.get("DATABASE_URL", "postgres://postgres@127.0.0.1:5432/bench")
DURATION = int(os.environ.get("DURATION", "3000"))
RATE = int(os.environ.get("RATE", "2000"))

@dataclass
class BenchJob:
    payload: str

async def main() -> None:
    client = awa.AsyncClient(DATABASE_URL)
    # Register the type so insert() knows the queue mapping. We DO NOT call
    # client.start() here — this script is producer-only.
    @client.task(BenchJob, queue="bench")
    async def _noop(job): pass

    burst_size = 50
    burst_period_s = burst_size / RATE   # at 2000/s × burst 50 → 25 ms between bursts
    deadline = time.monotonic() + DURATION
    next_burst = time.monotonic()
    n = 0

    while time.monotonic() < deadline:
        await asyncio.sleep(max(0, next_burst - time.monotonic()))
        await asyncio.gather(*[
            client.insert(BenchJob(payload=str(n + i)), queue="bench")
            for i in range(burst_size)
        ])
        n += burst_size
        next_burst += burst_period_s
        # Match pgbench progress: print every ~30 s
        if n % (RATE * 30) == 0:
            elapsed = DURATION - (deadline - time.monotonic())
            tps = n / max(elapsed, 0.001)
            print(f"progress: {elapsed:.1f} s, {tps:.1f} tps", flush=True)

    elapsed = DURATION - (deadline - time.monotonic())
    print(f"done: produced {n} jobs in {elapsed:.1f}s (avg {n/elapsed:.0f} tps)", flush=True)
    await client.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
