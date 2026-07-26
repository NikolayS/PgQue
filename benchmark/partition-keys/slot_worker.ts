/**
 * slot_worker.ts -- partition-keys slot-worker driver (bun + node-postgres).
 *
 * Drives the LEASE consume loop against the REAL installed pgque v0.8 API
 * (sql/pgque-api/partition_keys.sql) -- claim_slot / receive_partitioned /
 * ack_partitioned / release_slot. No demo schema; every call is a plain
 * autocommit query, so it works exactly as a transaction-mode-pooled worker
 * would.
 *
 * One async loop per slot, each on its own connection, each with a stable
 * worker id `${WORKER_PREFIX}${slot}`. A loop:
 *   1. claim_slot(queue, consumer, slot, worker, ttl); null => back off, retry.
 *   2. sticky drain: receive_partitioned(max=BATCH); while it returns events,
 *      ack_partitioned and log one line per ack; renewal rides receive/ack.
 *   3. after IDLE_ROUNDS empty polls, release_slot and sleep IDLE_MS, then
 *      re-claim and poll again.
 *
 * The runner runs steady phases as ONE process covering all slots
 * (SLOTS=0-<N-1>) and the stalled-slot phase as two processes -- the target
 * slot alone (so it can be SIGSTOPped) plus the rest -- via the SLOTS spec.
 *
 * Env (all optional except where noted):
 *   PGHOST / PGDATABASE / PGUSER   libpq connection (defaults: bench db)
 *   QUEUE          queue name                       (default bench_q)
 *   CONSUMER       partitioned consumer name         (default w16)
 *   N_SLOTS        pinned slot count N               (default 16)
 *   SLOTS          slot spec, e.g. "0-15" | "7" | "0-6,8-15" (default 0..N-1)
 *   BATCH          receive_partitioned max           (default 500)
 *   TTL_S          lease ttl seconds                 (default 30)
 *   IDLE_MS        sleep between empty drain cycles   (default 200)
 *   IDLE_ROUNDS    empty polls before release        (default 2)
 *   WORKER_PREFIX  worker-id prefix                  (default w)
 *   LOG            ack-log file to append; '' = stdout (default stdout)
 *   RUN_S          run seconds then exit; 0 = until SIGTERM (default 0)
 */
import pg from "pg";
import { appendFileSync } from "node:fs";
const { Client } = pg;

const QUEUE = process.env.QUEUE || "bench_q";
const CONSUMER = process.env.CONSUMER || "w16";
const N_SLOTS = Number(process.env.N_SLOTS || 16);
const BATCH = Number(process.env.BATCH || 500);
const TTL_S = Number(process.env.TTL_S || 30);
const IDLE_MS = Number(process.env.IDLE_MS || 200);
const IDLE_ROUNDS = Number(process.env.IDLE_ROUNDS || 2);
const WORKER_PREFIX = process.env.WORKER_PREFIX || "w";
const LOG = process.env.LOG || "";
const RUN_S = Number(process.env.RUN_S || 0);

let stopped = false;
process.on("SIGTERM", () => (stopped = true));
process.on("SIGINT", () => (stopped = true));

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function parseSlots(spec: string | undefined, n: number): number[] {
  if (!spec) return Array.from({ length: n }, (_, i) => i);
  const out: number[] = [];
  for (const part of spec.split(",")) {
    const t = part.trim();
    if (t === "") continue;
    if (t.includes("-")) {
      const [a, b] = t.split("-").map((x) => Number(x));
      for (let i = a; i <= b; i++) out.push(i);
    } else {
      out.push(Number(t));
    }
  }
  return out;
}

function logAck(line: string) {
  if (LOG) appendFileSync(LOG, line + "\n");
  else console.log(line);
}

function newClient() {
  return new Client({
    host: process.env.PGHOST,
    database: process.env.PGDATABASE || "bench",
    user: process.env.PGUSER || process.env.USER || "postgres",
  });
}

const deadline = () => (RUN_S > 0 ? Date.now() + RUN_S * 1000 : Infinity);

async function slotLoop(slot: number, endAt: number): Promise<void> {
  const worker = `${WORKER_PREFIX}${slot}`;
  const c = newClient();
  await c.connect();
  try {
    while (!stopped && Date.now() < endAt) {
      const claim = await c.query(
        "select pgque.claim_slot($1,$2,$3,$4, make_interval(secs => $5)) as epoch",
        [QUEUE, CONSUMER, slot, worker, TTL_S],
      );
      if (claim.rows[0].epoch === null) {
        // Held by another live worker (only happens across processes) -- wait.
        await sleep(IDLE_MS);
        continue;
      }

      let idle = 0;
      while (!stopped && Date.now() < endAt) {
        const r = await c.query(
          "select msg_id from pgque.receive_partitioned($1,$2,$3,$4,$5,$6)",
          [QUEUE, CONSUMER, slot, N_SLOTS, worker, BATCH],
        );
        const got = r.rows.length;
        // ack finishes the whole batch (finish_batch); a no-op when the empty
        // filtered batch was already auto-finished inside receive_partitioned.
        await c.query("select pgque.ack_partitioned($1,$2,$3,$4,$5)", [
          QUEUE,
          CONSUMER,
          slot,
          N_SLOTS,
          worker,
        ]);
        if (got > 0) {
          let maxId = 0;
          for (const row of r.rows) if (+row.msg_id > maxId) maxId = +row.msg_id;
          logAck(`${new Date().toISOString()},${worker},${slot},${got},${maxId}`);
          idle = 0;
        } else if (++idle >= IDLE_ROUNDS) {
          break;
        }
      }

      await c.query("select pgque.release_slot($1,$2,$3,$4)", [
        QUEUE,
        CONSUMER,
        slot,
        worker,
      ]);
      await sleep(IDLE_MS);
    }
  } finally {
    await c.end().catch(() => {});
  }
}

const slots = parseSlots(process.env.SLOTS, N_SLOTS);
const endAt = deadline();
console.error(
  `slot_worker: queue=${QUEUE} consumer=${CONSUMER} N=${N_SLOTS} slots=[${slots.join(",")}] batch=${BATCH} ttl=${TTL_S}s run=${RUN_S || "until-SIGTERM"}`,
);
await Promise.all(slots.map((s) => slotLoop(s, endAt)));
console.error("slot_worker: all slot loops exited");
