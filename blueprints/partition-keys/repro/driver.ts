/**
 * Partition-keys reproduction driver (TypeScript / bun + node-postgres).
 *
 * Produces a keyed workload, drains it with N concurrent workers, measures
 * throughput, and checks the design's guarantees empirically against a real
 * pgque install.
 *
 *   Tier A (mutual exclusion):  cooperative consumers + per-key advisory lock.
 *   Tier B (ordered per key):   N hash-routed slot subscriptions.
 *
 * Run:
 *   PGDATABASE=pgque_repro bun driver.ts --tier a --tenants 2000 --dups 4 --workers 8 --work-ms 3
 *   PGDATABASE=pgque_repro bun driver.ts --tier b --tenants 500 --events-per-tenant 20 --slots 8
 *
 * Connection: standard libpq env vars (PGDATABASE/PGHOST/PGUSER/...). Defaults
 * to database "pgque_repro".
 */
import pg from "pg";
const { Client } = pg;

const SEED = 1234;

// ---- tiny seeded RNG (mulberry32) for a reproducible shuffle ---------------
function rng(seed: number): () => number {
  let a = seed >>> 0;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
function shuffle<T>(arr: T[], rand: () => number): T[] {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}

function newClient() {
  // Honor libpq env. bun does not populate process.env.USER and its
  // os.userInfo() returns "unknown", so for peer auth the role must come from
  // PGUSER/USER (run.sh sets PGUSER); fall back to "postgres".
  return new Client({
    host: process.env.PGHOST,
    database: process.env.PGDATABASE || "pgque_repro",
    user: process.env.PGUSER || process.env.USER || "postgres",
  });
}
async function withClient<T>(fn: (c: pg.Client) => Promise<T>): Promise<T> {
  const c = newClient();
  await c.connect();
  try {
    return await fn(c);
  } finally {
    await c.end();
  }
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function resetQueue(c: pg.Client, queue: string) {
  // A prior run may have left cooperative members registered; drop_queue
  // refuses a queue that still has coop_members (pgque.sql), so tear them down
  // explicitly first (i_batch_handling=1 forces any leftover open batch).
  const members = await c
    .query(
      `select co.co_name
         from pgque.subscription s
         join pgque.queue q on q.queue_id = s.sub_queue
         join pgque.consumer co on co.co_id = s.sub_consumer
        where q.queue_name = $1 and s.sub_role = 'coop_member'`,
      [queue],
    )
    .catch(() => ({ rows: [] as { co_name: string }[] }));
  for (const m of members.rows) {
    const dot = m.co_name.indexOf(".");
    const main = m.co_name.slice(0, dot);
    const sub = m.co_name.slice(dot + 1);
    await c
      .query("select pgque.unregister_subconsumer($1,$2,$3,1)", [queue, main, sub])
      .catch(() => {});
  }
  try {
    await c.query("select pgque.drop_queue($1, true)", [queue]);
  } catch (e: any) {
    if (!String(e.message).includes("No such event queue")) throw e;
  }
  await c.query("select pgque.create_queue($1)", [queue]);
}
async function tick(c: pg.Client, queue: string) {
  await c.query("select pgque.force_tick($1)", [queue]);
  await c.query("select pgque.ticker($1)", [queue]);
}
const fmt = (n: number) => Math.round(n).toLocaleString("en-US");
function check(label: string, passed: boolean, detail = ""): boolean {
  const mark = passed ? "PASS" : "FAIL";
  console.log(`    [${mark}] ${label}${passed ? "" : `  <-- ${detail}`}`);
  return passed;
}

interface Args {
  tier: string;
  tenants: number;
  workers: number;
  dups: number;
  workMs: number;
  producers: number;
  dedupTtl: number; // seconds; 0 = producer dedup OFF
  slots: number;
  eventsPerTenant: number;
  chunk: number;
  maxBatch: number;
}

// ---------------------------------------------------------------------------
// Tier A — mutual exclusion (migration-style workload)
// ---------------------------------------------------------------------------
async function runTierA(a: Args): Promise<boolean> {
  const queue = "migrations";
  const main = "mig";
  const workers = Array.from({ length: a.workers }, (_, i) => `w${i}`);
  const dedupOn = a.dedupTtl > 0;

  await withClient(async (c) => {
    await c.query("select demo.reset()");
    await c.query("truncate demo.idem");
    await resetQueue(c, queue);
    for (const w of workers)
      await c.query("select pgque.register_subconsumer($1,$2,$3)", [queue, main, w]);
  });

  // --- producer phase: P concurrent producers, each attempting every tenant
  // `dups` times (simulating many app instances racing on the same tenants).
  // A background ticker runs continuously (as pg_cron would), creating many
  // tick windows so the cooperative consumer can spread the load.
  const attempts = a.producers * a.tenants * a.dups;
  let inserted = 0; // counted producer-side from send_idem / produce
  let producing = true;
  const tickerTask = withClient(async (c) => {
    while (producing) {
      await tick(c, queue);
      await sleep(25);
    }
    await tick(c, queue);
  });

  const t0 = performance.now();
  await Promise.all(
    Array.from({ length: a.producers }, (_, p) =>
      withClient(async (c) => {
        const rand = rng(SEED + p);
        let local = 0;
        for (let d = 0; d < a.dups; d++) {
          const order = shuffle(Array.from({ length: a.tenants }, (_, i) => i), rand);
          for (const t of order) {
            const key = `tenant-${t}`;
            if (dedupOn) {
              const r = await c.query(
                "select deduped from demo.send_idem($1,$2,$3,$4,$5, make_interval(secs => $6))",
                [queue, "migrate", '{"op":"ensure_latest"}', key, `migrate:${key}`, a.dedupTtl],
              );
              if (!r.rows[0].deduped) local++;
            } else {
              await c.query("select demo.produce($1,$2,$3,$4)", [
                queue, "migrate", '{"op":"ensure_latest"}', key,
              ]);
              local++;
            }
          }
        }
        inserted += local;
      }),
    ),
  );
  producing = false;
  await tickerTask;
  const produceS = (performance.now() - t0) / 1000;
  const total = inserted; // events actually appended to the log

  // drain with N concurrent workers (each its own connection)
  const stats = new Map(workers.map((w) => [w, { got: 0, ran: 0, dropped: 0 }]));
  const t1 = performance.now();
  await Promise.all(
    workers.map((w) =>
      withClient(async (c) => {
        let empty = 0;
        const s = stats.get(w)!;
        while (empty < 2) {
          const r = await c.query(
            "select got, ran, dropped from demo.tier_a_consume($1,$2,$3,$4,$5)",
            [queue, main, w, a.maxBatch, a.workMs],
          );
          const { got, ran, dropped } = r.rows[0];
          s.got += +got;
          s.ran += +ran;
          s.dropped += +dropped;
          empty = +got === 0 ? empty + 1 : 0;
          if (+got === 0) await sleep(10);
        }
      }),
    ),
  );
  const drainS = (performance.now() - t1) / 1000;

  return withClient(async (c) => {
    const got = [...stats.values()].reduce((x, s) => x + s.got, 0);
    const ran = [...stats.values()].reduce((x, s) => x + s.ran, 0);
    const dropped = [...stats.values()].reduce((x, s) => x + s.dropped, 0);
    const migrated = +(await c.query("select count(*) from demo.tenant_migrated")).rows[0].count;
    const doubleRun = +(
      await c.query("select count(*) from demo.tenant_migrated where runs > 1")
    ).rows[0].count;
    const overlaps = +(
      await c.query(`
        select count(*) from demo.mutex_log a join demo.mutex_log b
          on a.part_key = b.part_key and a.worker <> b.worker and a.id < b.id
         and a.started_at < b.ended_at and b.started_at < a.ended_at`)
    ).rows[0].count;
    const unfinished = +(
      await c.query("select count(*) from demo.mutex_log where ended_at is null")
    ).rows[0].count;

    const perWorker = workers.map((w) => `${w}:${stats.get(w)!.ran}`).join(", ");
    console.log(`  setup                   : ${a.tenants} tenants, ${a.producers} concurrent producers, ${a.dups} attempts each`);
    console.log(`  producer dedup          : ${dedupOn ? `ON  (TTL window ${a.dedupTtl}s)` : "OFF"}`);
    console.log(`  [L1 producer] attempts  : ${attempts}`);
    console.log(`  [L1 producer] INSERTED  : ${total}   (deduped at the door: ${attempts - total})`);
    console.log(`  produce time            : ${produceS.toFixed(2)}s`);
    console.log(`  drain time              : ${drainS.toFixed(2)}s   (${a.workers} workers, work-ms=${a.workMs})`);
    console.log(`  consume throughput      : ${fmt(got / drainS)} events/s`);
    console.log(`  [L2 consumer] jobs RUN  : ${ran}`);
    console.log(`  [L2 consumer] ack-dropped: ${dropped}   (duplicate/contended, collapsed at consume)`);
    console.log(`  per-worker ran          : { ${perWorker} }`);
    console.log("  ---- invariants ----");
    let ok = true;
    if (dedupOn)
      ok = check("L1 producer dedup: inserted == distinct tenants", total === a.tenants, `inserted=${total}, tenants=${a.tenants}`) && ok;
    ok = check("L2 G2 mutual exclusion: no overlapping runs per key", overlaps === 0, `${overlaps} overlaps`) && ok;
    ok = check("L2 no double-run per tenant (runs==1)", doubleRun === 0, `${doubleRun} tenants ran >1x`) && ok;
    ok = check("every tenant migrated exactly once", ran === a.tenants && migrated === a.tenants, `ran=${ran}, migrated=${migrated}, expected=${a.tenants}`) && ok;
    ok = check("every inserted event consumed once", got === total, `got=${got}, inserted=${total}`) && ok;
    ok = check("all processing windows closed", unfinished === 0, `${unfinished} unfinished`) && ok;
    return ok;
  });
}

// ---------------------------------------------------------------------------
// Tier B — ordered per key (lifecycle workload)
// ---------------------------------------------------------------------------
async function runTierB(a: Args): Promise<boolean> {
  const queue = "lifecycle";
  const n = a.slots;
  const types = ["FileCreated", "FileOverwritten", "FileDeleted"];

  const produceS = await withClient(async (c) => {
    await c.query("select demo.reset()");
    await resetQueue(c, queue);
    for (let k = 0; k < n; k++)
      await c.query("select pgque.register_consumer($1,$2)", [queue, `life_slot_${k}`]);

    // ordered lifecycle events, interleaved across tenants so each tick window
    // holds a mix of keys; per-tenant emission order is preserved (round j emits
    // each tenant's j-th event) → ev_id monotonic per key.
    const t0 = performance.now();
    let pending = 0;
    for (let j = 0; j < a.eventsPerTenant; j++) {
      const etype = j === 0 ? types[0] : j === a.eventsPerTenant - 1 ? types[2] : types[1];
      for (let t = 0; t < a.tenants; t++) {
        await c.query("select demo.produce($1,$2,$3,$4)", [
          queue,
          etype,
          `{"seq":${j}}`,
          `tenant-${t}`,
        ]);
        if (++pending >= a.chunk) {
          await tick(c, queue);
          pending = 0;
        }
      }
    }
    if (pending) await tick(c, queue);
    return (performance.now() - t0) / 1000;
  });
  const total = a.tenants * a.eventsPerTenant;

  // drain: static assignment — worker k owns slot k
  const agg = { scanned: 0, delivered: 0 };
  const t1 = performance.now();
  await Promise.all(
    Array.from({ length: n }, (_, k) =>
      withClient(async (c) => {
        let empty = 0;
        while (empty < 2) {
          const r = await c.query(
            "select scanned, delivered from demo.tier_b_consume($1,$2,$3,$4,$5)",
            [queue, `life_slot_${k}`, k, n, a.maxBatch],
          );
          const sc = +r.rows[0].scanned;
          agg.scanned += sc;
          agg.delivered += +r.rows[0].delivered;
          empty = sc === 0 ? empty + 1 : 0;
          if (sc === 0) await sleep(10);
        }
      }),
    ),
  );
  const drainS = (performance.now() - t1) / 1000;

  return withClient(async (c) => {
    const consumed = +(await c.query("select count(*) from demo.consume_log")).rows[0].count;
    const distinct = +(await c.query("select count(distinct msg_id) from demo.consume_log")).rows[0].count;
    const multislot = +(
      await c.query(`select count(*) from (
        select part_key from demo.consume_log group by part_key
        having count(distinct slot) > 1) z`)
    ).rows[0].count;
    const outoforder = +(
      await c.query(`select count(*) from (
        select msg_id, lag(msg_id) over (partition by part_key order by seq) as prev
        from demo.consume_log) t
        where prev is not null and msg_id < prev`)
    ).rows[0].count;

    const amp = agg.delivered ? agg.scanned / agg.delivered : 0;
    console.log(`  produced events         : ${total}  (${a.tenants} tenants x ${a.eventsPerTenant} events)`);
    console.log(`  produce time            : ${produceS.toFixed(2)}s`);
    console.log(`  drain time              : ${drainS.toFixed(2)}s   (${n} slots = ${n} workers)`);
    console.log(`  consume throughput      : ${fmt(consumed / drainS)} events/s`);
    console.log(`  read amplification      : ${amp.toFixed(2)}x   (scanned ${fmt(agg.scanned)} / delivered ${fmt(agg.delivered)}; ideal = N = ${n})`);
    console.log("  ---- invariants ----");
    let ok = true;
    ok = check("delivered exactly once (no loss, no dup)", consumed === total && distinct === total, `consumed=${consumed}, distinct=${distinct}, produced=${total}`) && ok;
    ok = check("G1 affinity: each key on exactly one slot", multislot === 0, `${multislot} keys on >1 slot`) && ok;
    ok = check("G1 FIFO: per-key msg_id non-decreasing", outoforder === 0, `${outoforder} out-of-order`) && ok;
    return ok;
  });
}

// ---------------------------------------------------------------------------
function parseArgs(): Args {
  const a: any = {
    tier: "", tenants: 1000, workers: 8, dups: 4, workMs: 3,
    producers: 4, dedupTtl: 0,
    slots: 8, eventsPerTenant: 20, chunk: 500, maxBatch: 100000,
  };
  const map: Record<string, keyof Args> = {
    "--tier": "tier", "--tenants": "tenants", "--workers": "workers",
    "--dups": "dups", "--work-ms": "workMs", "--producers": "producers",
    "--dedup-ttl": "dedupTtl", "--slots": "slots",
    "--events-per-tenant": "eventsPerTenant", "--chunk": "chunk", "--max-batch": "maxBatch",
  };
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i += 2) {
    const k = map[argv[i]];
    if (!k) throw new Error(`unknown arg ${argv[i]}`);
    (a as any)[k] = k === "tier" ? argv[i + 1] : Number(argv[i + 1]);
  }
  if (a.tier !== "a" && a.tier !== "b") throw new Error("--tier must be a or b");
  return a as Args;
}

const args = parseArgs();
const ok = await (args.tier === "a" ? runTierA(args) : runTierB(args));
process.exit(ok ? 0 : 1);
