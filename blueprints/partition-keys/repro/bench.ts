/**
 * PgQue vs pg-boss-style mutable queue — bloat-under-backlog + throughput.
 *
 * The question that actually triggered Fabrizio: when workers fall behind, does
 * the store bloat? PgQue is append-only + rotation (TRUNCATE whole tables);
 * a mutable job table is insert->update->delete (dead tuples, vacuum, bloat).
 *
 *   bun bench.ts --mode bloat       --build-sec 30 --consume-rate 4000
 *   bun bench.ts --mode throughput  --dur 15
 *
 * Connection: libpq env (PGHOST/PGDATABASE/PGUSER). Default db pgque_repro.
 */
import pg from "pg";
const { Client } = pg;

function newClient() {
  return new Client({
    host: process.env.PGHOST,
    database: process.env.PGDATABASE || "pgque_repro",
    user: process.env.PGUSER || process.env.USER || "postgres",
    connectionTimeoutMillis: 5000,
    // fail fast instead of hanging if the server is reaped mid-run
    options: "-c statement_timeout=30000",
  });
}
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const now = () => performance.now();
const MiB = (b: number) => (b / 1024 / 1024).toFixed(1) + " MiB";
const k = (n: number) => Math.round(n).toLocaleString("en-US");

interface Sample { t: number; bytes: number; live: number; dead: number; vacs: number; }

interface Engine {
  name: string;
  reset(c: pg.Client): Promise<void>;
  produce(c: pg.Client, n: number): Promise<void>; // append n events/jobs (fast)
  postProduce?(c: pg.Client): Promise<void>;        // pgque: tick to make consumable
  consume(c: pg.Client, max: number): Promise<number>;
  maint?(c: pg.Client): Promise<void>;              // pgque: rotate/reclaim
  sample(c: pg.Client): Promise<Sample>;
}

// --- PgQue: append-only events + rotation -----------------------------------
function pgqueEngine(): Engine {
  const queue = "bench_pgque";
  const consumer = "c";
  let qid = 0;
  return {
    name: "pgque (append + rotation)",
    async reset(c) {
      try { await c.query("select pgque.drop_queue($1,true)", [queue]); } catch {}
      await c.query("select pgque.create_queue($1)", [queue]);
      await c.query("select pgque.register_consumer($1,$2)", [queue, consumer]);
      // aggressive rotation so reclaim is visible within the run
      await c.query("select pgque.set_queue_config($1,'rotation_period','1 second')", [queue]);
      qid = (await c.query("select queue_id from pgque.queue where queue_name=$1", [queue])).rows[0].queue_id;
    },
    async produce(c, n) {
      // set-based bulk insert (the fair fast path, mirrors jobq's bulk INSERT)
      await c.query(
        "select pgque.insert_event_bulk($1,'e', array(select '{\"x\":1}' from generate_series(1,$2)))",
        [queue, n],
      );
    },
    async postProduce(c) {
      await c.query("select pgque.force_tick($1)", [queue]);
      await c.query("select pgque.ticker($1)", [queue]);
    },
    async consume(c, max) {
      const r = await c.query("select batch_id from pgque.receive($1,$2,$3)", [queue, consumer, max]);
      if (r.rows.length === 0) return 0;
      await c.query("select pgque.ack($1)", [r.rows[0].batch_id]);
      return r.rows.length;
    },
    async maint(c) {
      await c.query("select pgque.ticker($1)", [queue]);
      await c.query("select pgque.maint()");
    },
    async sample(c) {
      const r = await c.query(
        `select coalesce(sum(pg_total_relation_size(cl.oid)),0)::bigint bytes,
                coalesce(sum(st.n_live_tup),0)::bigint live,
                coalesce(sum(st.n_dead_tup),0)::bigint dead,
                coalesce(sum(st.vacuum_count+st.autovacuum_count),0)::bigint vacs
           from pg_class cl
           join pg_namespace ns on ns.oid=cl.relnamespace
           left join pg_stat_user_tables st on st.relid=cl.oid
          where ns.nspname='pgque' and cl.relkind='r' and cl.relname like 'event_'||$1||'_%'`,
        [qid],
      );
      const x = r.rows[0];
      return { t: 0, bytes: +x.bytes, live: +x.live, dead: +x.dead, vacs: +x.vacs };
    },
  };
}

// --- pg-boss-style: mutable rows, insert->update->delete --------------------
function jobqEngine(): Engine {
  return {
    name: "jobq (mutable: insert/update/delete)",
    async reset(c) {
      await c.query("truncate demo.jobq");
      // realistic: leave autovacuum ON (so we measure how hard it must work)
      await c.query("select pg_stat_reset_single_table_counters('demo.jobq'::regclass)");
    },
    async produce(c, n) {
      await c.query(
        "insert into demo.jobq(key,state,payload) select null,'created','{\"x\":1}' from generate_series(1,$1)",
        [n],
      );
    },
    async consume(c, max) {
      // claim (update -> active), then complete (delete): pg-boss-shaped churn,
      // two statements so we don't update+delete the same row in one (UB).
      const claimed = await c.query(
        `with cl as (
           select id from demo.jobq where state='created' order by id
           limit $1 for update skip locked)
         update demo.jobq j set state='active' from cl
          where j.id = cl.id returning j.id`,
        [max],
      );
      const ids = claimed.rows.map((r) => r.id);
      if (ids.length) await c.query("delete from demo.jobq where id = any($1::bigint[])", [ids]);
      return ids.length;
    },
    async sample(c) {
      const r = await c.query(
        `select pg_total_relation_size('demo.jobq')::bigint bytes,
                n_live_tup live, n_dead_tup dead, (vacuum_count+autovacuum_count) vacs
           from pg_stat_user_tables where relid='demo.jobq'::regclass`,
      );
      const x = r.rows[0] || { bytes: 0, live: 0, dead: 0, vacs: 0 };
      return { t: 0, bytes: +x.bytes, live: +x.live, dead: +x.dead, vacs: +x.vacs };
    },
  };
}

// ---------------------------------------------------------------------------
async function runBloat(eng: Engine, a: Args) {
  await usingClient((c) => eng.reset(c));
  const series: Sample[] = [];
  const start = now();
  let producing = true;
  let produced = 0;
  let consumed = 0;
  console.log(`\n  === ${eng.name} ===`);
  console.log(`  build ${a.buildSec}s: produce ~${k(a.produceRate)}/s, consume ~${k(a.consumeRate)}/s  (backlog grows)`);
  console.log(`  ${"phase".padEnd(6)} ${"t(s)".padStart(6)} ${"size".padStart(11)} ${"live".padStart(10)} ${"dead".padStart(10)} ${"vac".padStart(5)}`);

  // PRODUCER: throttled to --produce-rate events/sec (its own connection)
  const producer = usingClient(async (c) => {
    while (producing) {
      const t = now();
      let allow = Math.ceil(a.produceRate * 0.1); // per 100ms window
      while (allow > 0) {
        const n = Math.min(a.batch, allow);
        await eng.produce(c, n);
        produced += n;
        allow -= n;
      }
      if (eng.postProduce) await eng.postProduce(c);
      const dt = now() - t;
      if (dt < 100) await sleep(100 - dt);
    }
  });

  // CONSUMER: throttled to --consume-rate events/sec (its own connection)
  const consumer = usingClient(async (c) => {
    while (producing) {
      const t = now();
      let allow = Math.ceil(a.consumeRate * 0.1); // per 100ms window
      while (allow > 0) {
        const got = await eng.consume(c, Math.min(a.batch, allow));
        if (got === 0) break;
        consumed += got;
        allow -= got;
      }
      const dt = now() - t;
      if (dt < 100) await sleep(100 - dt);
    }
  });

  // SAMPLER (streams each line live)
  const line = (phase: string, s: Sample) =>
    console.log(`  ${phase.padEnd(6)} ${s.t.toFixed(1).padStart(6)} ${MiB(s.bytes).padStart(11)} ${k(s.live).padStart(10)} ${k(s.dead).padStart(10)} ${String(s.vacs).padStart(5)}`);
  const sampler = usingClient(async (c) => {
    while (producing) {
      const s = await eng.sample(c);
      s.t = (now() - start) / 1000;
      series.push(s);
      line("build", s);
      await sleep(a.sampleMs);
    }
  });

  await sleep(a.buildSec * 1000);
  producing = false;
  await Promise.all([producer, consumer, sampler]);

  // DRAIN: stop producing, consume flat out + reclaim, then final sample
  const drainStart = now();
  await usingClient(async (c) => {
    if (eng.postProduce) await eng.postProduce(c);
    while ((now() - drainStart) / 1000 < a.drainTimeout) {
      const got = await eng.consume(c, a.batch);
      consumed += got;
      if (got === 0) {
        if (eng.maint) { await eng.maint(c); await sleep(1100); } // let rotation_period elapse
        const more = await eng.consume(c, a.batch);
        consumed += more;
        if (more === 0) break;
      }
    }
    if (eng.maint) for (let i = 0; i < 4; i++) { await eng.maint(c); await sleep(1100); }
  });
  const afterDrain = await usingClient((c) => eng.sample(c));
  const drainS = (now() - drainStart) / 1000;

  afterDrain.t = (now() - start) / 1000;
  line("DRAIN", afterDrain);
  const peak = series.reduce((m, s) => (s.bytes > m.bytes ? s : m), series[0] || afterDrain);
  const peakDead = series.reduce((m, s) => Math.max(m, s.dead), 0);
  console.log(`  produced ${k(produced)}, consumed ${k(consumed)}; peak size ${MiB(peak.bytes)}, peak dead ${k(peakDead)}`);
  console.log(`  >> AFTER DRAIN (${drainS.toFixed(1)}s): size ${MiB(afterDrain.bytes)} · dead ${k(afterDrain.dead)} · vacuums ${afterDrain.vacs}`);
  return { peak: peak.bytes, peakDead, afterDrain };
}

async function runThroughput(eng: Engine, a: Args) {
  await usingClient((c) => eng.reset(c));
  // PRODUCE flat out for --dur seconds
  let produced = 0;
  const pStart = now();
  await usingClient(async (c) => {
    while ((now() - pStart) / 1000 < a.dur) {
      await eng.produce(c, a.batch);
      produced += a.batch;
      if (eng.postProduce) await eng.postProduce(c);
    }
  });
  const produceS = (now() - pStart) / 1000;
  // CONSUME flat out until drained
  let consumed = 0;
  const cStart = now();
  await usingClient(async (c) => {
    if (eng.postProduce) await eng.postProduce(c);
    let empty = 0;
    while (empty < 3 && (now() - cStart) / 1000 < a.drainTimeout) {
      const got = await eng.consume(c, a.batch);
      consumed += got;
      empty = got === 0 ? empty + 1 : 0;
    }
  });
  const consumeS = (now() - cStart) / 1000;
  console.log(`\n  === ${eng.name} ===`);
  console.log(`  produce: ${k(produced)} in ${produceS.toFixed(1)}s = ${k(produced / produceS)} ev/s`);
  console.log(`  consume: ${k(consumed)} in ${consumeS.toFixed(1)}s = ${k(consumed / consumeS)} ev/s`);
}

// ---------------------------------------------------------------------------
async function usingClient<T>(fn: (c: pg.Client) => Promise<T>): Promise<T> {
  const c = newClient();
  await c.connect();
  try { return await fn(c); } finally { await c.end(); }
}

interface Args {
  mode: string; engine: string; batch: number; buildSec: number;
  produceRate: number; consumeRate: number; sampleMs: number; drainTimeout: number; dur: number;
}
function parseArgs(): Args {
  const a: any = {
    mode: "bloat", engine: "both", batch: 2000, buildSec: 20,
    produceRate: 20000, consumeRate: 4000, sampleMs: 3000, drainTimeout: 120, dur: 15,
  };
  const map: Record<string, keyof Args> = {
    "--mode": "mode", "--engine": "engine", "--batch": "batch", "--build-sec": "buildSec",
    "--produce-rate": "produceRate", "--consume-rate": "consumeRate", "--sample-ms": "sampleMs",
    "--drain-timeout": "drainTimeout", "--dur": "dur",
  };
  const argv = process.argv.slice(2);
  for (let i = 0; i < argv.length; i += 2) {
    const key = map[argv[i]];
    if (!key) throw new Error(`unknown arg ${argv[i]}`);
    (a as any)[key] = ["mode", "engine"].includes(key) ? argv[i + 1] : Number(argv[i + 1]);
  }
  return a as Args;
}

const args = parseArgs();
const engines = args.engine === "pgque" ? [pgqueEngine()]
  : args.engine === "jobq" ? [jobqEngine()]
  : [pgqueEngine(), jobqEngine()];
console.log(`# ${args.mode} — PgQue vs pg-boss-style mutable queue`);
for (const eng of engines) {
  if (args.mode === "bloat") await runBloat(eng, args);
  else await runThroughput(eng, args);
}
