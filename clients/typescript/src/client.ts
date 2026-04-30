// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import pg from 'pg';
import { Consumer } from './consumer.js';
import {
  PgqueConnectionError,
  PgqueError,
  PgqueQueueNotFoundError,
  PgqueSqlError,
} from './errors.js';
import type { ConsumerOptions, Event, Message, NackOptions } from './types.js';

const { Pool, types } = pg;

// PostgreSQL `bigint` (oid 20) is parsed by `pg` as string by default to
// avoid silent precision loss above Number.MAX_SAFE_INTEGER. We promote to
// JS `bigint` for safety AND ergonomics — `bigint` is the natural type for
// PG `bigint`, matches the Go driver's `int64`, and round-trips losslessly.
//
// This mutates a process-global parser table. We do it once at module load
// and document the side effect in the README.
types.setTypeParser(20, (val) => BigInt(val));

const PG_RAISE_EXCEPTION_CODE = 'P0001';

interface PgError extends Error {
  code?: string;
}

function isPgError(e: unknown): e is PgError {
  return e instanceof Error && typeof (e as PgError).code === 'string';
}

/** Internal: row shape returned by `pgque.receive` after type parsers run. */
interface RawMessageRow {
  msg_id: bigint;
  batch_id: bigint;
  type: string;
  payload: string;
  retry_count: number | null;
  created_at: Date;
  extra1: string | null;
  extra2: string | null;
  extra3: string | null;
  extra4: string | null;
}

/**
 * The main PgQue client backed by a `pg.Pool`. Construct via
 * {@link connect}; do not invoke the constructor directly.
 */
export class Client {
  /** @internal — use {@link connect} instead. */
  constructor(private readonly pool: pg.Pool) {}

  /** Release the connection pool. After this, the client must not be used. */
  async close(): Promise<void> {
    await this.pool.end();
  }

  /** Underlying `pg.Pool` for direct SQL access (escape hatch). */
  get rawPool(): pg.Pool {
    return this.pool;
  }

  /**
   * Publish an event to the named queue. Returns the new event ID as
   * `bigint`. Empty {@link Event.type} defaults to `"default"` (matches
   * the SQL `pgque.send` default).
   *
   * **Payload shape requirements:** `event.payload` is serialized with
   * `JSON.stringify`. This means:
   * - Values that are not JSON-serializable (`undefined`, functions,
   *   symbols, `BigInt` literals) will be silently dropped or throw.
   * - Circular references throw a `TypeError` from `JSON.stringify`.
   * - `undefined` at the top level becomes the JSON string `"null"`.
   *
   * Pass plain JSON-compatible values (objects, arrays, strings, numbers,
   * booleans, `null`) to avoid surprises.
   */
  async send(queue: string, event: Event): Promise<bigint> {
    if (!queue) {
      throw new PgqueSqlError('send', { cause: new Error('queue must be a non-empty string') });
    }
    const type = event.type && event.type.length > 0 ? event.type : 'default';
    const payload = JSON.stringify(event.payload);
    try {
      const result = await this.pool.query<{ send: bigint }>(
        'select pgque.send($1, $2, $3::jsonb) as send',
        [queue, type, payload],
      );
      const row = result.rows[0];
      if (!row) {
        throw new PgqueSqlError('send', { cause: new Error('no row returned') });
      }
      return row.send;
    } catch (err) {
      if (err instanceof PgqueError) throw err;
      throw mapPgError('send', err, { queue });
    }
  }

  /**
   * Fetch up to `maxMessages` from the next batch for `consumer` on `queue`.
   * Returns an empty array when no batch is currently available.
   */
  async receive(queue: string, consumer: string, maxMessages = 100): Promise<Message[]> {
    if (!queue) {
      throw new PgqueSqlError('receive', { cause: new Error('queue must be a non-empty string') });
    }
    if (!consumer) {
      throw new PgqueSqlError('receive', {
        cause: new Error('consumer must be a non-empty string'),
      });
    }
    if (!Number.isInteger(maxMessages) || maxMessages <= 0) {
      throw new PgqueSqlError('receive', {
        cause: new Error('maxMessages must be a positive integer'),
      });
    }
    try {
      const result = await this.pool.query<RawMessageRow>(
        'select * from pgque.receive($1, $2, $3)',
        [queue, consumer, maxMessages],
      );
      return result.rows.map(rowToMessage);
    } catch (err) {
      if (err instanceof PgqueError) throw err;
      throw mapPgError('receive', err, { queue });
    }
  }

  /** Acknowledge (finish) a batch, advancing the consumer's position. */
  async ack(batchId: bigint): Promise<void> {
    if (typeof batchId !== 'bigint') {
      throw new PgqueSqlError('ack', { cause: new Error('batchId must be bigint') });
    }
    try {
      await this.pool.query('select pgque.ack($1)', [batchId.toString()]);
    } catch (err) {
      if (err instanceof PgqueError) throw err;
      throw mapPgError('ack', err);
    }
  }

  /**
   * Negatively acknowledge a single message. Routes to the retry queue if
   * `retry_count < queue_max_retries`, otherwise to the dead-letter queue.
   * Other messages in the same batch are not affected.
   */
  async nack(batchId: bigint, msg: Message, opts: NackOptions = {}): Promise<void> {
    if (typeof batchId !== 'bigint') {
      throw new PgqueSqlError('nack', { cause: new Error('batchId must be bigint') });
    }
    const retryAfter = opts.retryAfter ?? '60 seconds';
    const reason = opts.reason ?? null;
    // pgque.message has 10 fields: (msg_id, batch_id, type, payload,
    // retry_count, created_at, extra1, extra2, extra3, extra4). The ROW()
    // literal must supply exactly that many values in that order.
    try {
      await this.pool.query(
        `select pgque.nack(
           $1,
           ROW($2,$3,$4,$5,$6,$7,$8,$9,$10,$11)::pgque.message,
           $12::interval,
           $13
         )`,
        [
          batchId.toString(), // $1 i_batch_id
          msg.msgId.toString(), // $2 msg_id
          msg.batchId.toString(), // $3 batch_id
          msg.type, // $4 type
          msg.payload, // $5 payload
          msg.retryCount, // $6 retry_count
          msg.createdAt, // $7 created_at
          msg.extra1, // $8 extra1
          msg.extra2, // $9 extra2
          msg.extra3, // $10 extra3
          msg.extra4, // $11 extra4
          retryAfter, // $12 i_retry_after
          reason, // $13 i_reason
        ],
      );
    } catch (err) {
      if (err instanceof PgqueError) throw err;
      throw mapPgError('nack', err);
    }
  }

  /**
   * Subscribe `consumer` to `queue` (wraps `pgque.register_consumer`).
   * Re-subscribing is a no-op (returns 0); first subscribe returns 1.
   */
  async subscribe(queue: string, consumer: string): Promise<number> {
    try {
      const result = await this.pool.query<{ subscribe: number }>(
        'select pgque.subscribe($1, $2) as subscribe',
        [queue, consumer],
      );
      return result.rows[0]?.subscribe ?? 0;
    } catch (err) {
      throw mapPgError('subscribe', err, { queue });
    }
  }

  /** Unsubscribe `consumer` from `queue` (wraps `pgque.unregister_consumer`). */
  async unsubscribe(queue: string, consumer: string): Promise<number> {
    try {
      const result = await this.pool.query<{ unsubscribe: number }>(
        'select pgque.unsubscribe($1, $2) as unsubscribe',
        [queue, consumer],
      );
      return result.rows[0]?.unsubscribe ?? 0;
    } catch (err) {
      throw mapPgError('unsubscribe', err, { queue });
    }
  }

  /**
   * Construct a Consumer that polls `queue` under `name`. The consumer
   * must already be subscribed (e.g. via {@link subscribe}).
   */
  newConsumer(queue: string, name: string, opts: ConsumerOptions = {}): Consumer {
    return new Consumer(this, queue, name, opts);
  }

  /**
   * Wrapper for pgque.ticker(): if `queue` is given, runs the per-queue
   * overload (`pgque.ticker(queue text)`); otherwise runs the no-arg global
   * overload. Returns the underlying tick result count.
   */
  async ticker(queue?: string): Promise<void> {
    try {
      if (queue !== undefined) {
        await this.pool.query('select pgque.ticker($1)', [queue]);
      } else {
        await this.pool.query('select pgque.ticker()');
      }
    } catch (err) {
      throw mapPgError('ticker', err, queue !== undefined ? { queue } : undefined);
    }
  }

  /** Exact wrapper for pgque.force_tick(queue). Bumps the event-seq threshold so the next ticker run produces a tick. */
  async forceTick(queue: string): Promise<void> {
    try {
      await this.pool.query('select pgque.force_tick($1)', [queue]);
    } catch (err) {
      throw mapPgError('force_tick', err, { queue });
    }
  }
}

/**
 * Connect to PostgreSQL and return a ready-to-use {@link Client}. Verifies
 * the connection eagerly; rejects with {@link PgqueConnectionError} on
 * failure.
 *
 * @example
 * ```ts
 * const client = await connect('postgres://user:pass@localhost/mydb');
 * try {
 *   await client.send('orders', { type: 'order.created', payload: { id: 42 } });
 * } finally {
 *   await client.close();
 * }
 * ```
 */
export async function connect(
  dsn: string,
  poolOptions: Omit<pg.PoolConfig, 'connectionString'> = {},
): Promise<Client> {
  const pool = new Pool({ connectionString: dsn, ...poolOptions });
  let probe: pg.PoolClient;
  try {
    probe = await pool.connect();
  } catch (err) {
    await pool.end().catch(() => undefined);
    throw new PgqueConnectionError(`pgque: connect: ${(err as Error).message}`, { cause: err });
  }
  probe.release();
  return new Client(pool);
}

function rowToMessage(row: RawMessageRow): Message {
  return {
    msgId: row.msg_id,
    batchId: row.batch_id,
    type: row.type,
    payload: row.payload,
    retryCount: row.retry_count,
    createdAt: row.created_at,
    extra1: row.extra1,
    extra2: row.extra2,
    extra3: row.extra3,
    extra4: row.extra4,
  };
}

function mapPgError(op: string, err: unknown, ctx?: { queue?: string }): PgqueError {
  if (!isPgError(err)) {
    return new PgqueSqlError(op, { cause: err });
  }
  const msg = err.message ?? '';
  if (err.code === PG_RAISE_EXCEPTION_CODE && /queue not found/i.test(msg) && ctx?.queue) {
    return new PgqueQueueNotFoundError(ctx.queue, { cause: err });
  }
  return new PgqueSqlError(op, { cause: err });
}
