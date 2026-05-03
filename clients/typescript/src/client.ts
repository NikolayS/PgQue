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

const { Pool, Client: Client_, types } = pg;

// PostgreSQL `bigint` (OID 20) is parsed by `pg` as string by default to
// avoid silent precision loss above Number.MAX_SAFE_INTEGER. We promote to
// JS `bigint` for safety AND ergonomics — `bigint` is the natural type for
// PG `bigint`, matches the Go driver's `int64`, and round-trips losslessly.
//
// We do NOT touch the process-global parser table. Instead, each Pool created
// by pgque is given a per-pool CustomTypesConfig that overrides OID 20 only
// for queries issued by pgque. Unrelated pg pools in the same process are
// unaffected.
/**
 * Per-pool type overrides used by pgque's `connect()`.
 * Parses PostgreSQL `bigint` (OID 20) as JS `bigint` without touching the
 * process-global `pg.types` table.
 *
 * @internal — exported for testing only; do not depend on this in application code.
 */
export const pgqueTypes: pg.CustomTypesConfig = {
  getTypeParser(oid: number, format?: string) {
    if (oid === 20) {
      // int8 / bigint → JS bigint
      return (val: string) => BigInt(val);
    }
    return types.getTypeParser(oid, format as 'text' | 'binary');
  },
};

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
  constructor(
    private readonly pool: pg.Pool,
    private readonly dsn?: string,
  ) {}

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
   * - Top-level `undefined` (or an omitted `payload` field) is coerced to
   *   the JSON literal `null` so the JSONB column receives a valid value.
   * - `null` round-trips as JSON `null`.
   * - Object properties whose values are `undefined` are dropped by
   *   `JSON.stringify` per the JSON spec.
   * - Functions, symbols, and `BigInt` literals are not JSON-serializable
   *   as top-level payloads and are rejected.
   * - Circular references throw a `TypeError` from `JSON.stringify`.
   *
   * Pass plain JSON-compatible values (objects, arrays, strings, numbers,
   * booleans, `null`) to avoid surprises.
   */
  async send(queue: string, event: Event): Promise<bigint> {
    if (!queue) {
      throw new PgqueSqlError('send', { cause: new Error('queue must be a non-empty string') });
    }
    const type = event.type && event.type.length > 0 ? event.type : 'default';
    const payload = serializePayload(event.payload);
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

  /** Publish same-type payloads atomically; empty type defaults to `default`. */
  async sendBatch(queue: string, type: string, payloads: unknown[]): Promise<bigint[]> {
    if (!queue) {
      throw new PgqueSqlError('sendBatch', {
        cause: new Error('queue must be a non-empty string'),
      });
    }
    const eventType = type && type.length > 0 ? type : 'default';
    const encoded = payloads.map((payload, index) => {
      try {
        return JSON.stringify(payload) ?? 'null';
      } catch (err) {
        throw new PgqueSqlError('sendBatch', {
          cause: new Error(`payload at index ${index} is not JSON-serializable`, { cause: err }),
        });
      }
    });

    try {
      const result = await this.pool.query<{ send_batch: Array<bigint | string> }>(
        'select pgque.send_batch($1, $2, $3::jsonb[]) as send_batch',
        [queue, eventType, encoded],
      );
      const row = result.rows[0];
      if (!row) {
        throw new PgqueSqlError('sendBatch', { cause: new Error('no row returned') });
      }
      return row.send_batch.map((id) => (typeof id === 'bigint' ? id : BigInt(id)));
    } catch (err) {
      if (err instanceof PgqueError) throw err;
      throw mapPgError('sendBatch', err, { queue });
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
    // Inject a factory for the dedicated LISTEN pg.Client when we have a DSN
    // and the caller hasn't already provided a test-stub factory.
    const dsn = this.dsn;
    const optsWithFactory: ConsumerOptions =
      dsn !== undefined && opts._listenClientFactory === undefined
        ? {
            ...opts,
            _listenClientFactory: async () => {
              const c = new Client_({ connectionString: dsn });
              return c;
            },
          }
        : opts;
    return new Consumer(this, queue, name, optsWithFactory);
  }

  /**
   * Run the per-queue ticker for `queue`. Wraps `pgque.ticker(queue text)`
   * (the one-argument SQL overload).
   *
   * Returns the new tick id (`bigint`) when a tick was created, or `null`
   * when no tick was needed (e.g. the queue is idle or the max-lag threshold
   * has not been reached yet). Mirrors the SQL function's `returns bigint`
   * contract where the function returns `NULL` on no-op.
   *
   * Throws if the queue does not exist or has an external ticker configured.
   *
   * For the global (all-queues) ticker use {@link tickerAll}.
   */
  async ticker(queue: string): Promise<bigint | null> {
    try {
      const result = await this.pool.query<{ ticker: bigint | null }>(
        'select pgque.ticker($1) as ticker',
        [queue],
      );
      const row = result.rows[0];
      if (!row) {
        throw new PgqueSqlError('ticker', { cause: new Error('no row returned') });
      }
      return row.ticker !== null ? BigInt(row.ticker) : null;
    } catch (err) {
      if (err instanceof PgqueError) throw err;
      throw mapPgError('ticker', err, { queue });
    }
  }

  /**
   * Run the global ticker across all eligible queues. Wraps the zero-argument
   * `pgque.ticker()` SQL overload.
   *
   * Returns the number of queues that had a tick inserted during this call.
   * The SQL function returns `bigint`; this method narrows to JS `number`
   * because the queue count is always well within `Number.MAX_SAFE_INTEGER`.
   */
  async tickerAll(): Promise<number> {
    try {
      const result = await this.pool.query<{ ticker: bigint }>(
        'select pgque.ticker() as ticker',
      );
      const row = result.rows[0];
      if (!row) {
        throw new PgqueSqlError('tickerAll', { cause: new Error('no row returned') });
      }
      return Number(row.ticker);
    } catch (err) {
      if (err instanceof PgqueError) throw err;
      throw mapPgError('tickerAll', err);
    }
  }

  /**
   * Bump the event-seq threshold for `queue` so the next `ticker(queue)` call
   * produces a tick. Wraps `pgque.force_tick(queue text)`.
   *
   * Returns the current last tick id (`bigint`) for the queue, or `null` if
   * the queue has no ticks yet (brand-new queue) or if the queue is paused /
   * has an external ticker (the SQL function silently skips those cases).
   */
  async forceTick(queue: string): Promise<bigint | null> {
    try {
      const result = await this.pool.query<{ force_tick: bigint | null }>(
        'select pgque.force_tick($1) as force_tick',
        [queue],
      );
      const row = result.rows[0];
      if (!row) {
        throw new PgqueSqlError('force_tick', { cause: new Error('no row returned') });
      }
      return row.force_tick !== null ? BigInt(row.force_tick) : null;
    } catch (err) {
      if (err instanceof PgqueError) throw err;
      throw mapPgError('force_tick', err, { queue });
    }
  }
}

/**
 * Connect to PostgreSQL and return a ready-to-use {@link Client}. Verifies
 * the connection eagerly; rejects with {@link PgqueConnectionError} on
 * failure.
 *
 * The `types` parser config is reserved for pgque's internal bigint parsing;
 * user-supplied `types` is ignored.
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
  poolOptions: Omit<pg.PoolConfig, 'connectionString' | 'types'> = {},
): Promise<Client> {
  // Defensively strip `types` from poolOptions before spreading. The
  // `Omit<..., 'types'>` already rejects this at compile time, but JS
  // callers (or `as` casts) can still smuggle one in. Dropping it here
  // makes the pgque types config impossible to override regardless of
  // spread order — see REV review on PR #189.
  const { types: _userTypes, ...restPoolOptions } = poolOptions as pg.PoolConfig;
  void _userTypes;
  const pool = new Pool({ connectionString: dsn, ...restPoolOptions, types: pgqueTypes });
  let probe: pg.PoolClient;
  try {
    probe = await pool.connect();
  } catch (err) {
    await pool.end().catch(() => undefined);
    throw new PgqueConnectionError(`pgque: connect: ${(err as Error).message}`, { cause: err });
  }
  probe.release();
  return new Client(pool, dsn);
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

function serializePayload(payload: unknown): string {
  // JSON.stringify(undefined) returns the literal `undefined` (not the
  // string "null"), which would coerce to a SQL NULL bind param. Coerce
  // top-level undefined to JSON null instead so it round-trips as a
  // valid JSONB value. Omitted payload fields also arrive here as
  // `undefined` because Event.payload is optional.
  if (payload === undefined) return 'null';

  let encoded: string | undefined;
  try {
    encoded = JSON.stringify(payload);
  } catch (err) {
    throw new PgqueSqlError('send', {
      cause: err instanceof Error ? err : new Error(String(err)),
    });
  }
  if (encoded === undefined) {
    throw new PgqueSqlError('send', {
      cause: new Error('payload must be JSON-serializable'),
    });
  }
  return encoded;
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
