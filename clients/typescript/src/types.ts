// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/**
 * A message received from a PgQue queue. Mirrors the `pgque.message`
 * composite type in SQL.
 *
 * `msgId` and `batchId` are `bigint` (JavaScript native) because the
 * underlying PostgreSQL `bigint` columns can exceed `Number.MAX_SAFE_INTEGER`.
 */
export interface Message {
  msgId: bigint;
  batchId: bigint;
  type: string;
  /** Raw `ev_data` text. Caller may `JSON.parse()` if the producer used `Event.payload`. */
  payload: string;
  /** Number of prior retry attempts. `null` on the first delivery. */
  retryCount: number | null;
  createdAt: Date;
  extra1: string | null;
  extra2: string | null;
  extra3: string | null;
  extra4: string | null;
}

/**
 * Event input to {@link Client.send}. `payload` is JSON-marshalled before
 * being passed to `pgque.send`; omitted payloads are stored as JSON `null`.
 * An empty `type` defaults to `"default"`.
 */
export interface Event {
  type?: string;
  payload?: unknown;
}

/** Options for {@link Client.nack}. */
export interface NackOptions {
  /** Retry delay if the message has not exceeded `queue_max_retries`. Default `60s`. */
  retryAfter?: string;
  /** Free-form reason recorded on the dead-letter row when the retry limit is hit. */
  reason?: string;
}

/** Options for {@link Client.newConsumer}. */
export interface ConsumerOptions {
  /** Interval between poll cycles when no messages are available. Default `30s`. */
  pollInterval?: number;
  /**
   * Maximum messages returned per `receive()` call. By default the
   * high-level consumer requests the PostgreSQL `int` maximum so it drains
   * the whole PgQ batch before acknowledging it.
   *
   * WARNING: `pgque.ack(batch_id)` finishes the entire underlying batch,
   * including rows the client never returned. If you set `maxMessages`
   * below the real batch size, unreturned rows are skipped after ack.
   * Only lower this value when it is at least as large as the queue's
   * possible batch size for your workload.
   */
  maxMessages?: number;
  /**
   * What to do with messages whose `type` has no registered handler:
   * - `'nack'` (default) — nack each unknown message with a reason; PgQ
   *   routes to the retry queue or DLQ per the queue's `queue_max_retries`.
   * - `'ack'` — log a warning and let the batch ack absorb them (silent
   *   discard). Use only when stray types are expected and benign.
   */
  unknownHandlerPolicy?: 'ack' | 'nack';
  /** Optional logger. Defaults to `console`. */
  logger?: Pick<Console, 'warn' | 'error'>;
  /**
   * @internal — used in tests to inject a stub `pg.Client` for the
   * dedicated LISTEN connection instead of opening a real one.
   */
  _listenClientFactory?: () => Promise<import('pg').Client>;
}

/**
 * Handler for a single message. Throwing or rejecting causes the message to
 * be nacked individually; other messages in the same batch still process.
 */
export type HandlerFunc = (msg: Message) => Promise<void> | void;
