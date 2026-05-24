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
  /**
   * Retry delay in **seconds** if the message has not exceeded
   * `queue_max_retries`. Default `60`.
   *
   * The driver binds this as a PostgreSQL `interval` (`'<n> seconds'`) for
   * `pgque.nack`'s `i_retry_after` parameter. Cross-driver parity with
   * `pgque-py` and `pgque-go`.
   */
  retryAfter?: number;
  /** Free-form reason recorded on the dead-letter row when the retry limit is hit. */
  reason?: string;
}

/** Options for {@link Client.newConsumer}. */
export interface ConsumerOptions {
  /**
   * Interval between poll cycles when no messages are available, in
   * **milliseconds**. Default `30000` (30 seconds).
   */
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
   * Retry delay in **seconds** used by the high-level Consumer when it
   * nacks messages after handler failure or unknown event type. Default `60`.
   */
  retryAfter?: number;
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
   * **Experimental.** When set, the consumer joins a cooperative group on
   * `(queue, name)` as this `subconsumer` worker. The poll loop calls
   * `client.receiveCoop(queue, name, subconsumer, ...)` instead of
   * `client.receive(...)`. PgQue auto-registers the cooperative main +
   * subconsumer rows on first call.
   *
   * Function names, edge-case behavior, and the option shape may change
   * before this feature is marked stable.
   */
  subconsumer?: string;
  /**
   * **Experimental.** PostgreSQL `interval` text passed to `receive_coop` to
   * enable stale-batch takeover (e.g. `"5 minutes"`). Only valid together
   * with `subconsumer`; supplying it without `subconsumer` throws at
   * construction.
   */
  deadInterval?: string;
}

/**
 * Handler for a single message. Throwing or rejecting causes the message to
 * be nacked individually; other messages in the same batch still process.
 */
export type HandlerFunc = (msg: Message) => Promise<void> | void;
