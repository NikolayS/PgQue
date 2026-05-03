// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import type { Client } from './client.js';
import type { ConsumerOptions, HandlerFunc, Message } from './types.js';

const DEFAULT_MAX_MESSAGES = 2_147_483_647; // PostgreSQL int4 max; request the whole batch by default.

// Reconnect backoff steps (ms) for the LISTEN connection on disconnect.
// Capped at the consumer's pollInterval.
const LISTEN_BACKOFF_MS = [1_000, 2_000, 5_000];

/**
 * High-level consumer that polls `pgque.receive`, dispatches each message
 * to a per-event-type handler, and finalizes the batch with `ack` (or
 * per-message `nack` on handler failure / unknown event type).
 *
 * **LISTEN/NOTIFY wakeup:** the consumer opens a dedicated `pg.Client`
 * connection and issues `LISTEN pgque_<queue>`. When the PgQue ticker
 * emits a `pg_notify`, the consumer wakes immediately instead of waiting
 * for the next poll interval. Polling is retained as a safety net for
 * missed notifications and network drops.
 *
 * Usage:
 * ```ts
 * const consumer = client.newConsumer('orders', 'order_worker');
 * consumer.handle('order.created', async (msg) => { ... });
 *
 * const ac = new AbortController();
 * await consumer.start(ac.signal);
 * ```
 */
export class Consumer {
  private readonly handlers = new Map<string, HandlerFunc>();
  private readonly pollIntervalMs: number;
  private readonly maxMessages: number;
  private readonly unknownHandlerPolicy: 'ack' | 'nack';
  private readonly logger: Pick<Console, 'warn' | 'error'>;
  private readonly listenClientFactory: ConsumerOptions['_listenClientFactory'];

  /** @internal — use {@link Client.newConsumer}. */
  constructor(
    private readonly client: Client,
    private readonly queue: string,
    private readonly name: string,
    opts: ConsumerOptions = {},
  ) {
    this.pollIntervalMs = opts.pollInterval ?? 30_000;
    this.maxMessages = opts.maxMessages ?? DEFAULT_MAX_MESSAGES;
    this.unknownHandlerPolicy = opts.unknownHandlerPolicy ?? 'nack';
    this.logger = opts.logger ?? console;
    this.listenClientFactory = opts._listenClientFactory;
  }

  /** Register a handler for `eventType`. Replaces any previous handler. */
  handle(eventType: string, fn: HandlerFunc): void {
    this.handlers.set(eventType, fn);
  }

  /**
   * Start the poll loop. Resolves when `signal` is aborted; rejects only
   * on terminal errors that should bubble up (the routine `Receive`/`Ack`
   * errors are logged and the loop continues).
   *
   * **Abort granularity:** aborting the signal interrupts the inter-poll
   * `sleep()` immediately, but does **not** cancel an in-flight
   * `client.receive()` call. If a `receive()` round-trip is in progress
   * when the signal fires, the loop will drain that call to completion
   * before exiting.
   *
   * **LISTEN/NOTIFY:** a dedicated `pg.Client` opens `LISTEN pgque_<queue>`
   * so the loop wakes as soon as the ticker fires rather than waiting for the
   * full `pollInterval`. Polling remains active as a fallback safety net.
   */
  async start(signal?: AbortSignal): Promise<void> {
    // notifyResolve is the resolve function for the current "wait for notify"
    // promise. Calling it wakes the poll loop early.
    let notifyResolve: (() => void) | null = null;

    const makeNotifyPromise = (): Promise<void> =>
      new Promise<void>((resolve) => {
        notifyResolve = resolve;
      });

    let currentNotifyPromise = makeNotifyPromise();

    const onNotification = (): void => {
      const resolve = notifyResolve;
      if (resolve) {
        notifyResolve = null;
        resolve();
        // Pre-arm next cycle so the handler is always ready.
        currentNotifyPromise = makeNotifyPromise();
      }
    };

    // Spin up LISTEN connection in the background; reconnect on drops.
    const channel = `pgque_${this.queue}`;

    const connectListen = async (): Promise<void> => {
      if (!this.listenClientFactory) {
        // No factory injected — skip LISTEN; poll-only fallback.
        return;
      }
      let backoffIdx = 0;
      while (!signal?.aborted) {
        try {
          const client = await this.listenClientFactory();

          client.on('notification', onNotification);
          client.on('error', (_err: Error) => {
            // Suppress the unhandled rejection that Node.js emits for
            // EventEmitter 'error' events; the disconnect is handled below.
          });

          await client.connect();
          await client.query(`LISTEN ${quoteIdentifier(channel)}`);
          backoffIdx = 0; // reset on successful connect

          // Block until aborted or the connection emits 'end'.
          if (!signal?.aborted) {
            await new Promise<void>((resolve) => {
              const onEnd = (): void => resolve();
              const onAbort = (): void => {
                client.removeListener('end', onEnd);
                resolve();
              };
              client.once('end', onEnd);
              signal?.addEventListener('abort', onAbort, { once: true });
            });
          }

          // Clean up: UNLISTEN + end.
          client.removeListener('notification', onNotification);
          try {
            await client.query(`UNLISTEN ${quoteIdentifier(channel)}`);
          } catch {
            // ignore: connection may already be broken
          }
          try {
            await client.end();
          } catch {
            // ignore
          }

          if (signal?.aborted) return;

          // Unexpected disconnect — fall through to reconnect logic below.
          this.logger.warn(`pgque: LISTEN connection dropped for ${channel}, reconnecting`);
        } catch (err) {
          this.logger.error(
            `pgque: LISTEN connect error for ${channel}: ${formatErr(err)}, reconnecting`,
          );
        }

        if (signal?.aborted) return;

        // Exponential backoff capped at pollInterval.
        const delay = Math.min(
          LISTEN_BACKOFF_MS[backoffIdx] ?? LISTEN_BACKOFF_MS[LISTEN_BACKOFF_MS.length - 1]!,
          this.pollIntervalMs,
        );
        backoffIdx = Math.min(backoffIdx + 1, LISTEN_BACKOFF_MS.length - 1);
        await sleep(delay, signal);
      }
    };

    // Launch the LISTEN loop; don't await — it runs alongside the poll loop.
    const listenDone = connectListen();

    // Poll loop.
    while (!signal?.aborted) {
      let msgs: Message[];
      try {
        msgs = await this.client.receive(this.queue, this.name, this.maxMessages);
      } catch (err) {
        this.logger.error(`pgque: receive error: ${formatErr(err)}`);
        await Promise.race([sleep(this.pollIntervalMs, signal), currentNotifyPromise]);
        currentNotifyPromise = makeNotifyPromise();
        continue;
      }

      if (msgs.length === 0) {
        await Promise.race([sleep(this.pollIntervalMs, signal), currentNotifyPromise]);
        currentNotifyPromise = makeNotifyPromise();
        continue;
      }

      let batchId: bigint | null = null;
      let anyNackFailed = false;
      for (const msg of msgs) {
        batchId = msg.batchId;
        const handler = this.handlers.get(msg.type);
        if (!handler) {
          if (this.unknownHandlerPolicy === 'ack') {
            this.logger.warn(
              `pgque: no handler registered for event type "${msg.type}", acking msg ${msg.msgId} (unknownHandlerPolicy='ack')`,
            );
            // Fall through; the batch ack at the end of the loop covers it.
            continue;
          }
          this.logger.warn(
            `pgque: no handler registered for event type "${msg.type}", nacking msg ${msg.msgId}`,
          );
          if (!(await this.tryNack(batchId, msg, 'unknown event type'))) {
            anyNackFailed = true;
          }
          continue;
        }
        try {
          await handler(msg);
        } catch (err) {
          this.logger.error(`pgque: handler error for "${msg.type}": ${formatErr(err)}`);
          if (!(await this.tryNack(batchId, msg, 'handler error'))) {
            anyNackFailed = true;
          }
        }
      }

      if (batchId !== null) {
        if (anyNackFailed) {
          // At least one required nack failed. Skip ack so PgQ redelivers
          // the batch on the next poll instead of advancing the consumer
          // past messages we couldn't route.
          this.logger.error(
            `pgque: skipping ack for batch ${batchId}; one or more nacks failed and the batch will be redelivered`,
          );
        } else {
          try {
            await this.client.ack(batchId);
          } catch (err) {
            this.logger.error(`pgque: ack error: ${formatErr(err)}`);
          }
        }
      }
    }

    // Signal aborted: wake the notify promise so the listen loop exits cleanly.
    onNotification();
    // Ensure the LISTEN loop also exits (it checks signal?.aborted).
    await listenDone;
  }

  /** Returns true if the nack succeeded, false if it threw (and was logged). */
  private async tryNack(batchId: bigint, msg: Message, reason: string): Promise<boolean> {
    try {
      await this.client.nack(batchId, msg, { reason });
      return true;
    } catch (err) {
      this.logger.error(`pgque: nack error for "${msg.type}": ${formatErr(err)}`);
      return false;
    }
  }
}

function formatErr(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) {
      resolve();
      return;
    }
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort);
      resolve();
    }, ms);
    const onAbort = (): void => {
      clearTimeout(timer);
      resolve();
    };
    signal?.addEventListener('abort', onAbort, { once: true });
  });
}

/**
 * Quote a PostgreSQL identifier (channel name) for safe use in LISTEN/UNLISTEN.
 * Doubles any double-quote characters and wraps in double quotes.
 */
function quoteIdentifier(name: string): string {
  return `"${name.replace(/"/g, '""')}"`;
}
