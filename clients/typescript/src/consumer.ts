// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import type { Client } from './client.js';
import type { ConsumerOptions, HandlerFunc, Message } from './types.js';

/**
 * Default `maxMessages` for the high-level Consumer. PostgreSQL `int4` max
 * (`2^31 - 1`); request the whole PgQ batch by default so a subsequent
 * `pgque.ack(batch_id)` does not strand events the client never saw.
 */
export const DEFAULT_MAX_MESSAGES = 2_147_483_647;

/**
 * High-level consumer that polls `pgque.receive`, dispatches each message
 * to a per-event-type handler, and finalizes the batch with `ack` (or
 * per-message `nack` on handler failure / unknown event type).
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
   */
  async start(signal?: AbortSignal): Promise<void> {
    while (!signal?.aborted) {
      let msgs: Message[];
      try {
        msgs = await this.client.receive(this.queue, this.name, this.maxMessages);
      } catch (err) {
        this.logger.error(`pgque: receive error: ${formatErr(err)}`);
        await sleep(this.pollIntervalMs, signal);
        continue;
      }

      if (msgs.length === 0) {
        await sleep(this.pollIntervalMs, signal);
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
          if (!(await this.tryNack(batchId, msg, `unknown event type: ${msg.type}`))) {
            anyNackFailed = true;
          }
          continue;
        }
        try {
          await handler(msg);
        } catch (err) {
          this.logger.error(`pgque: handler error for "${msg.type}": ${formatErr(err)}`);
          if (!(await this.tryNack(batchId, msg, `handler error: ${formatErr(err)}`))) {
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
