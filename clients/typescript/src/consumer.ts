// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

import type { Client } from './client.js';
import type { ConsumerOptions, HandlerFunc, Message } from './types.js';

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
  private readonly logger: Pick<Console, 'warn' | 'error'>;

  /** @internal — use {@link Client.newConsumer}. */
  constructor(
    private readonly client: Client,
    private readonly queue: string,
    private readonly name: string,
    opts: ConsumerOptions = {},
  ) {
    this.pollIntervalMs = opts.pollInterval ?? 30_000;
    this.maxMessages = opts.maxMessages ?? 100;
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
      for (const msg of msgs) {
        batchId = msg.batchId;
        const handler = this.handlers.get(msg.type);
        if (!handler) {
          this.logger.warn(
            `pgque: no handler registered for event type "${msg.type}", nacking msg ${msg.msgId}`,
          );
          await this.tryNack(batchId, msg);
          continue;
        }
        try {
          await handler(msg);
        } catch (err) {
          this.logger.error(`pgque: handler error for "${msg.type}": ${formatErr(err)}`);
          await this.tryNack(batchId, msg);
        }
      }

      if (batchId !== null) {
        try {
          await this.client.ack(batchId);
        } catch (err) {
          this.logger.error(`pgque: ack error: ${formatErr(err)}`);
        }
      }
    }
  }

  private async tryNack(batchId: bigint, msg: Message): Promise<void> {
    try {
      await this.client.nack(batchId, msg);
    } catch (err) {
      this.logger.error(`pgque: nack error for "${msg.type}": ${formatErr(err)}`);
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
