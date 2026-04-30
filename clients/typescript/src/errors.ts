// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

/** Base class for all pgque-specific errors. */
export class PgqueError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'PgqueError';
  }
}

/** Failed to connect to PostgreSQL or the connection dropped mid-operation. */
export class PgqueConnectionError extends PgqueError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = 'PgqueConnectionError';
  }
}

/** The named queue does not exist (caller forgot `pgque.create_queue`). */
export class PgqueQueueNotFoundError extends PgqueError {
  constructor(public readonly queue: string, options?: { cause?: unknown }) {
    super(`pgque: queue not found: ${queue}`, options);
    this.name = 'PgqueQueueNotFoundError';
  }
}

/** The named consumer is not subscribed to the queue. */
export class PgqueConsumerNotFoundError extends PgqueError {
  constructor(
    public readonly queue: string,
    public readonly consumer: string,
    options?: { cause?: unknown },
  ) {
    super(`pgque: consumer ${consumer} not subscribed to ${queue}`, options);
    this.name = 'PgqueConsumerNotFoundError';
  }
}

/** Generic SQL-level failure surfaced from `pg` (constraint, syntax, etc.). */
export class PgqueSqlError extends PgqueError {
  constructor(
    public readonly op: string,
    options?: { cause?: unknown },
  ) {
    const causeMsg =
      options?.cause instanceof Error ? options.cause.message : String(options?.cause ?? '');
    super(`pgque: ${op}: ${causeMsg}`, options);
    this.name = 'PgqueSqlError';
  }
}
