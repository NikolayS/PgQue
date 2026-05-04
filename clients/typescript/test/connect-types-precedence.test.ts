// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
//
// Regression test for REV finding on PR #189:
// `poolOptions` spread was after `types: pgqueTypes`, so a caller passing
// `{ types: ... }` silently overwrote pgque's bigint parser.
//
// These tests are pure Vitest — no live PostgreSQL required. pg.Pool is
// mocked via vi.mock (hoisted before imports) so we can capture the Pool
// constructor argument without opening connections.

import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import pg from 'pg';

// ---------------------------------------------------------------------------
// Mock pg.Pool so that:
//   1. The constructor records `capturedPoolConfig` for inspection.
//   2. pool.connect() always rejects (we don't need a real connection;
//      connect() from client.ts catches this and throws PgqueConnectionError,
//      which is fine — we only care about what was passed to the constructor).
//   3. pool.end() resolves cleanly.
//
// vi.mock is hoisted to the top of the file by Vitest's transformer, so this
// runs BEFORE `import { pgqueTypes, connect } from '../src/index.js'` below.
// ---------------------------------------------------------------------------

let capturedPoolConfig: pg.PoolConfig | undefined;

vi.mock('pg', async (importOriginal) => {
  const original = await importOriginal<typeof import('pg')>();

  class MockPool {
    constructor(config: pg.PoolConfig) {
      capturedPoolConfig = config;
    }
    connect() {
      return Promise.reject(new Error('mock: no real PG'));
    }
    end() {
      return Promise.resolve();
    }
  }

  return {
    ...original,
    default: {
      ...original.default,
      Pool: MockPool,
    },
  };
});

// These imports happen AFTER vi.mock is applied (hoisting ensures that).
import { pgqueTypes, connect } from '../src/index.js';

// ---------------------------------------------------------------------------
// TypeScript-level check: connect() must reject { types: ... } at compile time.
//
// The parameter type must be Omit<pg.PoolConfig, 'connectionString' | 'types'>,
// so passing `{ types: ... }` is a TS error.
// We assert with @ts-expect-error so CI catches any type regression.
// ---------------------------------------------------------------------------
function _tsCompileTimeCheck() {
  // @ts-expect-error — 'types' must be excluded from poolOptions by Omit
  void connect('postgres://fake@localhost/fake', { types: {} as pg.CustomTypesConfig });
}

// ---------------------------------------------------------------------------
// Runtime checks
// ---------------------------------------------------------------------------

describe('connect() — pgqueTypes wins over user pool options (REV #189)', () => {
  beforeEach(() => {
    capturedPoolConfig = undefined;
  });

  it('pgqueTypes is exported from client.ts (@internal)', () => {
    // Ensures the production object is available for identity checks.
    expect(pgqueTypes).toBeDefined();
    expect(typeof pgqueTypes.getTypeParser).toBe('function');
    // Spot-check: OID 20 must parse to bigint.
    const parser = pgqueTypes.getTypeParser(20, 'text');
    expect(typeof parser('42')).toBe('bigint');
    expect(parser('42')).toBe(42n);
  });

  it('Pool is constructed with types === pgqueTypes (identity check)', async () => {
    // connect() rejects because MockPool.connect() throws, but we can inspect
    // what was passed to the Pool constructor before the error propagates.
    await expect(connect('postgres://fake@localhost/fake')).rejects.toThrow();
    expect(capturedPoolConfig).toBeDefined();
    // The `types` field on the Pool config must be the exact same object
    // as the exported pgqueTypes — not user-supplied and not undefined.
    expect(capturedPoolConfig!.types).toBe(pgqueTypes);
  });

  it('pgqueTypes wins when poolOptions carries a types key (runtime safety belt)', async () => {
    const fakeTypes: pg.CustomTypesConfig = {
      getTypeParser: (_oid: number, _format?: string) => (v: string) => v,
    };
    // Cast bypasses TS — simulates a JS caller that ignores the type constraint.
    await expect(
      connect(
        'postgres://fake@localhost/fake',
        { types: fakeTypes } as Omit<pg.PoolConfig, 'connectionString' | 'types'>,
      ),
    ).rejects.toThrow();
    expect(capturedPoolConfig).toBeDefined();
    // pgqueTypes must win — NOT fakeTypes.
    expect(capturedPoolConfig!.types).toBe(pgqueTypes);
    expect(capturedPoolConfig!.types).not.toBe(fakeTypes);
  });
});
