// pgque -- TypeScript client for PgQue
// Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.
//
// Tests for issue #145: importing pgque must not mutate the process-global
// pg-types parser table (OID 20 / int8).
//
// These tests are PURE VITEST — no live PostgreSQL required. They mock
// pg.Pool.prototype.query to observe the type-parser behaviour without
// needing a real database connection.

import { beforeAll, describe, expect, it, vi } from 'vitest';
import pg from 'pg';

// ---------------------------------------------------------------------------
// Test 1 (red on unfixed code, green on fix):
// BEFORE importing pgque, record what the global pg-types parser returns for
// OID 20. After importing pgque, a SECOND unrelated pg.Pool must still use
// the original (string) parser — not the bigint one injected by pgque.
//
// Implementation strategy: we don't actually connect to PG. We use the
// `pg.types.getTypeParser` API directly to read the registered parser for
// OID 20, which is exactly what pg's query engine calls internally.
// ---------------------------------------------------------------------------

describe('pgque import must not mutate global pg-types parser (OID 20)', () => {
  it('global int8 parser is string-returning before pgque import', () => {
    // pg's built-in default: parse int8 as string.
    const parser = pg.types.getTypeParser(20, 'text');
    const result = parser('9007199254740993'); // larger than MAX_SAFE_INTEGER
    expect(typeof result).toBe('string');
  });

  it('after importing pgque, a second pg.Pool still gets string for int8 (global parser is unchanged)', async () => {
    // Import pgque — on unfixed code this mutates the global parser.
    await import('../src/index.js');

    // The global parser for OID 20 must still return string (default pg behaviour).
    // On the unfixed code, pgque's module-load `types.setTypeParser(20, ...)` call
    // will have made this return `bigint` — so this assertion will FAIL (red).
    const globalParser = pg.types.getTypeParser(20, 'text');
    const result = globalParser('9007199254740993');
    expect(typeof result).toBe('string');
  });

  it('pgque pool queries still receive bigint for int8 columns (per-pool parser)', async () => {
    // This test verifies that the FIX actually works: pgque internally uses a
    // per-pool CustomTypesConfig so its own queries receive bigint, while the
    // global parser remains untouched.
    //
    // We mock pg.Pool to intercept the types config passed to the constructor,
    // then call the parser that pgque registered on the pool to confirm it
    // returns bigint.
    //
    // On unfixed code: pgque relies on the global parser so the pool is
    // constructed without a custom `types` option — this test will FAIL (red).
    // On fixed code: pgque passes `types: { getTypeParser }` to the pool —
    // this test will PASS (green).

    const capturedConfigs: pg.PoolConfig[] = [];
    const OriginalPool = pg.Pool;

    // Patch pg.Pool constructor to capture configs.
    const MockPool = vi.fn(function (this: InstanceType<typeof pg.Pool>, config: pg.PoolConfig) {
      capturedConfigs.push(config);
      // We don't actually connect — just need the constructor argument.
      // Return a stub with the methods Client uses.
      return {
        connect: vi.fn().mockResolvedValue({ release: vi.fn() }),
        end: vi.fn().mockResolvedValue(undefined),
        query: vi.fn().mockResolvedValue({ rows: [] }),
      };
    }) as unknown as typeof pg.Pool;

    // Temporarily replace pg.Pool.
    const pgModule = pg as unknown as { Pool: typeof pg.Pool };
    pgModule.Pool = MockPool;

    try {
      const { connect } = await import('../src/client.js');
      // connect() will call `new Pool(...)` — capture its config.
      // It also calls pool.connect() for the probe; our stub handles that.
      await connect('postgres://fake@localhost/fake').catch(() => undefined);
    } finally {
      pgModule.Pool = OriginalPool;
    }

    // At least one Pool was constructed.
    expect(capturedConfigs.length).toBeGreaterThan(0);
    const config = capturedConfigs[0]!;

    // On the fix, pgque passes a `types` object with a `getTypeParser` method
    // that returns a bigint parser for OID 20.
    expect(config.types, 'pgque must pass a custom types config to the pool').toBeDefined();
    const customParser = config.types!.getTypeParser(20, 'text');
    const parsed = (customParser as (val: string) => unknown)('9007199254740993');
    expect(typeof parsed).toBe('bigint');
  });
});
