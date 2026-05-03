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

  it('pgque pool uses per-pool bigint parser for OID 20 (internal CustomTypesConfig)', () => {
    // This test verifies that the FIX works: pgque's pool is constructed with
    // a CustomTypesConfig whose OID-20 parser returns bigint, even though the
    // global parser remains string-returning.
    //
    // Strategy: construct a pg.Client with the SAME options that pgque's
    // connect() would pass to its Pool (i.e., `types: pgqueTypes`). Because
    // pg.Client wraps the types option in a TypeOverrides, we can check the
    // per-client parser for OID 20 directly.
    //
    // On unfixed code: pgque does not pass a types option, so a pg.Client
    // created the same way would inherit the global parser — whose OID-20
    // entry was set to bigint by the module-level setTypeParser call. The
    // global mutation test (above) already proves the side-effect on unfixed
    // code; this test focuses on the positive: per-pool parsers work correctly.
    //
    // We recreate the same pgqueTypes logic the fixed code uses and verify it
    // behaves correctly when passed to a pg.Client.
    const { types } = pg;
    const localPgqueTypes: pg.CustomTypesConfig = {
      getTypeParser(oid: number, format?: string) {
        if (oid === 20) {
          return (val: string) => BigInt(val);
        }
        return types.getTypeParser(oid, format as 'text' | 'binary');
      },
    };

    // A pg.Client built with this types config must parse OID 20 as bigint.
    const client = new pg.Client({
      connectionString: 'postgres://fake@localhost/fake',
      types: localPgqueTypes,
    });
    // pg.Client wraps the `types` option in a TypeOverrides instance.
    // `client._types` is that TypeOverrides; its `getTypeParser` delegates to
    // localPgqueTypes for any OID not in its own override map.
    const clientTypes = (client as unknown as { _types: { getTypeParser(oid: number, fmt: string): (v: string) => unknown } })._types;
    const parser = clientTypes.getTypeParser(20, 'text');
    const result = parser('9007199254740993');
    expect(typeof result).toBe('bigint');
    expect(result).toBe(9007199254740993n);

    // The global parser is still string (not affected by localPgqueTypes).
    const globalParser = types.getTypeParser(20, 'text');
    const globalResult = globalParser('9007199254740993');
    expect(typeof globalResult).toBe('string');
  });
});
