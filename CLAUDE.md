# CLAUDE.md -- pg_current

## Project

pg_current (pg_current) -- PgQ Universal Edition. Zero-bloat PostgreSQL queue repackaged
from PgQ for managed database environments. Repo: NikolayS/pg_current.

## Naming Convention

- `pg_current` -- lowercase for schema, package names, CLI, repo, function prefixes
- `pg_current` -- capitalized form for prose, headings, README titles
- Mirrors PostgreSQL ecosystem convention (pgmq, pg_cron, psycopg)

## Architecture

Two-layer design:

- **pg_current-core:** Productization of PgQ (rename, PG14+ modernization, pg_cron,
  security hardening, single-file install). Mechanical transformation of ~4,028
  lines of proven PL/pgSQL.
- **pg_current-api:** Modern convenience layer (send/receive/ack/nack, DLQ, delayed
  delivery). New code (~1,500 lines) that must reduce cleanly to PgQ primitives.

See `blueprints/SPECx.md` for the full specification.
See `blueprints/SPEC.md` for PgQ internals reference (rotation mechanics,
snapshot isolation, batch_event_sql algorithm, dual-filter optimization).

## Stack

- Pure SQL + PL/pgSQL (no extensions, no `.control` file)
- Anti-extension design: `\i pg_current.sql` to install
- Works on RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon, Crunchy Bridge
- Optional pg_cron for automated ticker and maintenance
- PostgreSQL 14+ required (uses `pg_snapshot`, `xid8`)

## Style Rules

Follow the shared rules at https://gitlab.com/postgres-ai/rules/-/tree/main/rules

### SQL Style

- Lowercase SQL keywords: `select`, `create function`, not `SELECT`, `CREATE FUNCTION`
- `snake_case` for all identifiers
- Schema-qualify all internal references: `pg_current.queue`, not just `queue`
- All `SECURITY DEFINER` functions MUST include
  `SET search_path = pg_current, pg_catalog`
- Use `xid8` for transaction ID columns, `pg_snapshot` for snapshot columns
- Use modern PG14+ function names: `pg_current_xact_id()` not `txid_current()`

### Binary Units

- Always use binary units (KiB, MiB, GiB, TiB) for memory, storage, and data
  sizes in prose, reports, and documentation
- Exception: PostgreSQL config parameters use PG's native format
  (`shared_buffers = '2GB'`, `work_mem = '64MB'`)

### Shell Style

```bash
#!/usr/bin/env bash
set -Eeuo pipefail
```

- 2-space indent, no tabs
- Quote all variable expansions

### Git Commits

- Conventional Commits: `feat:`, `fix:`, `docs:`, `refactor:`, `chore:`
- Subject < 50 chars, present tense
- Never amend -- create new commits
- Never force-push unless explicitly confirmed

## File Organization

```
pg_current/
  pgq/                 -- git submodule (github.com/pgq/pgq, pinned tag)
  build/
    transform.sh       -- mechanical rename + modernization script
  blueprints/          -- specs, design docs, brainstorms
    SPECx.md           -- pg_current specification (the main spec)
    SPEC.md            -- PgQ internals reference (from pgq repo)
    PHASES.md          -- what ships in v0.1 vs experimental
  sql/                 -- source SQL files
    pg_current.sql  -- single-file install (built from sources)
    pg_current-unpg_current.sql
  tests/               -- regression tests (sql/ + expected/)
  docs/                -- user-facing documentation (flat layout)
    README.md          -- index of the docs directory
    tutorial.md        -- hands-on walkthrough (start here for new users)
    reference.md       -- per-function reference for the v0.1 install
    examples.md        -- short patterns (fan-out, exactly-once, etc.)
    benchmarks.md      -- throughput numbers + methodology
    pgq-concepts.md    -- contributor glossary (batch, tick, rotation)
    pgq-history.md     -- short timeline from Skype to pg_current
  clients/             -- client libraries
    python/            -- pg_current-py
    go/                -- pg_current-go
    typescript/        -- pg_current-ts
  cli/                 -- pg_current CLI (Go)
```

## Key Design Rules

1. **Every SECURITY DEFINER function must pin search_path.** No exceptions.
2. **The PgQ engine is sacred.** Batch/tick/rotation/consumer tracking logic
   is inherited from PgQ and must not be modified without deep understanding.
3. **Modern API functions must reduce cleanly to PgQ primitives.**
   If `pg_current.send()` cannot be explained as "calls `pg_current.insert_event()` with
   these args," it is too complex.
4. **No subtransactions in hot paths.** Avoid `BEGIN ... EXCEPTION WHEN ... END`
   in ticker, event insertion, and trigger functions.
5. **Test against PG 14, 15, 16, 17, 18.** CI must cover all supported versions.
6. **Red/green TDD for all new code.** Write the failing test first, then
   the implementation. Applies to pg_current-api, observability, client libraries,
   CLI. Does NOT apply to pg_current-core repackaging (PgQ already has tests).
   See SPECx.md section 13.2.

## Copyright

Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

pg_current includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
Always preserve the PgQ copyright notice in source headers.
