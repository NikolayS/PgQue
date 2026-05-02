# CLAUDE.md -- PgQue

## Project

PgQue (pgque) -- PgQ Universal Edition. Zero-bloat PostgreSQL queue repackaged
from PgQ for managed database environments. Repo: NikolayS/pgque.

## Naming Convention

- `pgque` -- lowercase for schema, package names, CLI, repo, function prefixes
- `PgQue` -- capitalized form for prose, headings, README titles
- Mirrors PostgreSQL ecosystem convention (pgmq, pg_cron, psycopg)

## Architecture

Two-layer design:

- **pgque-core:** Productization of PgQ (rename, PG14+ modernization, pg_cron,
  security hardening, single-file install). Mechanical transformation of ~4,028
  lines of proven PL/pgSQL.
- **pgque-api:** Modern convenience layer (send/receive/ack/nack, DLQ, delayed
  delivery). New code (~1,500 lines) that must reduce cleanly to PgQ primitives.

See `blueprints/SPECx.md` for the full specification.
See `blueprints/SPEC.md` for PgQ internals reference (rotation mechanics,
snapshot isolation, batch_event_sql algorithm, dual-filter optimization).

## Stack

- Pure SQL + PL/pgSQL (no extensions, no `.control` file)
- Anti-extension design: `\i pgque.sql` to install
- Works on RDS, Aurora, Cloud SQL, AlloyDB, Supabase, Neon, Crunchy Bridge
- Optional pg_cron for automated ticker and maintenance
- PostgreSQL 14+ required (uses `pg_snapshot`, `xid8`)

## Style Rules

These rules are the source of truth for agentic engineering in this repo. Agents and AI coding tools must read this file before making changes.

### SQL Style

- Lowercase SQL keywords: `select`, `create function`, not `SELECT`, `CREATE FUNCTION`
- `snake_case` for all identifiers
- Schema-qualify all internal references: `pgque.queue`, not just `queue`
- All `SECURITY DEFINER` functions MUST include
  `SET search_path = pgque, pg_catalog`
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
pgque/
  pgq/                 -- git submodule (github.com/pgq/pgq, pinned tag)
  build/
    transform.sh       -- mechanical rename + modernization script
  blueprints/          -- specs, design docs, brainstorms
    SPECx.md           -- pgque specification (the main spec)
    SPEC.md            -- PgQ internals reference (from pgq repo)
    PHASES.md          -- what ships in v0.1 vs experimental
  sql/                 -- source SQL files
    pgque.sql  -- single-file install (built from sources)
    pgque-unpgque.sql
  tests/               -- regression tests (sql/ + expected/)
  docs/                -- user-facing documentation (flat layout)
    README.md          -- index of the docs directory
    tutorial.md        -- hands-on walkthrough (start here for new users)
    reference.md       -- per-function reference for the v0.1 install
    examples.md        -- short patterns (fan-out, exactly-once, etc.)
    benchmarks.md      -- throughput numbers + methodology
    pgq-concepts.md    -- contributor glossary (batch, tick, rotation)
    pgq-history.md     -- short timeline from Skype to PgQue
  clients/             -- client libraries
    python/            -- pgque-py
    go/                -- pgque-go
    typescript/        -- pgque-ts
  cli/                 -- pgque CLI (Go)
```

## Agentic Engineering Rules

- Use red/green TDD for new code: write the failing test first, then the implementation.
- Keep changes surgical: one logical fix or feature per PR.
- Preserve PgQ core behavior unless the change is intentional, documented, and tested.
- Keep the default install managed-Postgres-compatible: no C extension, no `shared_preload_libraries`, no restart requirement.
- Keep generated files and source changes consistent when both are affected.
- Include the relevant test or manual verification command in PR descriptions.

## Key Design Rules

1. **Every SECURITY DEFINER function must pin search_path.** No exceptions.
2. **The PgQ engine is sacred.** Batch/tick/rotation/consumer tracking logic
   is inherited from PgQ and must not be modified without deep understanding.
3. **Modern API functions must reduce cleanly to PgQ primitives.**
   If `pgque.send()` cannot be explained as "calls `pgque.insert_event()` with
   these args," it is too complex.
4. **No subtransactions in hot paths.** Avoid `BEGIN ... EXCEPTION WHEN ... END`
   in ticker, event insertion, and trigger functions.
5. **Test against PG 14, 15, 16, 17, 18.** CI must cover all supported versions.
6. **Red/green TDD for all new code.** Write the failing test first, then
   the implementation. Applies to pgque-api, observability, client libraries,
   CLI. Does NOT apply to pgque-core repackaging (PgQ already has tests).
   See SPECx.md section 13.2.

## Copyright

Copyright 2026 Nikolay Samokhvalov. Apache-2.0 license.

PgQue includes code derived from PgQ (ISC license, Marko Kreen / Skype Technologies OU).
Always preserve the PgQ copyright notice in source headers.
