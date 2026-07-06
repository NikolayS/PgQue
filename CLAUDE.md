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

- Follow the Postgres.AI SQL style guide:
  https://gitlab.com/postgres-ai/rules/-/blob/main/rules/development__db-sql-style-guide.mdc
  The rules below restate its core plus pgque-specific additions; on any
  conflict, this file wins.
- Prefer C-style `/* ... */` block comments for anything spanning 2+ lines;
  use `--` only for single-line comments.
- Collapse single-argument keyword clauses onto one line (`select 1`,
  `from t`, `where x = y`, `into v`); expand only multi-argument clauses,
  one argument per line.
- Lowercase SQL keywords: `select`, `create function`, not `SELECT`, `CREATE FUNCTION`
- `snake_case` for all identifiers
- Schema-qualify all internal references: `pgque.queue`, not just `queue`
- **Root SQL keywords must be left-aligned within the statement.** Do not use
  decorative vertical indentation like `select` / `from` / `where` shifted by
  different amounts. This is non-negotiable for generated and source SQL.
- Put `and` / `or` at the start of continuation lines, aligned under the
  statement's `where` clause indentation.
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

### Documentation

- **Write for new users.** README and docs target someone arriving today, not
  someone migrating from an older state. Do not include "before X" or
  "previously did Y, now does Z" framing in the README or under `docs/`.
  Migration notes belong in release notes (`CHANGELOG.md` / GitHub releases),
  full stop.
- **No PR or issue numbers, and no concrete version tags, in the README or
  `docs/` unless it really helps the reader.** No `#123`, no `(see PR #456)`,
  no `refs #102, #106`, no `Breaking change in v0.2`, no `PgQue v0.2.0 has
  …`. Refer to behavior, not the change history or release labels that
  produced it. (External citations to other projects' issue trackers, used
  as evidence for a claim, are fine. Version tags are fine in roadmap-style
  contexts under `blueprints/` where the version IS the topic.) PR/issue
  numbers and version tags may appear in commit messages, release notes,
  `blueprints/`, and `CONTRIBUTING.md`.
- **Installation docs must be self-complete but not bloated.** Every
  install/quickstart doc covers: install command, ticker setup (or how to skip
  it), and role grants (`pgque_reader` / `pgque_writer` / `pgque_admin`,
  including the produce+consume case that needs both). One short snippet,
  not a tour.

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
    reference.md       -- per-function reference for the default install
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

## PR Lifecycle

Every PR walks through this loop. Do not skip steps; do not reorder them.

1. **CI green.** All check runs on the head commit must pass before review.
   If CI is red, fix the failure first (red/green TDD when the fix is code:
   reproduce in a failing test, make it green, then refactor).
2. **Review done.** Use REV (`https://gitlab.com/postgres-ai/rev/`) — it
   targets GitLab MRs natively but works on GitHub PRs too. Ignore SOC2
   findings; this repo does not need them. Address blocking findings;
   reply to non-blocking with rationale or a follow-up.
3. **Real testing, where it makes sense.** Walk the change as a new user
   would. For docs PRs that means executing the snippets verbatim against
   a fresh Postgres. For code PRs that means running the affected tests
   and any manual verification command listed in the PR description. Post
   the evidence (commands run, output, screenshots when relevant) as a
   PR comment so a reviewer can see what was checked.
4. **Approve and merge.** If 1–3 are clean, approve, merge, and delete
   the branch. Otherwise return to the author with a concrete fix list
   and LOOP from step 1 on the next push.

For any code change made during the loop, follow red/green TDD: failing
test first, then the implementation that turns it green.

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
