# Contributing to PgQue

Thanks for helping improve PgQue.

## Start here

- Read [README.md](README.md) for the project overview.
- Read [docs/tutorial.md](docs/tutorial.md) for the user-facing flow.
- Read [docs/reference.md](docs/reference.md) for the default install API.
- Read [blueprints/SPECx.md](blueprints/SPECx.md) before changing API behavior.
- Read [docs/pgq-concepts.md](docs/pgq-concepts.md) if you are new to PgQ terms such as batch, tick, rotation, and consumer cursor.

## Agentic engineering

This repository includes [CLAUDE.md](CLAUDE.md). Agentic coding tools and AI reviewers should load it before making changes. It contains PgQue-specific architecture, style, safety, testing, and commit rules.

Human contributors should also skim it: the SQL style and design rules there are normative for this repo.

## Development loop

PgQue uses red/green TDD for new code:

1. Write the failing test first.
2. Commit the RED test when practical.
3. Implement the smallest fix.
4. Run the focused test.
5. Run the broader validation that matches the change.

For pure documentation changes, run at least `git diff --check` and inspect rendered Markdown when formatting is non-trivial.

## Build

The canonical install script is generated:

```bash
bash build/transform.sh
```

This assembles `sql/pgque.sql` from source SQL files and runs build-time checks. Do not hand-edit generated sections in `sql/pgque.sql` without also updating the source file that generates them.

## Tests

At minimum for SQL/API changes:

```bash
# install sql/pgque.sql into a fresh PostgreSQL database first
psql -v ON_ERROR_STOP=1 -f tests/test_api_send.sql
```

Before merge, run the full SQL suite on a fresh database:

```bash
psql -v ON_ERROR_STOP=1 -f tests/run_all.sql
```

CI must stay green for PostgreSQL 14, 15, 16, 17, and 18.

## SQL style

Follow [CLAUDE.md](CLAUDE.md) and the shared Postgres.ai rules. Highlights:

- lowercase SQL keywords
- `snake_case` identifiers
- public SQL argument names matter; PostgreSQL supports named calls (`arg := value`)
- prefer simple public argument names such as `queue_name`, `type_name`, `payload`, `payloads`
- schema-qualify internal references (`pgque.queue`, `pgque.insert_event`)
- every `security definer` function must set `search_path = pgque, pg_catalog`
- avoid `begin ... exception when ... then null` in hot paths and avoid silent exception swallowing in general
- keep PgQ-compatible behavior unless intentionally changing it and documenting why

## API compatibility

Public SQL signatures include argument names, not just types. Changing argument names can break named-argument callers:

```sql
select pgque.send_batch(
    queue_name := 'orders',
    type_name := 'order.created',
    payloads := array['{"id":1}'::jsonb]
);
```

When adding or changing public functions, update:

- SQL source under `sql/`
- generated `sql/pgque.sql` via `bash build/transform.sh`
- tests under `tests/`
- user docs under `docs/`
- relevant dated notes in `blueprints/SPECx.md` when the spec is superseded
- client libraries only if they call the changed SQL surface in a way affected by the change

## Pull requests

Keep PRs focused. Use conventional commits:

```text
feat(api): add default send_batch overloads
fix(roles): avoid exception-based grant cleanup
docs(api): clarify publishing argument types
```

PR lifecycle:

1. CI green.
2. REV-style review done; ignore SOC2-only items unless relevant.
3. Actual testing evidence posted to the PR.
4. Maintainer approval.
5. Merge.

Do not merge without explicit maintainer approval.
