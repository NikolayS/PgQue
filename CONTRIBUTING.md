# Contributing to PgQue

Thanks for helping improve PgQue.

PgQue is a pure SQL / PL/pgSQL repackaging of PgQ for managed Postgres environments. Keep changes small, testable, and compatible with managed Postgres unless a document explicitly says otherwise.

## Start here

- Read [README.md](README.md) for project positioning and user-facing behavior.
- Read [blueprints/SPECx.md](blueprints/SPECx.md) for the current specification and implementation plan.
- Read [CLAUDE.md](CLAUDE.md). It is the source of truth for agentic engineering rules, style, design constraints, and project conventions.

## Development rules

Use [CLAUDE.md](CLAUDE.md) for the detailed rules. In short:

- Use red/green TDD for new code: failing test first, implementation second.
- Keep PRs focused: one logical change per PR.
- Preserve PgQ core behavior unless the change is intentional, documented, and tested.
- Keep the default install managed-Postgres-compatible.
- Keep generated files and source changes consistent when both are affected.

## Tests

Run the relevant SQL regression tests before opening a PR. For changes touching supported-version behavior, test against the supported Postgres matrix when possible.

At minimum, include the test or manual verification command in the PR description.

## Pull requests

- Use a descriptive branch name.
- Explain the motivation and the user-visible change.
- Link related issues or design docs.
- Keep PRs focused and easy to review.
