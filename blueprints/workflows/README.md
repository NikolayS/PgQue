# PgQue Durable Workflows — spec (experimental, samospec-authored)

This directory holds the versioned specification for the proposed
**event-sourced durable-execution layer** on PgQue (see
`../DURABLE_EXECUTION_FEASIBILITY.md` for the strategy this spec realizes).

- `SPEC.md` — the spec (current version in its header).
- `BRIEF.html` / `index.html` — self-contained HTML brief (derivative of
  SPEC.md). `index.html` is the GitHub Pages entry point.
- `TLDR.md`, `decisions.md`, `changelog.md`, `architecture.json` — auxiliary
  artifacts.

Authored and iterated with [samospec](https://github.com/NikolayS/samospec)
running an all-Claude review panel (lead + two reviewer personas). Each
version is committed and the brief republished.

Status: **experimental** — ships as optional `sql/experimental/durable.sql`
gated by the project promotion rule.
