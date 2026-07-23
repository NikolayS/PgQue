# TL;DR

## Goal

> Status: **experimental**, ships as optional `sql/experimental/durable.sql` gated by the project promotion rule. Workflow support ships first as **one thin-SQL-wrapper reference client (Python)**; the other PgQue clients (Go, TypeScript, + WIP) are a planned follow-up, not v0.1 (§7–§9, §12). Engine layer is sacred and untouched. Fresh prior art: `pg_durable` is considered explicitly, but the PgQue boundary remains workflow durability in Postgres with workflow code in application repositories.

## Scope summary

- 1. Goal & why it's needed
- 2. Scope & resolved interview decisions
- 3. User stories
- 4. Architecture
- 5. Implementation details
- 6. Tests plan
- 7. Team (veteran experts to hire)
- 8. Implementation plan (sprints, parallelization, ordering)
- 9. Topic-specific: API surface (reference SDK, Python v0.1)
- 10. Operability notes (managed-PG)
- 11. Open items carried to v0.6
- 12. Non-goals / disclaimers (honored strictly — not reintroduced anywhere above)
- 13. Embedded Changelog

## Next action

Keep as draft until the benchmark hypothesis is tested and the `pg_durable`
comparison is reviewed.
