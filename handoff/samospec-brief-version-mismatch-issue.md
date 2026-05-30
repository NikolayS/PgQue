# Issue: `brief` HTML shows two different versions at once (stale `published_version`)

**Repo:** NikolayS/samospec · **Version:** 0.9.0 · **Type:** bug
**Related to:** the publish-republish issue (`samospec-publish-republish-issue.md`) —
this is a second, user-visible symptom of the same root cause.

## Symptom

The generated brief's `<title>` (and subtitle) shows **two different version
numbers simultaneously**, e.g.:

```
Brief — PgQue Durable Workflows — SPEC v0.3 (v0.1)
```

- `v0.3` is correct (the spec's current version).
- `(v0.1)` is stale and wrong.

## Root cause

`brief` composes the title from **two independent version sources**:

- the spec **H1 line** (`# … SPEC vX`), which reflects the current spec, and
- **`state.published_version`** (`src/cli/brief.ts:195` →
  `publishedVersion: state.published_version ?? "v0.0"`).

`state.published_version` is written **once, at the first `samospec publish`**,
and is **never updated afterward** — because `publish` refuses to re-promote
(see the related issue). So as soon as the spec is iterated past the first
published version, the H1 advances but `published_version` is frozen, and the
brief stamps both: current H1 version + stale published version.

## Steps to reproduce

```bash
samospec new demo --idea "x" --yes
samospec publish demo            # sets state.published_version = "v0.1"
samospec iterate demo --rounds 1 # spec H1 -> v0.2, state.published_version still "v0.1"
# (re-promote the draft however you can — e.g. the file-copy workaround from the
#  related issue, since `publish` refuses to run again)
samospec brief demo
grep -o '<title>[^<]*</title>' blueprints/demo/BRIEF.html
# -> Brief — ... SPEC v0.2 (v0.1)
```

## Expected

A single, consistent version in the brief — matching the spec being summarized.

## Suggested fixes (any one resolves it)

1. Derive the brief version from a **single source of truth** — parse it from the
   spec H1 (`# … vX`) rather than from `state.published_version`.
2. Make re-promotion (the `--force/--republish` fix in the related issue) update
   `state.published_version`, so it can never lag the published `SPEC.md`.
3. If both sources are kept, assert they agree and fail/raise on mismatch rather
   than silently emitting two versions.

## Environment

samospec 0.9.0 (from source), bun 1.3.11, Linux.
