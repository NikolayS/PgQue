# Issue: `publish` hard-blocks re-promotion — no republish path, brief stuck on first version

**Repo:** NikolayS/samospec · **Version:** 0.9.0 (run from source, bun 1.3.11, Linux)
**Type:** bug / missing-feature (UX contradiction)

## Summary

After the first `samospec publish <slug>`, there is **no way to re-promote an
iterated spec**. `publish` refuses to run again, and `brief` only ever reads the
**published** `SPEC.md`. So an iterative workflow — "run a review round, publish
the new version, regenerate the brief, repeat" — is impossible without manually
copying files. The blocking error even tells the user to "**then republish**",
but no republish command/flag exists.

## Steps to reproduce

```bash
samospec new demo --idea "anything" --yes        # drafts v0.1 (working draft)
samospec publish demo                            # promotes v0.1 -> blueprints/demo/SPEC.md
                                                 # sets state.published_at
samospec iterate demo --rounds 1                 # working draft advances to v0.2
samospec publish demo                            # <-- FAILS
samospec brief demo                              # regenerates brief from the STALE v0.1 snapshot
```

## Actual

`samospec publish demo` (the 2nd time) exits 1 with:

```
samospec: 'demo' is already published at 2026-05-30T09:41:56.622Z.
Use `samospec iterate demo` to run more rounds, then republish.
```

- The message says "then republish" but there is **no `republish` command and no
  `--force`/`--republish` flag** — the guidance is unactionable.
- `blueprints/demo/SPEC.md` stays at v0.1; `brief` (which reads
  `<blueprints_dir>/<slug>/SPEC.md` and requires `state.published_at`) keeps
  emitting a v0.1 brief even though the working draft is v0.2+.

## Expected

A supported way to re-promote the current working draft (so
`blueprints/<slug>/SPEC.md` and the brief reflect the latest iterated version),
matching the README's "iterate → publish" loop.

## Root cause

- `src/cli/publish.ts` (~L137): the republish guard errors whenever
  `state.published_at !== undefined`, with no override.
- `src/cli/brief.ts` (header): "Requires a published spec... Reads
  `<blueprints_dir>/<slug>/SPEC.md` as the canonical source" — so the brief can
  only ever reflect the published snapshot, never a newer working draft.

## Suggested fixes (any one unblocks it)

1. Add `samospec publish <slug> --force` (or `--republish`) that re-promotes the
   current committed working draft and updates `state.published_at`.
2. Or auto-allow republish when the working draft's version (or content hash) is
   newer than the published snapshot.
3. Make the "then republish" message actionable (point at the real flag).
4. Optionally: `samospec brief <slug> --from-draft` to preview a brief from the
   working draft without publishing.

## Workaround

Copy the working draft over the published snapshot, then run brief:

```bash
cp .samo/spec/<slug>/SPEC.md blueprints/<slug>/SPEC.md
samospec brief <slug>
```
