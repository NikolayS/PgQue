# samospec — per-seat configurable review adapter (transport)

This branch only carries a patch for **NikolayS/samospec** (a different
repo). It is NOT meant to merge into PgQue — it is a transport because this
environment's git proxy only allows pushing to nikolays/pgque.

## Apply to samospec

```bash
git clone https://github.com/NikolayS/samospec && cd samospec
git checkout -b feat/configurable-reviewer-adapter v0.9.0
git am ../samospec-configurable-adapter.patch   # preserves the commit
# or: git apply ../samospec-configurable-adapter.diff
git push -u origin feat/configurable-reviewer-adapter
```

Single commit `64a09a1` off `v0.9.0`. Commit is unsigned (env signing
server 400s outside pgque) — re-sign if required.

## What it does
Adds a per-seat `adapters.<seat>.adapter` enum ("claude" | "codex") in
`.samo/config.json`. Reviewer A is no longer hardcoded to codex: set
`adapters.reviewer_a.adapter: "claude"` to run an all-Claude panel. A
Claude Reviewer A keeps the verbatim security/ops persona and joins the
lead+reviewer_b shared resolver (SPEC §11). Default codex preserved;
non-breaking. Tests: new+from-config 63/63, all adapter 335/335, typecheck
+ lint clean.
