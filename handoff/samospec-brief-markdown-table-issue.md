# Issue: deterministic `brief` HTML doesn't render GFM tables — raw markdown leaks as a paragraph

**Repo:** NikolayS/samospec · **Version:** 0.9.0 · **Type:** bug (renderer)

## Symptom

The non-AI (deterministic) `samospec brief <slug>` emits any GitHub-flavored
**markdown table** as raw pipe text wrapped in a single `<p>`, instead of an
HTML `<table>`. On the published page the whole table collapses into one
run-on paragraph of `| col | col | |---|---| | cell | cell |`.

Observed on a real spec (section "Scope & resolved interview decisions"):

```html
<p>| Question | Decision (v0.1, carried unchanged through v0.3) | |---|---| | Primary users | Backend engineers ...
```

`grep -c '<table' BRIEF.html` → **0**; the markdown table survives verbatim.

## Steps to reproduce

```bash
# any spec whose SPEC.md contains a GFM table (samospec's own baseline
# "Scope" section is a table), then:
samospec publish demo
samospec brief demo            # deterministic renderer
grep -c '<table' blueprints/demo/BRIEF.html      # -> 0
grep -c '| ---' blueprints/demo/BRIEF.html        # -> raw pipes present
```

## Root cause

`src/render/brief.ts` markdown->HTML handles headings / paragraphs / lists /
code / inline emphasis, but has **no GFM table support**, so a `| … |` block
falls through to the paragraph path. (`src/render/brief-ai.ts` *does* render
tables — "side-by-side scope tables", "decision log table" — so only the
deterministic path is affected.)

This matters because samospec's own **mandatory baseline sections** include a
Scope section that is conventionally a table, so a large fraction of real specs
hit this on the default (non-AI) brief.

## Expected

GFM tables render as HTML `<table>` in the deterministic brief.

## Suggested fixes

1. Add GFM table parsing to the deterministic markdown->HTML in `brief.ts`
   (header row + `|---|` delimiter row + body rows -> `<table><thead>/<tbody>`).
2. Or reuse a small GFM-aware renderer for the deterministic path.
3. Short-term: document that table-containing specs need `brief --ai`, and/or
   warn when a `| ... |\n|---|` block is detected on the deterministic path.

## Environment

samospec 0.9.0 (from source), bun 1.3.11, Linux. Reproduced on a published
GitHub Pages brief (deterministic renderer).
