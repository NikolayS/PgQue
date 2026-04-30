# PgQue v0.2.0 sprint — handoff

**As of:** 2026-04-30 (mid-day Pacific). Current session is mid-sprint.

## What this is

A handoff doc for an engineer / manager picking up the in-flight v0.2.0 release sprint for [NikolayS/pgque](https://github.com/NikolayS/pgque). The work is being coordinated by a Claude-Code "manager" role: spawns engineer agents, spawns reviewer agents, tracks state, surfaces decisions to the user (Nikolay) for final merge approval. The manager does NOT write code, run tests, or merge PRs.

## Where to find the rest

- **Project memory** (always read first): `/Users/nik/.claude/projects/-Users-nik-github-pgque/memory/MEMORY.md` plus the files it indexes. Most importantly `project_status.md` and the `feedback_*.md` files.
- **Repo**: `/Users/nik/github/pgque`. Clone of `git@github.com:NikolayS/pgque.git`.
- **Project rules**: `CLAUDE.md` at repo root.
- **Sprint plan / parent issue**: GitHub issue #85 — see also #113 (umbrella audit triage).
- **REV review tool**: `/Users/nik/.claude/rev/` (GitLab-native; manager adapts the parallel-agent prompts for GitHub PRs).

## Sprint goal

Ship v0.2.0:
1. Drivers (Go / Python / TypeScript) polished, tested, published-ready.
2. Critical bug fixes from the raw-SQL audit (#113 umbrella).
3. CI exercising live PG with per-client visibility.
4. README mention of `benchmark/` (the larger hero rewrite was reverted; minimal scope only).
5. **No SOC2** in REV review (project policy).
6. **Strict anti-leak**: no `gitlab.com`, `sahmed`, `Artifact Registry` / `AR`, `WI #76` / `WI #77` / `Round 8` / `R8` references in any commit, PR, doc, or code comment. Issue thread comments are exempt; commit messages and PR descriptions are not.

## Roles + lifecycle

- **Engineer agent**: writes code with strict red/green TDD (failing test FIRST in its own commit, then fix in a separate commit). Pushes to a branch, opens a PR. Comments progress on the linked GitHub issue.
- **Reviewer agent** (separate from engineer): runs REV-style multi-perspective review (security / bug-hunter / test-analyzer / guidelines / docs; SKIP SOC2). Posts a structured review on the PR. Loops with the engineer until clean.
- **Manager (this role)**: spawns / coordinates / tracks. Files follow-up issues. Cleans up PR titles + bodies for anti-leak (metadata only — no code). Presents cleared PRs to user.
- **User (Nikolay)**: final review + merge authority. **Manager NEVER merges.**

## Current state — PRs

### Merged on main during this sprint

| PR | What |
|---|---|
| #75 | bugfix follow-ups |
| #79 | quote_ident fix, dlq_replay_all return-type, Nack tests |
| #78 | Go driver pkg.go.dev polish |
| #80 | bench/xmin-horizon harness |
| #82 | Python driver parity + REV r2 fixes |
| #83 | TypeScript driver parity + REV r2 fixes |
| #114 | receive(max_return) reject < 1 + batch-ownership docs |
| #115 | Go bugfix: Nack `$12` placeholder + eager Connect ping + Consumer panic recovery |
| #66 | benchmark methodology + tooling under benchmark/ + 3 prose corrections |
| #68 | three-latencies dedicated doc |

### Cleared by REV — awaiting user's merge call (11 PRs)

| PR | What | Notes |
|---|---|---|
| #72 | R8 chart analyzers under benchmark/charts/ | Rebased onto post-#66 main, KB→KiB units, README index updated |
| #116 | nack/DLQ canonical re-query + idempotent terminal handling | Closes #98 + #104. Comment-scrub `cae72ec`. REV r2 PASS. |
| #117 | receive() empty-batch consumer-stranding fix | Closes #103. Was the root cause of all 19 Python + 6 TS test failures on PR #84. REV PASS, comment-scrub `b849887`, rebased to `5fab5c4`. |
| #118 | revoke PUBLIC EXECUTE + harden queue_extra_maint | Closes #96 + #101. Uses `pg_get_userbyid(p.proowner)` for managed-DB compat (RDS/Aurora/Cloud SQL). REV r2 PASS, scrub `2634042`. |
| #119 | maint() drops VACUUM (PL/pgSQL forbids it) | Closes #110. RAISE NOTICE on skip + pg_cron alternative documented. REV r3 PASS. Scrub `ea0cabf`. |
| #120 | batch_retry NULL::int8 → NULL::xid8 + transform.sh sedi patch | Closes #107. REV PASS. Scrub `3c31fa3`. |
| #121 | Python consumer nacks unhandled types + send() str payload docs | Closes #111. REV r2 PASS. Docstring clarity fix `c67d0d4`. **OPEN DESIGN QUESTION — see below.** |
| #122 | reject queue names > 57 bytes (pg_notify channel limit) | Closes #109. REV r2 PASS, awk bug fixed, UTF-8 boundary tests. Scrub `6ac8a43`. |
| #126 | docs cleanup trio: reference drift + examples + roles scope | Closes #99 + #105 + #112. REV PASS. |
| #128 | minimal mention of `benchmark/` directory in README | Closes #87. **Scope was reduced significantly** from the original hero rewrite per maintainer; final is `8adb76b` adding ~5 lines to existing `## Benchmarks` section. |

### On hold — explicit user decision

| PR | What | Why held |
|---|---|---|
| #125 | concurrent receive() FOR UPDATE serialization fix | **Held for v0.2.1.** Modifies `next_batch_custom` — flagged as PgQ core engine ("sacred" per CLAUDE.md key design rule #2). Conceptually right, but maintainer wants more rigorous validation (concurrent integration tests under sustained load, deadlock scenario coverage with `maint()`, lock-contention measurement) before shipping. Branch + PR remain open; title prefixed `[ON HOLD v0.2.1]`. |

### In flight (awaiting / will need work post-merge)

| PR | What | Status |
|---|---|---|
| #84 | CI: split client-tests into per-client jobs (Go / Python / TypeScript) | **Blocked on #117 merging.** Branch dropped band-aids; Go now passes (Nack fix on main). Python (19) + TS (6) tests fail because of the receive() empty-batch trap that #117 fixes. Once #117 lands and #84 rebases, all 3 client jobs go green. Manager has been instructed not to merge; engineer will rebase post-#117. |
| #81 | comprehensive Go test coverage (concurrency, edge cases, benchmarks, error paths) | REV r1 verdict REQUEST_CHANGES non-blocking (test-correctness fixes — several tests claim more than they prove; PR body has factual inaccuracies vs post-#115 code). User's call: loop or ship as-is. Comment-scrub sweep determined no narrative scrub needed. |

### Open follow-up issues (filed during sprint, deferred to next bench cycle)

- **#123** — `logging_collector=off ≠ zero PG log I/O` (closed in PR #66 prose)
- **#124** — pgmq-partitioned planner cost is first-query-in-session, not per-query (closed in PR #66 prose; PgBouncer speculation removed in `dbfdfeb`)
- **#127** — NOTICE-vs-pgss observer effect (closed in PR #66 prose)

These three were closed-by-prose-edit on PR #66; the bench-cycle revisit (with measured numbers) is genuine future work, not v0.2.0.

### Deferred per umbrella triage (#113)

- **#100** — config validation / external ticker hardening
- **#102 / #106** — per-queue ACL design call (depends on #112 docs landing first; #112 explicitly says global roles intentionally)
- **#108** — get_batch_cursor(extra_where) docs hardening

## Open design question — PR #121

Maintainer flagged this in PR #121's README example:

> "Without [a `*` catch-all], messages whose type has no handler are nacked automatically (moved to retry_queue) rather than silently dropped."

Question: **why retries for unhandled types?** If the type has no handler, retrying is pointless — the same code path hits the same wall every time, until DLQ at max_retries. That's a slow path to DLQ for a routing failure.

Options the next engineer should bring to the maintainer:

1. **Fast-path to DLQ** on first nack-due-to-no-handler (skip retry_queue entirely). Routing failures land in DLQ immediately for triage.
2. **Configurable `unhandled_action`** on `Consumer.start()`: `"nack"` (current), `"dlq"` (fast-path), `"ack"` (silently consume), `"raise"` (fail loud). Default `"dlq"`.
3. **Keep current behavior + document loudly** — current behavior arguably makes sense if you expect handler registration to race with consumer start (the message gets a second chance after the late handler registers).
4. **Just `RAISE WARNING` and ack** — log it, drop it, move on. Closest to "best-effort consumer" semantics.

Prep this as a small design doc for maintainer review before changing #121's behavior. Don't ship as-is without addressing it; the docstring fix `c67d0d4` was a docstring-only fix, the underlying behavior is unchanged.

## After the merge wave — sprint-end alignment

When the 11 cleared PRs are merged, the manager owes the user a final pass:

1. Re-read `blueprints/SPECx.md` and the audit umbrella `#113`.
2. Verify shipped behavior aligns with SPECx; for any deliberate deviation, write `blueprints/SPECx.amendments.md` (do NOT edit SPECx itself).
3. Write a sprint summary report to chat: what shipped, what's deferred, what amendments the spec needed.
4. Then proceed to the next sprint (whatever that is — TBD with user).

## Active agents at handoff

**None.** All engineer + reviewer agents have returned. The most recent active was `eng-pr121-doc-clarify`, which finished `c67d0d4` for the `send()` str-payload docstring clarity fix.

If you spawn new agents:
- **Manager waves** of 2–3 engineers in parallel to stay under intermittent rate-limit caps (lesson from earlier in the sprint — eng-go-bugfix-1 hit limit at 127 tool uses).
- Use `subagent_type: "general-purpose"`, `isolation: "worktree"`, `model: "sonnet"` for engineers; `model: "opus"` for REV reviewers (the multi-perspective work benefits from deeper reasoning).
- Always include in the prompt: read CLAUDE.md, anti-leak rules, no `--admin`/`--no-verify`/force-push (except `--force-with-lease` for rebase), don't merge.

## Process notes the next manager should know

1. **Source vs built artifact**: SQL changes go in `sql/pgque-api/*.sql` or `sql/pgque-core/*.sql` (sources); `bash build/transform.sh` regenerates `sql/pgque.sql`. **Both** must be in the same commit. PR #114 originally landed only in the built artifact and CI's regen wiped the fix on first run — REV caught it. Subsequent PRs all do source + regen together.

2. **Pre-release migration notes are noise**: there's no v0.1 user upgrading through these specific changes. The maintainer scrubbed all "v0.2 hardening note" / "Migration:" paragraphs from PRs #116 and #118 via the `eng-scrub-migration-notes` agent.

3. **Source-code comments describing the fix narrative**: the maintainer flagged this as CLAUDE.md violation ("don't reference the current task, fix, or callers; they belong in the PR description"). The `eng-comment-scrub-sweep` agent cleaned 5 PRs (#116, #118, #119, #120, #122). #117 was scrubbed individually.

4. **Stray changes leaking across branches**: PR #128 had a `build/transform.sh` patch from PR #122 accidentally bundled in via a `git commit --amend` during anti-leak rewrite. The fix: `git checkout origin/main -- <file>` then commit the cleanup. Agents should avoid amending across multi-PR worktrees.

5. **REV reviewer adapts the GitLab REV tool**: `/Users/nik/.claude/rev/.claude/commands/review-mr.md` defines the 5-perspective parallel-review pattern (security / bugs / tests / guidelines / docs). Reviewer agents replicate the perspectives sequentially against GitHub PRs using `gh pr diff` / `gh pr view`. **SKIP SOC2 — project policy.** Confidence scoring: 0–10, with 8+ blocking, 4–7 potential, <4 filtered.

6. **Anti-leak grep** (use this exact regex):
   ```
   grep -iE "round[ -]?[4-9]\\b|\\bR[4-9]\\b|wi[ -]?#?7[67]|gitlab|sahmed|artifact[_-]?registry|@AR\\b|hetzner|@postgres-ai|nik-[a-z0-9-]+|i-[0-9a-f]{17}"
   ```
   Apply to: README, all `.md`, all source files, commit messages, PR title, PR body. Issue thread comments are exempt. Be mindful of legitimate matches (`R8` in `i4i.2xlarge` instance specs is fine; "Round" in non-numeric senses is fine).

7. **`claude-review` GitHub Action is intermittently broken** (403 token issue). Treat as known infra failure, not a code signal. **Use `--admin` ONLY for that broken gate.** All other CI failures are real.

8. **Self-approval blocked by GitHub** (PR author = NikolayS). Skip `gh pr review --approve`; user merges directly.

## Hand-back protocol

When done with your slice of work, write a follow-up section to this file (don't overwrite) describing what you did + what you handed back. Then ping Nikolay.

---

*This handoff doc lives at `/Users/nik/pgque-v0.2.0-sprint-handoff.md` (outside the repo so it's not accidentally committed).*
