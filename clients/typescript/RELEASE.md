# TypeScript client release

Package name: `pgque` on npm.

Install:

```bash
npm install pgque
# or
bun add pgque
```

## Versioning and compatibility

The TypeScript client version is independent from the SQL/server
`pgque.version()`. Bump this package when the TypeScript API, runtime behavior,
generated types, or packaging changes. Server-only SQL changes do not require an
npm release unless they change behavior expected by the TypeScript client.

The TypeScript package declares the minimum compatible PgQue SQL API/server
version in docs and tests. If a client feature requires a newer SQL API, client
code should fail with a clear compatibility error rather than surfacing a raw
missing-function SQL error.

This matters especially for experimental cooperative consumers / subconsumers:
the client must not silently call SQL functions that may not exist.

Use an explicit compatibility table in the client README and release notes, for
example:

| npm package | PgQue SQL/server | Notes |
|---|---|---|
| `0.2.x` | `pgque.version() >= 0.2.0` | Includes experimental cooperative consumers if enabled. |

## Experimental features

If a release exposes cooperative consumers / subconsumers, mark the feature as
**experimental** in release notes and public docs. The API and edge-case behavior
are not stable until PgQue explicitly graduates the feature.

## Package shape

- Runtime support: Node.js 20+.
- Release workflow runtime: Node.js 22.14.0+ with npm 11.5.1+ for Trusted
  Publishing / provenance.
- Module format: ESM.
- `package.json` must contain `"type": "module"` and a well-defined `exports`
  map.
- Entry point: `dist/index.js`.
- Type declarations: `dist/index.d.ts`.
- Source maps should be published as `dist/*.js.map` so production stack traces
  point back to TypeScript sources.
- Development package manager: Bun.
- Published package contents are controlled by `package.json#files`.

The package is for server-side Node/Bun applications. It depends on `pg`
(`node-postgres`) and is not intended for browsers.

Before the first stable npm release, decide and document the `pg` dependency
strategy:

- Prefer `peerDependencies` plus `devDependencies` if PgQue should share the
  host application's existing `pg.Pool` / `pg.Client` and avoid duplicate driver
  copies.
- Keep `dependencies` only if PgQue intentionally owns its own driver version
  and managed connection pool.

The README must state how database access is provided: whether users pass an
existing `pg.Pool` / `pg.Client`, call PgQue's `connect()` helper to create a
managed pool, or both.

## One-time setup

The release workflow is `.github/workflows/release-typescript.yml`.

Before the first real publish:

1. Create GitHub environment `npm` in `NikolayS/pgque`. Protect it as
   appropriate for releases, for example:
   - required reviewers
   - deployment branch restriction to `main`
   - no self-approval, if practical
2. Ensure the release workflow runs on GitHub-hosted runners. npm Trusted
   Publishing does not currently support self-hosted runners.
3. Ensure the publish job has OIDC permissions:

   ```yaml
   permissions:
     contents: read
     id-token: write
   ```

4. In npm, configure Trusted Publishing for:
   - package: `pgque`
   - repository owner: `NikolayS`
   - repository: `pgque`
   - workflow filename: `release-typescript.yml`
   - environment: `npm`
5. Verify `package.json#repository.url` and `repository.directory` exactly match
   the GitHub repository and package location expected by npm.

The workflow also checks that it is running from `main`, but GitHub environment
protection is the human approval gate.

### First publish caveat

If `pgque` does not yet exist on npm, verify that npm currently supports
configuring Trusted Publishing before initial publication. If it does not, do a
one-time manual/token-based initial publish by a trusted maintainer, immediately
configure Trusted Publishing, then disable token publishing.

After Trusted Publishing is configured, prefer npm's publishing-access setting
that requires 2FA and disallows traditional tokens.

Do not assume first package creation through OIDC is deterministic across npm
policy changes.

## Per-release workflow

1. Update `clients/typescript/package.json` version.
2. Update release notes/changelog if present.
3. Update the client README compatibility table when SQL/server compatibility
   changes.
4. Merge the release prep PR to `main`.
5. Ensure the `npm` GitHub environment exists and is protected.
6. Ensure npm Trusted Publishing is configured exactly for this repository,
   workflow filename, and environment.
7. Run **Release TypeScript client** with `dry_run=true`.
8. Verify:
   - build output
   - generated `dist/`
   - packed file list
   - package metadata
   - no tests, source-only internals, local config, or secrets are included
9. Before publishing, run the TypeScript integration tests against the oldest
   supported PgQue SQL/server version and the current `main` SQL version.
10. Run the workflow again with `dry_run=false`.

`dry_run=true` verifies build/package contents only. It does not fully validate
npm Trusted Publishing. The real publish may still fail if npm's Trusted
Publisher configuration, GitHub environment, workflow filename, branch, runner
type, OIDC permissions, or `package.json#repository.url` do not match exactly.

The workflow installs with `bun install --frozen-lockfile`, runs `bun run check`,
runs `bun run test`, builds `dist/`, verifies npm >= 11.5.1, and publishes via
npm Trusted Publishing / OIDC. No long-lived npm token is needed.

## Future automation

Consider Changesets or Release Please once releases become routine. Manual
version bumps and changelog updates are acceptable for early releases, but they
are exactly the kind of boring human step that fails on Friday afternoon.
