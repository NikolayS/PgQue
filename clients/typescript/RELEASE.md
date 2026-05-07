# TypeScript client release

Package name: `pgque` on npm.

Install:

```bash
npm install pgque
# or
bun add pgque
```

## Versioning

The TypeScript client version is independent from the SQL/server
`pgque.version()`. Bump this package when the TypeScript API, runtime behavior,
or packaging changes; server-only SQL changes do not require an npm release.

Use npm semver in `package.json`. Pre-release forms like `0.2.0-dev.0`,
`0.2.0-rc.1`, or `0.2.0-dev` are valid for npm; publish them with an
appropriate dist-tag if needed.

## Package shape

- Runtime: Node.js 20+
- Module format: ESM
- Entry point: `dist/index.js`
- Type declarations: `dist/index.d.ts`
- Development package manager: Bun
- Published package contents are controlled by `package.json#files`.

The package is for server-side Node/Bun applications. It depends on
`node-postgres` and is not intended for browsers.

## GitHub environment prerequisite

Before the first real publish, create GitHub environment `npm` in
`NikolayS/pgque`. Protect it as appropriate for releases (for example,
required reviewers and `main` branch restrictions). The workflow also checks
that it is running from `main`, but environment protection is the human approval
gate.

## Release process

The release workflow is `.github/workflows/release-typescript.yml`.

1. Update `clients/typescript/package.json` version and any release notes/changelog if present.
2. Merge the release prep PR.
3. Ensure the `npm` GitHub environment exists and is protected.
4. In npm, configure Trusted Publishing for:
   - package: `pgque`
   - repository: `NikolayS/pgque`
   - workflow: `release-typescript.yml`
   - environment: `npm`
5. Run **Release TypeScript client** with `dry_run=true` first. Dry runs only
   build, test, and run `npm publish --dry-run`; they do not require the
   protected `npm` environment or OIDC permissions.
6. Verify the packed file list and build output.
7. Run the workflow again with `dry_run=false`.

The workflow installs with `bun install --frozen-lockfile`, runs `bun run check`,
`bun run test`, builds `dist/`, ensures npm >= 11.5.1 for Trusted Publishing,
and publishes with npm provenance via OIDC. No long-lived npm token is needed.
