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

## Package shape

- Runtime: Node.js 20+
- Module format: ESM
- Entry point: `dist/index.js`
- Type declarations: `dist/index.d.ts`
- Development package manager: Bun
- Published package contents are controlled by `package.json#files`.

The package is for server-side Node/Bun applications. It depends on
`node-postgres` and is not intended for browsers.

## Release process

The release workflow is `.github/workflows/release-typescript.yml`.

1. Update `clients/typescript/package.json` version and changelog/release notes.
2. Merge the release prep PR.
3. In npm, configure Trusted Publishing for:
   - package: `pgque`
   - repository: `NikolayS/pgque`
   - workflow: `release-typescript.yml`
   - environment: `npm`
4. Run **Release TypeScript client** with `dry_run=true` first.
5. Verify the packed file list and build output.
6. Run the workflow again with `dry_run=false`.

The workflow installs with `bun install --frozen-lockfile`, runs `bun run check`,
builds `dist/`, ensures npm >= 11.5.1 for Trusted Publishing, and publishes
with npm provenance via OIDC. No long-lived npm token is needed.
