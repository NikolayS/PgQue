# PgQue 0.3 RC2: local build and qualification

This checkout prepares coordinated RC2 artifacts. The commands below build
local artifacts. They do not publish packages, create tags, or change a registry.

## Candidate identities

- SQL and TypeScript: `0.3.0-rc.2`.
- Python: `0.3.0rc2` (PEP 440 spelling).
- Ruby: `0.3.0.rc.2` (RubyGems spelling).
- Go: proposed mirror tag `v0.3.0-rc.2`; no source version constant or local tag.

The client versions are independent of the server version. This coordinated
candidate uses the same RC number to identify the new paging adapters clearly.
Previously published Ruby RC1 does not identify the new local paging code.

## SQL candidate

Generate from the canonical sources:

```sh
bash build/transform.sh
psql -X -v ON_ERROR_STOP=1 -f devel/sql/pgque.sql
```

Use a disposable database for qualification. The candidate SQL lives in
`devel/sql/`. The frozen `sql/` installers still identify the stable release;
they are not replaced by these build commands.

For the pg_tle path, register `devel/sql/pgque-tle.sql`. Fresh installations then
use `create extension pgque`. Existing released 0.2.1 or 0.2.2 installations
require an explicit `alter extension pgque update to '0.3.0-rc.2'`. Registration
alone does not update an installed extension or migrate queue state.

## Local SDK artifacts

Run each command from its client directory:

- Python: `python -m build`; check wheel/sdist metadata with `twine check`.
- TypeScript: `bun install --frozen-lockfile`, `bun run check`, `bun run test`,
  `bun run build`, then `npm pack`.
- Ruby: `gem build pgque.gemspec`.
- Go: run module tests from `clients/go`; a later mirror release uses its own tag.

Install each local artifact into a clean consumer tree. Check the package and
runtime versions, then use a database with the matching candidate SQL to test
ordinary, cooperative, and partitioned paging. Stable registry packages and
frozen SQL do not establish these candidate API checks.

## Handoff limits

Record exact artifact hashes, the local source commit, installed SQL version,
and test evidence. Keep local checks separate from remote CI, registry
publication, tags, and release approval. A short concurrent workload is not
production endurance. External effects still require application idempotency.
