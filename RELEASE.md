# PgQue server release

This checklist releases the SQL/server package. Client libraries have
independent versions and release instructions under `clients/*/RELEASE.md`.

The stable package consists of exactly four files under `sql/`:

- `pgque.sql`
- `pgque-tle.sql`
- `pgque_uninstall.sql`
- `pgque-tle-uninstall.sql`

`sql/release-manifest.txt` records their release version and SHA-256 hashes.
The manifest verifier also checks that the plain and pg_tle packages advertise
the same final version and that the pg_tle wrapper embeds the exact plain SQL.

## Prepare the release PR

Start from a clean branch containing the complete release candidate, including
the tested pg_tle update path from the immutable `v0.2.0` fixture. Fetch tags
and initialise the PgQ submodule before promoting:

```bash
git fetch origin --tags
git submodule update --init --recursive
bash build/promote-release.sh 0.3.0
bash build/verify-release-artifacts.sh sql
bash tests/test_release_promotion.sh
git diff --check
```

The promotion command accepts only final SemVer such as `0.3.0`. It stamps the
authoritative `pgque.version()` literal in
`devel/sql/pgque-additions/lifecycle.sql`, runs the generator twice and rejects
nondeterministic output, then prepares and validates a complete release
directory before replacing `sql/`. A failure or interrupt restores the original
release files.

Promote the user-facing documentation in the same release-prep PR. For this
release, write the concrete stable channel first:

```bash
printf '%s\n' 'stable:v0.3.0' > docs/.release-channel
```

Then make the documentation match that channel everywhere covered by the docs
contract:

- switch install, upgrade, uninstall, and website examples from `devel/sql/`
  to the released files under `sql/`;
- replace SQL source links in `docs/reference.md` with links to released files
  at the immutable tag, under
  `https://github.com/NikolayS/pgque/blob/v0.3.0/sql/`;
- remove or replace the `main` / development-channel banners in `README.md`,
  `docs/README.md`, `docs/installation.md`, `docs/reference.md`,
  `docs/tutorial.md`, and the website landing page so the tagged copies read as
  stable `v0.3.0` documentation.

For a future release, substitute its final version and tag consistently in the
channel file, source links, and prose. Validate the promoted documentation and
the production website before reviewing the release diff:

```bash
bash build/check-docs-contract.sh
cd web && bun run build
cd ..
git diff --check
```

Review the release-prep diff. It must contain the stamped lifecycle source, the
two regenerated devel install files, all four stable artifacts, and the updated
manifest, plus the reviewed release notes described below. It must also contain
the stable `docs/.release-channel` value and every user-facing documentation,
source-link, and website change required by `build/check-docs-contract.sh`.
Treat those documentation changes as required release artifacts, not a
follow-up. The stable install and pg_tle body must not contain `0.3.0-devel`.
Open and merge this as a dedicated release-prep PR only after the validation
below and normal CI are green.

## Validate the release candidate

Use disposable databases. Run the supported PostgreSQL matrix for the plain
stable package, including the complete regression and acceptance suites:

```bash
createdb pgque_release_plain
PAGER=cat psql \
  --no-psqlrc \
  --set=ON_ERROR_STOP=1 \
  --single-transaction \
  --dbname=pgque_release_plain \
  --file=sql/pgque.sql
PAGER=cat psql \
  --no-psqlrc \
  --set=ON_ERROR_STOP=1 \
  --set=expected_pgque_version=0.3.0 \
  --dbname=pgque_release_plain \
  --file=tests/run_all.sql
PAGER=cat psql \
  --no-psqlrc \
  --set=ON_ERROR_STOP=1 \
  --dbname=pgque_release_plain \
  --file=tests/acceptance/run_acceptance.sql
PAGER=cat psql \
  --no-psqlrc \
  --set=ON_ERROR_STOP=1 \
  --dbname=pgque_release_plain \
  --file=sql/pgque_uninstall.sql
PAGER=cat psql \
  --no-psqlrc \
  --tuples-only \
  --no-align \
  --dbname=pgque_release_plain \
  --command="select count(*) from pg_namespace where nspname = 'pgque'"
```

The final query must return `0`. Also test an in-place plain upgrade in a fresh
database: install the tagged `v0.2.0:sql/pgque.sql`, create a queue,
subscription, and pending event, apply the promoted `sql/pgque.sql` in one
transaction, then run `tests/run_all.sql` and confirm the fixture remains.

Validate pg_tle against the real pinned pg_tle image used by CI. Cover both
paths:

1. Fresh install: load `sql/pgque-tle.sql`, create extension `pgque`, run
   `tests/run_all.sql`, and run `sql/pgque-tle-uninstall.sql` twice. Confirm
   both the `pgque` extension/schema and the pg_tle registration are absent.
2. Update: register and install the immutable
   `v0.2.0:sql/pgque-tle.sql` fixture, then run
   `tests/test_tle_upgrade.sql`. This exercises the data-preserving
   `0.2.0 -> 0.3.0` update and verifies queue, subscription, pending, retry,
   and dead-letter state.

The pg_tle CI job contains the exact container setup and tagged-fixture
commands. Do not replace the tagged fixture with the mutable stable directory.
Finally, rerun both stable uninstall paths explicitly; successful installation
alone does not validate that their manifest entries are current.

## Tag and publish

The release-prep PR must include a reviewed `release-notes-0.3.0.md` covering
breaking or behavior changes, upgrade instructions, the pg_tle update path,
and any known limitations. Do not rely on automatically generated notes for a
release with migration-sensitive changes.

After the release-prep PR is merged and its `main` checks pass, tag that exact
commit and create the GitHub release from the reviewed notes:

```bash
git switch main
git pull --ff-only origin main
bash build/verify-release-artifacts.sh sql
test -s release-notes-0.3.0.md
git tag -s v0.3.0 -m 'PgQue 0.3.0'
git push origin v0.3.0
gh release create v0.3.0 --verify-tag --title 'PgQue 0.3.0' \
  --notes-file release-notes-0.3.0.md \
  sql/pgque.sql \
  sql/pgque-tle.sql \
  sql/pgque_uninstall.sql \
  sql/pgque-tle-uninstall.sql \
  sql/release-manifest.txt
```

Verify the release attachments against `release-manifest.txt` after download.

## Return main to development

The release tag keeps the final stamp and stable documentation. Immediately
prepare a follow-up PR that returns the living devel tree and `main`
documentation to development while leaving `sql/` frozen:

```bash
git switch -c chore/restore-0.3-devel
bash build/promote-release.sh --restore-devel 0.3.0
printf '%s\n' 'development' > docs/.release-channel
```

Restore every development-channel notice removed during promotion, switch the
user-facing install and uninstall examples back to `devel/sql/`, and point
`docs/reference.md` SQL source links back to
`https://github.com/NikolayS/pgque/blob/main/devel/sql/`. The README,
documentation index, installation guide, reference, tutorial, and website
landing page must again identify themselves as documentation for the living
`main` development build.

Validate the complete return-to-development diff:

```bash
bash build/check-docs-contract.sh
cd web && bun run build
cd ..
bash build/verify-release-artifacts.sh sql
git diff --exit-code -- sql
git diff --check
```

The expected diff is the lifecycle literal plus
`devel/sql/pgque.sql`, `devel/sql/pgque-tle.sql`, `docs/.release-channel`, and
the development documentation/website files required by the docs contract.
Merge that PR normally; never regenerate or overwrite the stable directory
during the restore step. The existing release tag remains immutable and must
continue to expose the stable channel, stable paths, and tag-pinned source
links; do not move the tag or apply the development reset to its contents.
