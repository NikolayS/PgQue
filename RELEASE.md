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

Review the release-prep diff. It must contain the stamped lifecycle source, the
two regenerated devel install files, all four stable artifacts, and the updated
manifest, plus the reviewed release notes described below. The stable install
and pg_tle body must not contain `0.3.0-devel`.
Open and merge this as a dedicated release-prep PR only after the validation
below and normal CI are green.

## Validate the release candidate

Use disposable databases. Run the supported PostgreSQL matrix for the plain
stable package, including the complete regression and acceptance suites:

```bash
createdb pgque_release_plain
psql -X -v ON_ERROR_STOP=1 --single-transaction \
  -d pgque_release_plain -f sql/pgque.sql
psql -X -v ON_ERROR_STOP=1 -v expected_pgque_version=0.3.0 \
  -d pgque_release_plain -f tests/run_all.sql
psql -X -v ON_ERROR_STOP=1 \
  -d pgque_release_plain -f tests/acceptance/run_acceptance.sql
psql -X -v ON_ERROR_STOP=1 -d pgque_release_plain -f sql/pgque_uninstall.sql
psql -X -At -d pgque_release_plain \
  -c "select count(*) from pg_namespace where nspname = 'pgque'"
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

The release tag keeps the final stamp. Immediately prepare a follow-up PR that
returns only the living devel tree to `0.3.0-devel` while leaving `sql/` frozen:

```bash
git switch -c chore/restore-0.3-devel
bash build/promote-release.sh --restore-devel 0.3.0
bash build/verify-release-artifacts.sh sql
git diff --exit-code -- sql
git diff --check
```

The expected diff is the lifecycle literal plus
`devel/sql/pgque.sql` and `devel/sql/pgque-tle.sql`. Merge that PR normally;
never regenerate or overwrite the stable directory during the restore step.
