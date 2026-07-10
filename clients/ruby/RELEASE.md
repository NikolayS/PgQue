# Ruby client release

Gem name: `pgque` on RubyGems.org.

```bash
gem install pgque --pre        # while v0.3.0 is in release-candidate
```

```ruby
require "pgque"
```

## Versioning

The Ruby client version is independent from the SQL/server
`pgque.version()`. Bump this gem when the Ruby API or packaging changes;
server-only SQL changes do not require a Ruby client release.

Use Ruby gem version strings in `clients/ruby/lib/pgque/version.rb`. For a
pre-release build, use dot-separated suffixes like `0.2.0.rc.1`,
`0.2.0.alpha.1`, or `0.2.0.beta`; do **not** use Git-style `0.2.0-dev`
hyphens, which `Gem::Version` parses but other tooling does not.
RubyGems treats any version containing a non-numeric segment as a
pre-release; users need `gem install pgque --pre` to receive it.

## Bootstrap (first publish only)

RubyGems' Trusted Publishing requires the gem to **already exist** on
the registry before a trusted publisher can be configured. The very
first release was therefore manual:

```bash
cd clients/ruby
gem build pgque.gemspec
gem signin                   # one-time, prompts for rubygems.org credentials
gem push pgque-0.3.0.rc.1.gem
```

That bootstrap publish is complete: `pgque 0.3.0.rc.1` already exists on
RubyGems. Published RubyGems versions are immutable, so the workflow must never
be dispatched with `0.3.0.rc.1`, even if its namespaced Git tag does not exist.
The next attempt must first bump `Pgque::VERSION` to a new version (for example,
`0.3.0.rc.2`). Every release after the bootstrap goes through the workflow
below.

## GitHub environment prerequisite

Before the first workflow-driven publish, create a GitHub environment
in `NikolayS/pgque`:

- `rubygems`

Protect it as appropriate for releases (for example, required reviewers
and `main` branch restrictions). The workflow also checks that it is
running from `main`, but environment protection is the human approval
gate.

## RubyGems Trusted Publisher prerequisite

After the bootstrap publish, configure Trusted Publishing on
rubygems.org:

1. Sign in to rubygems.org and open the gem's page.
2. **Settings → Trusted Publishers → Add Publisher**.
3. Provider: GitHub Actions.
4. Repository: `NikolayS/pgque`.
5. Workflow: `release-ruby.yml`.
6. Environment: `rubygems`.

Pin to a specific tag/branch only if you want to lock down which refs
can publish; otherwise leave the ref restriction empty.

## Release process

The release workflow is `.github/workflows/release-ruby.yml`.

1. Update `clients/ruby/lib/pgque/version.rb` and any release notes /
   changelog if present.
2. Merge the release prep PR.
3. Ensure the `rubygems` GitHub environment exists and is protected.
4. Ensure the gem already exists on RubyGems and Trusted Publishing
   is configured (bootstrap section above).
5. Run **Release Ruby client** with `dry_run=true` first. Dry runs validate the
   clean tree, version and namespaced tag, confirm the version is not already on
   RubyGems, install the current development SQL into a pinned PostgreSQL 18
   service, and run the full database-backed Ruby suite. Any skipped test fails
   the release check. They then build and inspect the `.gem`, smoke-install it,
   and confirm the tag is available. Dry runs do not create a tag, publish, or
   require the `rubygems` environment approval or OIDC permissions.
6. Run it with `dry_run=false`. Approve the `rubygems` environment
   when prompted.
7. Verify the published artifact installs in a clean environment:

   ```bash
   VERSION=0.3.0.rc.2             # replace with the version just published
   gem install pgque -v "$VERSION"
   ruby -rpgque -e 'puts Pgque::VERSION'
   ```

The workflow checks RubyGems version availability before uploading the artifact,
then builds with `gem build`, smoke-installs the resulting `.gem` against a
temporary `GEM_HOME`, and uploads that exact artifact to the publish job. The
publish job revalidates both the artifact and RubyGems availability immediately
before tagging, obtains short-lived credentials through RubyGems Trusted
Publishing / OIDC, creates the annotated tag `ruby/v${VERSION}` at the dispatch
SHA, pushes the tag, and publishes with `gem push`. No long-lived
`RUBYGEMS_API_KEY` is needed.

Ruby client tags are deliberately namespaced. Never use plain `v${VERSION}` for
a gem release: that namespace belongs to PgQue SQL/server releases, whose
version is independent from the Ruby client.

If `gem push` fails after the namespaced tag has been pushed, delete only that
Ruby tag, then re-dispatch:

```bash
git push origin --delete ruby/v0.3.0
```

If the workflow fails only while waiting for propagation, check RubyGems first:
the publish may already have succeeded, and retrying or yanking is unnecessary.

To retract a genuinely bad release, yank the gem and remove its namespaced tag:

```bash
gem yank pgque -v 0.3.0
git push origin --delete ruby/v0.3.0
```

Do not delete the SQL/server `v0.3.0` tag when retracting a Ruby client release.

## Why no test registry?

Unlike PyPI's TestPyPI sibling, RubyGems.org has no public staging
instance. Dry-run validation in this workflow covers `gem build` and
local install verification; the next step is the real publish. If you
need an isolated end-to-end test for the publish path itself, push to
a privately-owned alias gem (e.g. `pgque-staging`) using the same
workflow with a different gemspec name, then drop the alias gem when
you're done.
