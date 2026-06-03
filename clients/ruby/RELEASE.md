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
first release is therefore manual:

```bash
cd clients/ruby
gem build pgque.gemspec
gem signin                   # one-time, prompts for rubygems.org credentials
gem push pgque-0.3.0.rc.1.gem
```

After that, every subsequent release goes through the workflow below.

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
5. Run **Release Ruby client** with `dry_run=true` first. Dry runs
   only build, validate the version match, and smoke-install the
   resulting `.gem`; they do not require the `rubygems` environment
   approval or OIDC permissions.
6. Run it with `dry_run=false`. Approve the `rubygems` environment
   when prompted.
7. Verify the published artifact installs in a clean environment:

   ```bash
   gem install pgque --pre        # or pin: gem install pgque -v 0.3.0.rc.1
   ruby -rpgque -e 'puts Pgque::VERSION'
   ```

The workflow builds with `gem build`, smoke-installs the resulting
`.gem` against a temporary `GEM_HOME`, and publishes via RubyGems
Trusted Publishing / OIDC. No long-lived `RUBYGEMS_API_KEY` is
needed.

The publish step uses `rubygems/release-gem@v1`, which runs
`bundle exec rake release`. That task (provided by
`require "bundler/gem_tasks"` in `clients/ruby/Rakefile`) chains:

1. `rake build` — builds `pgque-${VERSION}.gem` under `pkg/`.
2. `release:guard_clean` — refuses to release if the working tree
   has uncommitted changes (CI checkouts are clean).
3. `release:source_control_push` — annotates the head commit with a
   `v${VERSION}` tag and pushes that tag to `origin`. The
   `contents: write` permission on the publish job, plus the
   `GITHUB_TOKEN` automatically injected by `actions/checkout`, is
   what authorizes the push. **The release workflow therefore
   pushes a git tag to `NikolayS/pgque` as a side effect.** If you
   need to retract a release, yank the gem on RubyGems *and* delete
   the tag with `git push --delete origin v${VERSION}`.
4. `release:rubygem_push` — `gem push pkg/pgque-${VERSION}.gem`.

If the gem push fails after the tag has already been pushed (rare
but possible if rubygems.org is degraded), you'll have a `v${VERSION}`
tag with no corresponding published gem. Re-running the workflow
will then fail at `release:guard_clean` if the tag already exists;
delete the tag and re-dispatch.

## Why no test registry?

Unlike PyPI's TestPyPI sibling, RubyGems.org has no public staging
instance. Dry-run validation in this workflow covers `gem build` and
local install verification; the next step is the real publish. If you
need an isolated end-to-end test for the publish path itself, push to
a privately-owned alias gem (e.g. `pgque-staging`) using the same
workflow with a different gemspec name, then drop the alias gem when
you're done.
