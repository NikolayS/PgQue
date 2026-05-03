# Go client release

Public module path: `github.com/NikolayS/pgque-go`.

Source of truth lives in this monorepo under `clients/go/`. Releases use a
hybrid mirror: the release workflow splits `clients/go` into a root-level tree
and pushes it to the read-only mirror repository `NikolayS/pgque-go`.

Install:

```bash
go get github.com/NikolayS/pgque-go@latest
```

## Versioning

The Go client version is independent from the SQL/server `pgque.version()`.
Bump this module when the Go API, behavior, or packaging changes; server-only
SQL changes do not require a Go client release.

## Tagging convention

The mirror repository uses normal Go module tags:

```bash
vX.Y.Z
```

Do not use monorepo subdirectory tags such as `clients/go/vX.Y.Z` for public
Go client releases.

## Mirror setup

Create `github.com/NikolayS/pgque-go` as the public mirror repository. It should
be treated as generated/read-only; changes happen in `clients/go` in this repo.

Configure repository secret `PGQUE_GO_MIRROR_TOKEN` in this repo. The token must
be able to push branches/tags and create GitHub Releases in `NikolayS/pgque-go`.
A fine-grained PAT scoped only to the mirror repository is preferred.

## Release process

The release workflow is `.github/workflows/release-go.yml`.

1. Update `clients/go` docs/code as needed and merge the release prep PR.
2. Run **Release Go client** from `main` with `version=vX.Y.Z` and `dry_run=true`.
3. If the dry run is clean, run it again with `dry_run=false`.
4. The workflow runs `go test ./...`, splits `clients/go`, pushes the mirror's
   `main`, creates annotated tag `vX.Y.Z` in the mirror, and optionally creates
   a GitHub Release in `NikolayS/pgque-go`.
5. pkg.go.dev indexes the module after the tag is visible through the Go proxy.
