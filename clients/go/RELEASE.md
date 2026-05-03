# Go client release

Module path: `github.com/NikolayS/pgque/clients/go`.

Install:

```bash
go get github.com/NikolayS/pgque/clients/go@latest
```

## Versioning

The Go client version is independent from the SQL/server `pgque.version()`.
Bump this module when the Go API, behavior, or packaging changes; server-only
SQL changes do not require a Go client release.

## Tagging convention

This module lives in a subdirectory. Go requires tags scoped to the module root:

```bash
clients/go/v0.2.0
```

A plain repository tag like `v0.2.0` does **not** identify this submodule for
`go get`.

## Release process

The release workflow is `.github/workflows/release-go.yml`.

1. Update `clients/go` docs/code as needed and merge the release prep PR.
2. Run **Release Go client** with `version=vX.Y.Z` and `dry_run=true`.
3. If the dry run is clean, run it again with `dry_run=false`.
4. The workflow runs `go test ./...`, creates annotated tag
   `clients/go/vX.Y.Z`, pushes it, and optionally creates a GitHub Release.
5. pkg.go.dev indexes the module after the tag is visible through the Go proxy.

No package registry credentials are required.
