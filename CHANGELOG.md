# Changelog

## Unreleased

### Go client

- The minimum Go version is 1.25, up from 1.21. Upgrade older toolchains
  before updating the client. Use the latest patch of a supported Go release.
- Update pgx to 5.9.2 and `golang.org/x/text` to 0.39.0 to fix dependency
  security advisories. Remove the unused `golang.org/x/crypto` dependency.
