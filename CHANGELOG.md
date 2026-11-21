# Changelog

## Unreleased

### Ruby client compatibility

`Pgque::Event` accepts only `payload:` and `type:`. Remove `extra:` arguments
and calls to `Event#extra` when upgrading from an earlier prerelease. The client
never sent that field to PostgreSQL. Unknown keywords now raise `ArgumentError`
so unsupported producer data is not silently discarded.
