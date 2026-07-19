# #223 Release lockfile coverage

## Defect

`main@v1.3.1` had a workspace package version of `1.3.1`, but the
`lake-iceberg` package entry in `Cargo.lock` remained `1.3.0`. The matching
Cargo.lock selector was absent from `release-please-config.json`, so the
next automated release would repeat the mismatch.

## Fix

- Added the missing `lake-iceberg` release-please Cargo.lock selector.
- Regenerated the workspace lockfile entry at `1.3.1`.
- Added `release_please_covers_every_workspace_lockfile_package`, which
  checks every locked `lake-*` package for both version equality and exactly
  one Cargo.lock selector.

## Verification

- `mise run spec-lint specs/issue-223-release-lockfile-coverage.spec.md`
  passed with a 100% quality score.
- The new test failed before the selector was added: `lake-iceberg must have
  exactly one Cargo.lock release-please selector` (found 0).
- `cargo +nightly fmt --check` passed.
- `cargo test -p lake-cli --test release_artifacts
  release_please_covers_every_workspace_lockfile_package -- --exact` passed.
- `cargo check --workspace --locked` passed.
- `mise run gate` passed (workspace tests, selftest, ADBC, and site checks).
