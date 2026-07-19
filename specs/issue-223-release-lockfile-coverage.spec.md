spec: task
name: "release-lockfile-coverage"
inherits: project
tags: [release, reliability]
---

## Intent

Every released Lake source tree must build with Cargo's locked resolver. The
release-please configuration must therefore update every workspace `lake-*`
package recorded in `Cargo.lock` whenever it updates the workspace version.

`v1.3.1` violates that contract: its `lake-iceberg` lockfile entry is `1.3.0`
while the workspace package version is `1.3.1`.

## Decisions

- Keep release-please as the sole release versioning authority.
- Add the missing `lake-iceberg` Cargo.lock selector beside the existing
  workspace package selectors.
- Guard the complete set, rather than only this package: a test reads the
  lockfile and requires every locked `lake-*` package to have the workspace
  version and a release-please selector.

## Boundaries

### Allowed Changes

- `release-please-config.json`
- `Cargo.lock`
- `crates/lake-cli/tests/release_artifacts.rs`
- this task contract and its verification record

### Forbidden

- Query, metadata, storage, or SDK runtime behavior changes
- A second version source or replacement release workflow

## Completion Criteria

Scenario: Release automation covers each workspace lockfile package
  Test:
    Package: lake-cli
    Filter: release_please_covers_every_workspace_lockfile_package
  Given the checked-in workspace manifest, lockfile, and release-please config
  When the release artifact checks inspect every locked `lake-*` package
  Then each package has the workspace version and exactly one Cargo.lock
  release-please selector

## Out of Scope

- Changing release version policy or generating a release outside the normal
  release-please flow
