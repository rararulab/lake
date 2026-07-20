spec: task
name: "cargo-chef-path-dependencies"
inherits: project
tags: [release, container, cargo, regression]
---

## Intent

Historical release-image recovery must compile the immutable release source,
including a workspace which uses `third_party/datafusion-execution` as a Cargo
path patch. Run 29718401014 proved that the split source/recipe workflow can
reach the current Dockerfile, but its builder copied only `recipe.json` before
`cargo chef cook`; Cargo therefore failed to load the path crate before an
image could be published.

## Decisions

- Keep the two-checkout release model unchanged: the published tag remains the
  sole Docker build context and source authority, while the current workflow
  revision remains the auditable recipe authority for a manual backfill.
- Transfer exactly `third_party/datafusion-execution` from the planner into the
  builder after `recipe.json` and before `cargo chef cook`. This is the one
  current local patch input that Cargo needs at that point; it invalidates the
  dependency layer when the crate changes without copying all application
  sources ahead of the cache boundary.
- Preserve Cargo-chef, build cache scope, pinned images/actions, release tags,
  OCI labels, credentials, platforms, and runtime stages.
- Validate the result with a real native v1.8.4 build through Docker's `builder`
  stage, not just a planner-stage check.

## Boundaries

### Allowed Changes
- Dockerfile
- crates/lake-cli/tests/release_artifacts.rs
- docs/guides/mise-ci.md
- docs/plans/2026-07-20-cargo-chef-path-dependencies.md
- specs/issue-321-cargo-chef-path-dependencies.spec.md
- verification/issue-321-cargo-chef-path-dependencies.md

### Forbidden
- .github/workflows/release-image.yml
- .github/workflows/release-please.yml
- release-please-config.json
- .release-please-manifest.json
- version.txt
- Cargo.toml
- Cargo.lock
- deploy/kubernetes/**
- crates/lake-query/**
- crates/lake-metasrv/**
- crates/lake-meta/**
- static credentials, mutable image tags, or runtime / SQL / storage / metadata
  / Iceberg behavior

## Completion Criteria

Scenario: Cargo-chef receives the historical path dependency before cooking
  Test:
    Package: lake-cli
    Filter: release_image_hydrates_path_dependencies_before_cargo_chef_cook
  Level: static-contract
  Targets: Dockerfile
  Given a Docker build that derives a Cargo-chef recipe from a release source
  with the local datafusion-execution patch
  When the builder prepares its dependency layer
  Then it transfers that exact path crate after recipe.json and before cook,
  while application sources still arrive only after cooking

Scenario: Path hydration retains the dependency-cache boundary
  Test:
    Package: lake-cli
    Filter: release_image_caches_rust_dependencies_before_copying_application_sources
  Level: static-contract
  Targets: Dockerfile
  Given the repaired release Dockerfile
  When it prepares and cooks Rust dependencies
  Then cargo-chef remains pinned and application sources are not copied before
  the exportable cooked-dependency layer

Scenario: Missing path input is rejected before the release build
  Test:
    Package: lake-cli
    Filter: release_image_hydrates_path_dependencies_before_cargo_chef_cook
  Level: static-contract
  Targets: Dockerfile
  Given a Cargo-chef recipe names datafusion-execution as a local path patch
  When a Dockerfile omits its planner-to-builder transfer before cook
  Then the release artifact contract fails before a historical Buildx run can
  publish an image

## Out of Scope

- Changing source/recipe authority, release automation, image publication, or
  historical release tags.
- Adding a general path-dependency discovery mechanism or changing the Cargo
  workspace's existing patch declaration.
- SQL, metadata, storage, Iceberg, or deployment behavior.
