spec: task
name: "release-image-cache"
inherits: project
tags: [release, ci, supply-chain, performance]
---

Spec: `specs/issue-301-release-image-cache.spec.md`

## Intent

Each official release image is built on a fresh GitHub-hosted Buildx runner.
The Dockerfile's Cargo cache mounts improve work only within that one runner,
so the next patch release recompiles Lake's Rust dependency graph for amd64 and
arm64. Recent release backfills made this cold path visible as three long
multi-platform builds.

Lake must retain the current release authority: Release Please selects the
published immutable tag, release-image independently validates its tag and
target revision, and the final image remains a multi-platform GHCR manifest.
This task makes only the disposable BuildKit layers reusable between release
runs. It does not cache data, credentials, or a release decision.

## Decisions

- Use Docker Buildx's documented GitHub Actions cache backend through the
  existing pinned `docker/build-push-action` step; it already runs under the
  required non-default Buildx driver.
- Import and export the same dedicated `lake-release-image` cache scope.
- Export with `mode=max` so intermediate Rust compilation layers, not only the
  final runtime image, can be reused by a later release.
- Preserve the existing tag validation, pinned actions, amd64/arm64 platforms,
  GHCR tags, OCI labels, and digest summary.

## Boundaries

### Allowed Changes

.github/workflows/release-image.yml
crates/lake-cli/tests/release_artifacts.rs
specs/issue-301-release-image-cache.spec.md

### Forbidden

.github/workflows/release-please.yml
Dockerfile
release-please-config.json
release-please manifest files
deploy/kubernetes/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-meta/**
Lake runtime, SQL, storage, metadata, or Iceberg behavior
third-party actions, static credentials, cache secrets, mutable image tags, or
changes to release-source verification

## Completion Criteria

Rule: scoped-release-build-cache — subsequent official image releases reuse
  disposable BuildKit layers without sharing release authority or image tags

Scenario: Release image imports and exports one dedicated BuildKit cache
  Test:
    Package: lake-cli
    Filter: release_image_workflow_reuses_scoped_build_cache
  Given an official release-image workflow on a fresh GitHub-hosted runner
  When its pinned Buildx action builds the amd64 and arm64 image
  Then it imports `type=gha,scope=lake-release-image` and exports the same
  scope with `mode=max`

Scenario: Cached build preserves trusted release publication
  Test:
    Package: lake-cli
    Filter: release_image_workflow_is_tag_pinned_and_multiarch
  Given an official release-image workflow with reusable build layers
  When it publishes a released tag
  Then it still validates the immutable source and publishes only the
  configured amd64/arm64 tag and digest contract

## Out of Scope

- Retrying, cancelling, or changing in-flight historical release builds.
- Caching local developer builds or changing the Dockerfile's Cargo cache
  mounts.
- Replacing the GitHub-hosted builder, adding remote execution, or changing
  the release/image versioning flow.
