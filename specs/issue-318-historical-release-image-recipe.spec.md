spec: task
name: "historical-release-image-recipe"
inherits: project
tags: [release, ci, supply-chain, recovery]
---

## Intent

Historical image backfills must publish the immutable source selected by an
already-published release tag within the existing bounded multi-platform build
policy. A manual v1.8.4 backfill checked that tag out as its Docker context,
therefore used the tag's pre-cargo-chef Dockerfile and ran until the 180-minute
bound cancelled it without a manifest. The recovery path must make the current
cached recipe available without making the source, image tag, or release
authority mutable.

## Decisions

- Check out the workflow revision at `github.sha` into `build-recipe`, and the
  published release tag into `release-source`.
- Validate the tag, GitHub Release target SHA, checked-out source SHA, annotated
  tag SHA, and `version.txt` from `release-source`; this remains the release
  source of truth and supplies the Docker build context.
- Build `release-source` using `build-recipe/Dockerfile`. Release events run at
  the tag revision, while a documented manual dispatch from `main` intentionally
  supplies the current cargo-chef recipe for historical source recovery.
- Emit the immutable build-recipe SHA in a distinct OCI label. Keep
  `org.opencontainers.image.revision` bound only to the release-source SHA.
- Preserve the existing cache scope, timeout, platforms, image tags, digest
  summary, permissions, pinned actions, and non-cancelling concurrency.

## Boundaries

### Allowed Changes
- .github/workflows/release-image.yml
- mise.toml
- crates/lake-cli/tests/release_artifacts.rs
- docs/guides/mise-ci.md
- docs/plans/2026-07-20-historical-release-image-recipe.md
- specs/issue-318-historical-release-image-recipe.spec.md
- verification/issue-318-historical-release-image-recipe.md

### Forbidden
- Dockerfile
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

Scenario: Historical backfill builds immutable source with auditable current recipe
  Test:
    Package: lake-cli
    Filter: release_image_workflow_separates_source_and_recipe_for_backfills
  Given a manually dispatched backfill workflow on main for an already-published
  release tag whose historical Dockerfile predates cargo-chef
  When the workflow prepares its release build
  Then it validates and builds the immutable tag source, uses the workflow
  revision Dockerfile, and records the distinct recipe revision label

Scenario: Split checkout preserves trusted multi-architecture publication
  Test:
    Package: lake-cli
    Filter: release_image_workflow_is_tag_pinned_and_multiarch
  Given a release-image workflow with separately checked out source and recipe
  When it publishes an official release image
  Then the source tag and revision remain validated and the existing amd64,
  arm64, cache, image-tag, and digest contracts remain intact

Scenario: Historical recovery still rejects an untrusted release source
  Test:
    Package: lake-cli
    Filter: release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning
  Given a manually dispatched historical backfill with a malformed, unpublished,
  or mismatched release tag
  When the workflow validates its separately checked out source
  Then it refuses publication before Buildx can publish a digest

Scenario: Workflow contract follows the invoking Jujutsu workspace
  Test:
    Package: lake-cli
    Filter: release_artifact_contract_uses_invocation_workspace
  Given a cached release-artifact test executable from a different Jujutsu
  workspace
  When the contract runs from the candidate workspace
   Then it resolves workflow and documentation files from that invocation
   workspace rather than a compile-time checkout path

Scenario: Cargo target cache is isolated by Jujutsu workspace
  Test:
    Package: lake-cli
    Filter: mise_target_directory_is_workspace_isolated
  Given independent Jujutsu workspaces with a shared XDG cache root
  When either workspace invokes a lane-1 Cargo selector
  Then its target directory includes a stable hash of the active workspace
  and cannot reuse a test executable compiled by another checkout

## Out of Scope

- Changing the Dockerfile or normal Release Please authority.
- Retagging, deleting, or mutating an existing image manifest.
- Replacing GitHub-hosted Buildx, adding credentials, or changing runtime data
  paths.
