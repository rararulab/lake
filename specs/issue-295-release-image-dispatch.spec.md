spec: task
name: "release-image-dispatch"
inherits: project
tags: [release, ci, supply-chain]
---

Spec: `specs/issue-295-release-image-dispatch.spec.md`

## Intent

Lake's published GitHub Release is the immutable source that operators use to
obtain the multi-architecture container image and its manifest digest. Release
Please creates that Release with the repository `GITHUB_TOKEN`; GitHub
therefore does not emit a downstream `release` workflow run. The release is
currently published without its promised GHCR image.

Reproducer: merge a Release Please release PR. The Release Please log confirms
that it creates the root GitHub Release, but `gh run list --workflow
release-image.yml` contains no corresponding publication run and the release
tag has no image in GHCR.

This advances the `goal.md` production signal that a deployment can run the
stateless Query and bounded Metasrv tiers from a durable, trusted artifact. It
does not change Lake's data planes, metadata authority, or SQL protocol.

Issue #184 and PR #189 introduced the existing trusted, multi-architecture
image publisher and its manual backfill. This task preserves that publisher
and fixes the missing handoff from Release Please, which creates the Release
with `GITHUB_TOKEN` and therefore cannot activate the existing `release` event
trigger.

## Decisions

- Keep Release Please as the only creator of release PRs, tags, and GitHub
  Releases.
- When its root `release_created` output is true, dispatch the existing
  tag-validated `release-image.yml` through `workflow_dispatch` with the
  exact `tag_name` output.
- Keep image publication in its existing workflow so its immutable published
  release/tag/revision validation and least-privilege package permission stay
  intact.
- Give the Release Please workflow only the GitHub Actions permission needed
  to create the dispatch. Do not introduce a PAT or another long-lived
  credential.
- Extend the release-artifact contract test and document the automatic
  handoff plus the manual-backfill recovery path.

## Boundaries

### Allowed Changes
.github/workflows/release-please.yml
crates/lake-cli/tests/release_artifacts.rs
docs/guides/mise-ci.md
specs/issue-295-release-image-dispatch.spec.md

### Forbidden
.github/workflows/release-image.yml
release-please-config.json
release-please manifest files
Dockerfile
deploy/kubernetes/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-meta/**
Lake runtime protocol or storage behavior
PATs, static credentials, or image digests in repository files
mutable container tags

## Completion Criteria

Rule: release-created-image-publication — every root GitHub Release created by
  Release Please starts exactly one trusted image-publication workflow

Scenario: Root release dispatches the exact image tag
  Test:
    Package: lake-cli
    Filter: release_please_dispatches_image_publication_for_root_release
  Given Release Please reports a root release through `release_created` and
  `tag_name`
  When its workflow reaches the publication handoff
  Then it dispatches `release-image.yml` on `main` with that exact tag and
  `actions: write` is explicitly scoped for the dispatch

Scenario: Release creation failure skips image publication
  Test:
    Package: lake-cli
    Filter: release_please_dispatches_image_publication_for_root_release
  Given Release Please reports `release_created` as false after no release or
  a release-creation failure
  When its workflow reaches the publication handoff
  Then the image dispatch step is guarded and cannot create a publication run

Scenario: Release guide preserves dispatch and recovery instructions
  Test:
    Package: lake-cli
    Filter: release_please_dispatches_image_publication_for_root_release
  Given the repository release runbook
  When a maintainer needs automatic image publication or a one-time backfill
  Then it documents both the automatic dispatch and the manual image backfill

Scenario: Image workflow remains the trusted publisher
  Test:
    Package: lake-cli
    Filter: release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning
  Given a dispatched release tag
  When the image workflow starts
  Then it still validates the published immutable release/tag/revision before
  publishing multi-architecture image tags

## Out of Scope

- Retrofitting old releases other than the current v1.8.2 recovery dispatch.
- Changing the container build, runtime deployment, Kubernetes digest policy,
  or the Release Please versioning strategy.
