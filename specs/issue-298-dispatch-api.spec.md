spec: task
name: "release-image-dispatch-api"
inherits: project
tags: [release, ci, supply-chain]
---

Spec: `specs/issue-298-dispatch-api.spec.md`

## Intent

Release Please created the published v1.8.3 release, then its image-dispatch
step failed before GitHub Actions received a request. The runner intentionally
does not check out the repository, while `gh workflow run` still tries to find
a local Git repository even when given `--ref main`.

Reproducer: Release Please run `29703682151`, job `88236941576`, records
`RELEASE_TAG=v1.8.3` and fails with `fatal: not a git repository` before an
image-workflow run exists.

This is a follow-up to issue #295. It keeps the release control-plane
boundary: Release Please supplies a root release tag, and the existing image
workflow independently validates the published immutable tag and revision
before publishing a multi-architecture image. Lake data, metadata, SQL, and
Iceberg behavior are unchanged.

## Decisions

- Invoke GitHub's repository-qualified workflow-dispatch REST endpoint rather
  than the Git-aware `gh workflow run` command.
- Keep the `actions: write` workflow permission, the short-lived
  `github.token`, `ref=main`, and the exact Release Please `tag_name` input.
- Keep the image publisher and its immutable release/tag/revision validation
  unchanged; no checkout, PAT, static credential, or mutable tag is added.
- Extend the release-artifact contract test so a local-Git-dependent dispatch
  cannot regress back into the workflow.

## Boundaries

### Allowed Changes
.github/workflows/release-please.yml
crates/lake-cli/tests/release_artifacts.rs
specs/issue-298-dispatch-api.spec.md

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
checkout steps, PATs, static credentials, image digests, or mutable tags

## Completion Criteria

Rule: checkout-independent-image-dispatch — a root release dispatches its
  trusted image publisher without requiring a local Git repository

Scenario: Release dispatch uses an explicit repository API endpoint
  Test:
    Package: lake-cli
    Filter: release_please_dispatches_image_publication_for_root_release
  Given Release Please created a root release and the runner has no checkout
  When its image handoff runs
  Then it POSTs the repository-qualified workflow dispatch endpoint with
  `ref=main` and the exact `tag_name` input

Scenario: Release dispatch remains least-privilege and guarded
  Test:
    Package: lake-cli
    Filter: release_please_dispatches_image_publication_for_root_release
  Given a non-release or failed release-creation result
  When the Release Please workflow reaches the handoff
  Then the guard prevents dispatch and the only added permission is
  `actions: write` for its short-lived workflow token

Scenario: Published image verification remains downstream
  Test:
    Package: lake-cli
    Filter: release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning
  Given a dispatched release tag
  When the image workflow starts
  Then it still validates the immutable published release/tag/revision before
  publishing multi-architecture image tags

## Out of Scope

- Retrying the failed historical Release Please run.
- Changing Docker image construction, deployment digest policy, or the
  Release Please versioning strategy.
