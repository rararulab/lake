spec: task
name: "release-image-artifact"
inherits: project
tags: [release, container, ghcr, deployment, operations]
---

## Intent

Every published Lake release must produce a deployable, official multi-platform
container image. The Kubernetes reference intentionally requires operator-owned
immutable digest pinning; it needs a trustworthy release artifact to pin.

## Decisions

- A published GitHub release triggers the image workflow. A guarded manual
  dispatch accepts an explicit release tag so an already-published release can
  be backfilled.
- The workflow checks out the requested tag and rejects it unless it is the
  checked-out commit's exact tag and matches `version.txt`.
- The image is pushed to GHCR for `linux/amd64` and `linux/arm64`, with both
  `vX.Y.Z` and `X.Y.Z` immutable release tags plus OCI source, revision, and
  version labels. It publishes the resulting manifest digest in the run
  summary.
- The workflow has only repository-read and package-write permissions. It uses
  the ephemeral GitHub Actions token and stores no credentials in the tree.
- The Kubernetes guide continues to require operator-owned digest pinning. No
  `latest` tag or mutable release tag is introduced into production YAML.

## Boundaries

### Allowed Changes
.github/workflows/release-image.yml
crates/lake-cli/tests/release_artifacts.rs
docs/guides/kubernetes.md
specs/issue-184-release-image.spec.md

### Forbidden
application protocol or runtime source changes
credentials, secrets, or image digests committed to source
mutable tags in deploy/kubernetes/lake.yaml
release version changes

## Completion Criteria

Scenario: Release images are tagged, multi-platform, and traceable to the release source
  Test:
    Package: lake-cli
    Filter: release_image_workflow_is_tag_pinned_and_multiarch
  Given the checked-in release-image workflow
  When its triggers, permissions, tag validation, build inputs, and output
  contract are inspected
  Then a published or explicitly selected release tag can only publish an
  `amd64` and `arm64` GHCR manifest with release-only tags, provenance labels,
  and a surfaced immutable digest

Scenario: Invalid release sources fail closed while deployment policy stays digest-pinned
  Test:
    Package: lake-cli
    Filter: release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning
  Given the checked-in release-image workflow and Kubernetes reference
  When the release validation and deployment image references are inspected
  Then a tag that is not exactly checked out or does not match `version.txt`
  fails before publication, and the guide rejects mutable deployment references

## Out of Scope

- Backfilling the already-published v1.0.0 image in this code change. That is
  an authenticated operational run after the workflow lands on `main`.
- Registry retention policy, SBOM/provenance attestation, signing, or a Helm
  chart.
