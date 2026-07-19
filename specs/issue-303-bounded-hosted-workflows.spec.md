spec: task
name: "issue-303-bounded-hosted-workflows"
inherits: project
tags: [ci, release, reliability]
---

## Intent

Every GitHub-hosted workflow that protects a production data path or publishes
a production image must fail within an explicit, repository-owned budget. A
stalled Iceberg interoperability fixture must fail after the same cold-run
margin as the existing LocalStack integration job. A stalled multi-architecture
release build must have a generous but finite ceiling instead of silently
falling back to GitHub Actions' six-hour default.

## Decisions

- This is configuration behavior with an executable Rust workflow-contract
  test, so it follows lane 1.
- The Apache Iceberg REST integration job has a 30-minute ceiling, matching
  the LocalStack integration job.
- The release-image job has a 180-minute ceiling. This accommodates a cold
  QEMU build while making a stuck release visible well before the platform
  default.
- Existing commands, runners, cache, permissions, triggers, tag validation,
  and concurrency semantics remain unchanged.

## Boundaries

### Allowed Changes

- .github/workflows/ci.yml
- .github/workflows/release-image.yml
- crates/lake-cli/tests/release_artifacts.rs
- docs/guides/mise-ci.md
- verification/issue-303-bounded-hosted-workflows.md

### Forbidden

- Application protocol or runtime behavior
- Workflow command, runner, cache, permission, release tag, or concurrency
  changes
- Checked-in credentials, image digests, or release version changes

## Completion Criteria

Scenario: Hosted workflows declare finite execution budgets
  Test:
    Package: lake-cli
    Filter: release_workflows_have_explicit_execution_budgets
  Level: static-contract
  Targets: .github/workflows/ci.yml, .github/workflows/release-image.yml
  Given the checked-in CI and release-image workflows
  When the workflow contract is loaded
  Then the Iceberg integration job has a 30-minute timeout and the release
  image job has a 180-minute timeout

Scenario: Existing execution semantics remain intact
  Test:
    Package: lake-cli
    Filter: release_image_workflow_is_tag_pinned_and_multiarch
  Level: static-contract
  Targets: .github/workflows/release-image.yml
  Given the two workflow timeout values are added
  When the workflow diff is reviewed
  Then its commands, runner class, permissions, triggers, tag validation, and
  multi-architecture publication settings are unchanged

Scenario: Release build cache remains configured
  Test:
    Package: lake-cli
    Filter: release_image_workflow_reuses_scoped_build_cache
  Level: static-contract
  Targets: .github/workflows/release-image.yml
  Given the release image workflow has an explicit execution budget
  When it constructs its Buildx invocation
  Then it still imports and exports the dedicated release-image cache scope

## Out of Scope

- Changing the actual integration-test or release-build duration
- Cancelling or retrying already-running historical release jobs
- Changing GitHub-hosted runner types or building on self-hosted infrastructure
