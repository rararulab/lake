spec: task
name: "release-please-recovery-schedule"
inherits: project
tags: []
---

## Intent

GitHub may transiently fail the Release Please action after a `main` push. A
failed invocation must not require a maintainer to notice it and manually
replay the workflow before the repository can form its next release PR. Lake
needs a bounded, automatic reconciliation path that invokes the existing
Release Please authority, not a second release writer.

## Decisions

- Add an hourly, off-the-hour `schedule` trigger to the existing
  `.github/workflows/release-please.yml` workflow.
- The scheduled run uses the default branch (`main`) and the existing
  non-cancelling `release-please-${{ github.ref }}` concurrency group, so it
  calls the same pinned action, configuration, manifest, and permission set as
  a normal push.
- A scheduled no-change execution is the upstream action's idempotent no-op;
  it may reconcile a prior transient failure but must not create a second
  version, tag, release, or image-publication implementation.
- Preserve `workflow_dispatch` as the immediate operator recovery path.

## Boundaries

### Allowed Changes
.github/workflows/release-please.yml
crates/lake-cli/tests/release_artifacts.rs
docs/guides/mise-ci.md
specs/issue-313-release-please-recovery-schedule.spec.md
verification/issue-313-release-please-recovery-schedule.md

### Forbidden
release-please-config.json
.release-please-manifest.json
version.txt
.github/workflows/release-image.yml
Rust production code
new GitHub Actions dependencies or credentials

## Completion Criteria

Rule: release-please-recovers-transient-platform-failures — one existing,
  idempotent release authority has both automatic and operator-triggered
  recovery paths

Scenario: Release Please has recurring and immediate recovery triggers
  Test:
    Package: lake-cli
    Filter: release_please_has_automatic_recovery_triggers
  Given GitHub API or Actions transiently fails the push-triggered invocation
  When the normal push event is not retried by the platform
  Then an hourly off-the-hour trigger re-invokes the existing workflow and
  `workflow_dispatch` remains available for immediate operator recovery

## Out of Scope

- Retrying the release-image build, reconciling an image publication that failed
  after a release was created, changing release version selection, or adding a
  third-party retry action.
