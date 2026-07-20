spec: task
name: "shared-cargo-target"
inherits: project
tags: [tooling, cargo, jj, developer-experience]
---

## Intent

Local Jujutsu workspaces must reuse one repository Cargo artifact cache. The
workspace-hash cache path introduced during historical release recovery created
a full dependency tree per checkout; six such paths accumulated about 179 GB
on one development machine. The cache policy must preserve source-safe
release-artifact tests without multiplying local build storage.

## Decisions

- Set the local mise `CARGO_TARGET_DIR` to one XDG-cache directory:
  `lake/target`, not a physical-workspace hash subdirectory.
- Rely on Cargo fingerprints to select compatible artifacts and Cargo's
  target-directory lock to serialize concurrent writers. Tests that inspect
  repository files continue to resolve them from the invocation workspace,
  rather than a compile-time source path.
- The former workspace-isolation assertion is superseded by this task and is
  removed from the historical-release spec; the historical source/recipe
  contract remains unchanged.
- Workspace-hash directories are untracked local cache only. They may be
  deleted after Cargo processes have stopped; no tracked source or user data is
  part of that cleanup.

## Boundaries

### Allowed Changes
- mise.toml
- docs/guides/mise-ci.md
- crates/lake-cli/tests/release_artifacts.rs
- specs/issue-318-historical-release-image-recipe.spec.md
- specs/issue-326-shared-cargo-target.spec.md

### Forbidden
- Cargo.toml
- Cargo.lock
- Rust runtime behavior
- release workflows or Dockerfiles
- SQL, metadata, storage, SDK, or Iceberg behavior
- destructive cleanup outside the XDG workspace-hash target directories

## Completion Criteria

Scenario: Cargo artifacts are shared across Jujutsu workspaces
  Test:
    Package: lake-cli
    Filter: mise_target_directory_is_shared_across_jj_workspaces
  Level: static-contract
  Targets: mise.toml
  Given isolated Jujutsu workspaces that use the same user XDG cache root
  When a local mise task resolves its Cargo target directory
  Then every workspace uses lake/target without a physical-workspace hash,
  allowing Cargo to reuse compatible dependency artifacts

Scenario: release artifact tests resolve the invoking workspace
  Test:
    Package: lake-cli
    Filter: release_artifact_contract_uses_invocation_workspace
  Level: static-contract
  Targets: crates/lake-cli/tests/release_artifacts.rs
  Given a cached release-artifact executable built from another Jujutsu
  workspace
  When the contract is invoked from the candidate workspace
  Then it resolves repository files from that invocation workspace rather than
  a compile-time checkout path

Scenario: workspace-hash target configuration is rejected
  Test:
    Package: lake-cli
    Filter: mise_target_directory_is_shared_across_jj_workspaces
  Level: static-contract
  Targets: mise.toml
  Given a local mise configuration that appends a physical-workspace hash below
  lake/target
  When the shared-target contract is evaluated
  Then it fails before a Cargo task can retain another independent build tree

## Out of Scope

- Recreating deleted cache directories or running a cold full workspace gate.
- Changing Cargo's own fingerprinting or lock behavior.
- Changing historical-release source/recipe authority or image publication.
