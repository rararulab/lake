# Verification: O(1) external manifest latest pointer

Issue: #41

## Candidate

- Base: `2f0e95204d26609578d167816b057a0b0e714c80`
- Workspace: `.worktrees/issue-41-manifest-latest-pointer`
- Allowed-change paths: 7

## Evidence

- `latest_pointer_avoids_history_scan_after_publication` proves two latest
  resolutions add exactly two `MetaStore::get` calls and zero history scans.
- `concurrent_manifest_claims_never_regress_latest` races two v2 staging
  claims: exactly one wins, latest is v2, and v1 is archived exactly.
- `legacy_history_installs_latest_pointer_once` observes one migration scan,
  then point-only latest reads.
- `legacy_staging_finalize_can_advance` proves a duplicated legacy staging
  record and fixed pointer converge to final before v2 can advance.
- Exact current/historical reads, historical backfill, delete, and recreate
  scenarios pass.
- `cargo test -p lake-engine-lance --lib`: 18/18 PASS.
- Real LocalStack S3 + Dynamo test
  `lance_engine_on_s3_with_dynamo_external_manifest`: PASS; Dynamo contains a
  fixed v2 latest record plus immutable v1 history, and drop clears both.
- `cargo clippy -p lake-engine-lance --all-targets -- -D warnings`: PASS.
- `mise run spec-lifecycle specs/issue-41-manifest-latest-pointer.spec.md`:
  6/6 PASS with every selector matching a real test.
- `mise run gate`: PASS (format, clippy, workspace tests, e2e self-check,
  and site checks); 245.41s from a cold issue-workspace build.

## Rollout

This changes the writer protocol. Pre-#41 committers must be drained before
upgrading every metadata node that may become leader; writes resume only after
the group is version-homogeneous. Stored datasets require no offline rewrite
and migrate lazily on first latest lookup.
