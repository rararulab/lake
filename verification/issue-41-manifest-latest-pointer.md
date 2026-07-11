# Verification: O(1) external manifest latest pointer

Issue: #41

## Candidate

- Base: `2f0e95204d26609578d167816b057a0b0e714c80`
- Workspace: `.worktrees/issue-41-manifest-latest-pointer`
- Allowed-change paths: 9

## Evidence

- `latest_pointer_avoids_history_scan_after_publication` proves two latest
  resolutions add exactly two `MetaStore::get` calls and zero history scans.
- `concurrent_manifest_claims_never_regress_latest` races two v2 staging
  claims: exactly one wins, latest is v2, and v1 is archived exactly.
- `legacy_history_installs_latest_pointer_once` observes one migration scan,
  then point-only latest reads.
- `legacy_staging_finalize_can_advance` proves a duplicated legacy staging
  record and fixed pointer converge to final before v2 can advance.
- `delete_fence_blocks_stale_migration_and_recreate` deterministically pauses
  migration and deletion to prove the durable deletion marker defeats ABA and
  excludes recreate until cleanup completes.
- `concurrent_finalize_converges_on_same_path` covers current, historical, and
  migrated duplicate staging records with a forced CAS barrier; same-target
  finalizers both succeed while different targets conflict.
- `history_create_is_guarded_by_exact_latest` pauses archive and backfill after
  reading latest, completes drop plus same-URI recreate/finalize, then proves
  their old-incarnation guarded transactions fail without publishing history.
- `stale_recreate_cannot_cross_incarnations` pauses recreate after reading a
  deleted marker, completes another recreate/delete cycle, then proves UUIDv7
  incarnation bytes prevent the stale CAS from matching.
- Exact current/historical reads, historical backfill, delete, and recreate
  scenarios pass.
- `cargo test -p lake-engine-lance --lib`: 22/22 PASS.
- Real LocalStack S3 + Dynamo test
  `lance_engine_on_s3_with_dynamo_external_manifest`: PASS; Dynamo contains a
  fixed v2 latest record plus immutable v1 history; drop clears live state and
  history while retaining only the durable `deleted` fence.
- `cargo clippy -p lake-engine-lance --all-targets -- -D warnings`: PASS.
- `mise run spec-lifecycle specs/issue-41-manifest-latest-pointer.spec.md`:
  10/10 PASS with every selector matching a real test.
- Final post-review `mise run gate`: PASS (format, clippy, workspace tests,
  e2e self-check, and site checks); 89.98s with incarnation-bound pointers.

## Rollout

This changes the writer protocol. Pre-#41 committers must be drained before
upgrading every metadata node that may become leader; writes resume only after
the group is version-homogeneous. Stored datasets require no offline rewrite
and migrate lazily on first latest lookup.
