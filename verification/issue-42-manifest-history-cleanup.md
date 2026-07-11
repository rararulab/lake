# Verification: bounded external manifest history cleanup

Issue: #42

## Candidate

- Base: `d785ba91e339edec827e13c733b6a0983b13ba97`
- Workspace: `.worktrees/issue-42-manifest-history-cleanup`
- Allowed-change paths: 8

## Evidence

- `removed_manifest_history_is_reclaimed_boundedly` proves each call visits no
  more than its page/HEAD budget and repeated calls reclaim all absent records.
- `retained_manifest_history_survives_cleanup` proves physically present
  manifest paths and fixed latest remain readable.
- `cleanup_cursor_resumes_without_touching_latest` proves durable continuation
  and byte-for-byte latest preservation.
- `maintenance_reclaims_external_manifest_history` runs real Lance cleanup on
  local storage, reclaims obsolete history, and preserves a tagged v1 record.
- Real LocalStack `external_manifest_cleanup_localstack`: PASS; S3 manifest
  absence is checked for every Dynamo history record removed by maintenance.
- Spec lifecycle: 4/4 PASS.
- Strict clippy: PASS with `-D warnings`.
- `mise run gate`: PASS (workspace tests, e2e, hooks, and site); 270.21s
  from the issue workspace's cold all-target build.

## Protocol

Lance applies retention first. Lake then reconciles one bounded page by exact
object existence; history deletion and cursor mutation are both guarded by the
current incarnation-bound latest bytes. No numeric cutoff is used to decide
external deletion.
