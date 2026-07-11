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
- `concurrent_delete_cannot_cross_recreate` pauses a resumed old deleter,
  completes delete/recreate plus new history/cursor, then proves exact deleting
  guards preserve the replacement incarnation.
- `maintenance_reclaims_external_manifest_history` runs real Lance cleanup on
  local storage, reclaims obsolete history, and preserves a tagged v1 record.
- Real LocalStack `external_manifest_cleanup_localstack`: PASS; tagged v1 keeps
  its S3 manifest, Dynamo record, and readable snapshot, while every removed
  Dynamo record is checked against exact S3 manifest absence.
- Spec lifecycle: 5/5 PASS.
- Strict clippy: PASS with `-D warnings`.
- `lake-engine-lance --lib`: 28/28 PASS.
- Post-review `mise run gate`: PASS (workspace tests, e2e, hooks, and site);
  88.62s after guarded delete-resume repair.

## Protocol

Lance applies retention first. Lake then reconciles one bounded page by exact
object existence; history deletion and cursor mutation are both guarded by the
current incarnation-bound latest bytes. No numeric cutoff is used to decide
external deletion.
