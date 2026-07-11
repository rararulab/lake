spec: task
name: "resumable-s3-managed-uploads"
inherits: project
tags: [objects, sdk, s3, multipart, resumable]
---

## Intent

Allow a multi-gigabyte SDK `FILE` upload from a local path to resume after a
process or network interruption without re-uploading completed S3 parts, while
preserving Lake's immutable-object and metadata-after-completion invariants.

## Decisions

- Resumability is opt-in through an SDK checkpoint directory; ordinary readers
  and deployments without a checkpoint directory keep the existing upload API.
- The object layer owns a versioned, atomically replaced checkpoint containing
  the random managed key, S3 upload id, source metadata, and completed-part
  ETag/checksum/SHA-256 records. Credentials and source bytes are never stored.
- A process holds an exclusive OS file lock for one checkpoint session. A crash
  releases the lock automatically; a concurrent resume fails fast.
- Resume reconciles the checkpoint with S3 `ListParts`, then rereads every
  completed local part to verify its per-part SHA-256 and rebuild the whole-file
  SHA-256 state. Any source or remote mismatch fails closed before uploading.
- Transient read/S3 errors preserve the multipart upload and checkpoint.
  Explicit cancellation aborts S3 and removes the checkpoint.
- An uncertain multipart completion checks the random destination object and
  streams it once for SHA-256 verification before publishing `DataLocation`.
- Query and Metasrv continue to receive only the completed `DataLocation` row.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-objects/**`
- `crates/lake-sdk/**`
- `README.md`
- `docs/architecture.md`
- `docs/design/managed-objects.md`
- `docs/plans/**`
- `specs/**`
- `verification/**`
- `**/.github/**`
- `**/AGENT.md`
- `**/CLAUDE.md`
- `**/docs/guides/mise-ci.md`
- `**/docs/guides/workflow.md`
- `**/mise.toml`

The final six patterns account for shared-checkout history from merged issue
12 that the repository-wide worktree verifier still reports. This workspace
does not edit those paths.

### Forbidden
- Sending object bytes through Query or Metasrv
- Publishing a `DataLocation` before S3 completion is verified
- Resuming when source identity, part digests, bucket, key, or upload id differ
- Serializing AWS credentials or bearer tokens into checkpoints
- Buffering more than one multipart part
- Silently abandoning an explicit user cancellation

## Completion Criteria

Scenario: Checkpoints round-trip without secrets and reject incompatible state
  Test:
    Package: lake-objects
    Filter: resumable_checkpoint_validates_source_and_stage
  Given a versioned upload checkpoint for one source and managed S3 stage
  When it is atomically loaded for a matching or changed source
  Then matching state round-trips, no credentials are serialized, and changed
  source/stage/part-size inputs return a typed incompatibility error

Scenario: Interrupted multipart upload resumes only missing parts
  Test:
    Package: lake-objects
    Filter: resumable_s3_upload_reuses_completed_parts_localstack
  Given a path upload interrupted after at least one completed S3 part
  When a new store instance resumes from the durable checkpoint
  Then S3 and local part records reconcile, completed parts are not uploaded
  again, and the final bytes, size, and SHA-256 match the source

Scenario: Resumed prefixes are verified before new bytes are sent
  Test:
    Package: lake-objects
    Filter: resumable_s3_upload_rejects_changed_source_localstack
  Given a checkpoint with a completed first part
  When the source bytes change without a compatible checkpoint identity
  Then resume returns a typed checkpoint mismatch and the existing upload is
  neither completed nor aliased to the changed source

Scenario: Explicit cancellation aborts remote and local resumable state
  Test:
    Package: lake-objects
    Filter: cancel_resumable_s3_upload_aborts_and_removes_checkpoint_localstack
  Given a resumable multipart upload with persisted completed parts
  When the caller explicitly cancels its checkpoint
  Then S3 has no multipart upload or completed object and the checkpoint is gone

Scenario: SDK path uploads select resumability without changing SQL visibility
  Test:
    Package: lake-sdk
    Filter: sdk_resumable_file_insert_uses_checkpoint_directory
  Given a client configured with an upload checkpoint directory and a path FILE
  When INSERT uploads and commits the object
  Then the path-aware resumable store method is used, the checkpoint is removed,
  and only the completed DataLocation batch is sent to Query

## Out of Scope

- Resuming arbitrary non-seekable readers
- Browser or mobile presigned uploads
- Tenant authorization and per-tenant checkpoint encryption
- Garbage collection for completed but unreferenced objects
- Cross-host sharing of a local checkpoint directory
