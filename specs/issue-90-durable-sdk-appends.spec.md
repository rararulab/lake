spec: task
name: "durable-sdk-append-recovery"
inherits: project
tags: [sdk, file, append, idempotency, recovery, checkpoint]
---

## Intent

Preserve the exact SQL `FILE` append identity across an SDK process restart.
Object uploads already have restart checkpoints and server-side append replay is
idempotent, but the SDK currently keeps the operation ID and encoded Arrow
payload only inside an in-memory `PendingAppend`. A crash after multi-gigabyte
uploads and before a conclusive commit response can therefore turn an ordinary
application retry into a second logical append.

## Decisions

- The existing operator-owned upload checkpoint directory also owns durable
  append checkpoints; no new service or credential-bearing state is introduced.
- The SDK atomically persists a versioned checkpoint before the first append RPC.
  It contains the operation ID, exact encoded Flight messages, stage identity,
  and an integrity digest, but never object bytes or credentials.
- Restart discovery lists only validated operation IDs with a finite entry cap.
  Loading or resuming one ID reads only that checkpoint and enforces the normal
  Flight payload ceiling before allocation.
- Checkpoint filenames are derived only from validated operation IDs. File
  contents must agree with the filename, stage identity, descriptor operation,
  and payload digest or loading fails closed.
- A successful or unambiguously rejected append removes its checkpoint.
  Ambiguous transport failures and retry-window expiry retain it.
- If commit succeeded but the process died before checkpoint removal, replaying
  the checkpoint converges to the original version through the existing server
  idempotency contract.
- With no checkpoint directory configured, current memory-only behavior remains
  unchanged.

## Boundaries

### Allowed Changes
Cargo.lock
Cargo.toml
crates/lake-sdk/**
README.md
docs/architecture.md
docs/design/managed-objects.md
docs/plans/2026-07-12-durable-sdk-appends.md
specs/issue-90-durable-sdk-appends.spec.md
verification/issue-90-durable-sdk-appends.md

### Forbidden
crates/lake-common/**
crates/lake-meta/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-engine*/**
persisting object bytes or credentials
unbounded checkpoint directory scans
loading a checkpoint before enforcing its byte ceiling
allocating a new operation ID during recovery
background retry daemons
weakening server-side operation/digest validation

## Completion Criteria

Scenario: A restarted SDK resumes the exact durable append
  Test:
    Package: lake-sdk
    Filter: durable_append_checkpoint_survives_client_restart
  Given a configured checkpoint directory and a prepared FILE append
  When the first SDK process is lost before a conclusive append result and a new client loads the checkpoint
  Then the new client reuses the exact operation ID and encoded payload without uploading object bytes again

Scenario: Replay after commit converges to the original version
  Test:
    Package: lake-sdk
    Filter: durable_append_checkpoint_replays_post_commit_crash
  Given the server committed an append but its durable SDK checkpoint was not removed
  When a restarted client resumes that operation
  Then the server returns the original committed version and the checkpoint is removed

Scenario: Conclusive outcomes clean durable state
  Test:
    Package: lake-sdk
    Filter: durable_append_checkpoint_cleans_up_conclusive_outcomes
  Given a persisted append checkpoint
  When append succeeds or returns a non-ambiguous terminal rejection
  Then the SDK removes the checkpoint while ambiguous failures retain it

Scenario: Invalid durable state fails closed within finite bounds
  Test:
    Package: lake-sdk
    Filter: durable_append_checkpoint_rejects_invalid_state
  Given corrupt, oversized, path-invalid, stage-mismatched, or descriptor-mismatched checkpoint state
  When the SDK lists or loads recovery state
  Then it returns a typed error without network append, path escape, or oversized allocation

Scenario: Response decoding failures retain the operation identity
  Test:
    Package: lake-sdk
    Filter: post_commit_response_decode_failure_retains_exact_checkpoint
  Given an append request may have committed before its response was decoded
  When Flight reports protocol, decode, or Arrow response failure
  Then the SDK classifies the result as ambiguous rather than deleting recovery state

Scenario: Post-publish sync uncertainty returns the exact pending append
  Test:
    Package: lake-sdk
    Filter: published_checkpoint_sync_failure_returns_recoverable_operation
  Given the final checkpoint rename succeeded but parent directory sync failed
  When preparation reports the durability uncertainty
  Then the error returns the exact operation and published checkpoint path for recovery

Scenario: Invalid committed result metadata returns operation ownership
  Test:
    Package: lake-sdk
    Filter: post_commit_invalid_result_metadata_returns_pending_append
  Given the server committed but returned malformed version metadata
  When the SDK cannot decode the successful result
  Then the error retains the exact pending append and checkpoint for idempotent replay

Scenario: Checkpoint loading never follows symbolic links
  Test:
    Package: lake-sdk
    Filter: checkpoint_load_never_follows_symlinks
  Given an operation checkpoint path is replaced by a symbolic link
  When the SDK loads that operation
  Then it fails closed without following the link or allocating from its target

Scenario: Checkpointing remains explicitly opt-in
  Test:
    Package: lake-sdk
    Filter: append_without_checkpoint_directory_remains_memory_only
  Given a client without a checkpoint directory
  When it prepares and retries an append
  Then no filesystem durable state is required and existing PendingAppend behavior is preserved

## Out of Scope

- Persisting non-FILE query results or arbitrary SQL statements.
- A background operation queue, retry daemon, or distributed scheduler.
- Garbage-collecting objects for appends that were conclusively rejected.
- Sharing one checkpoint directory across mutually untrusted tenants.
- Changing the Flight append protocol or metadata authority.
