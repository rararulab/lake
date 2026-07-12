spec: task
name: "cancel-safe-s3-uploads"
inherits: project
tags: [objects, s3, multipart, cancellation, reliability, bounded-resources]
---

## Intent

Ordinary reader-backed uploads cannot resume. Cancelling their caller future
after multipart creation currently leaves an orphan because Rust future drop
cannot await S3 cleanup. Give the active operation one bounded metadata-only
cleanup owner so normal cancellation converges to multipart abort without
moving object bytes through a server or retaining part buffers.

## Decisions

- Start exactly one cleanup owner only after S3 returns an upload id.
- The owner contains storage identity and the ordinary process-local AWS
  client, never source bytes, part buffers, serialized credentials, rows, or a
  checkpoint.
- Successful completion disarms cleanup. Explicit errors first drop the part
  pipeline, then wait for cleanup abort and return the original error.
- Dropping the caller decision channel triggers a best-effort abort capped at
  30 seconds. Process/host failure remains an S3 lifecycle responsibility.
- Resumable uploads are unchanged because cancellation intentionally preserves
  their upload and durable checkpoint for retry or explicit cancel.

## Boundaries

### Allowed Changes
crates/lake-objects/src/s3.rs
crates/lake-objects/tests/s3_localstack.rs
README.md
docs/design/managed-objects.md
docs/plans/2026-07-12-cancel-safe-s3-uploads.md
specs/issue-104-cancel-safe-s3-uploads.spec.md
verification/issue-104-cancel-safe-s3-uploads.md

### Forbidden
crates/lake-sdk/**
crates/lake-query/**
crates/lake-metasrv/**
DataLocation, SQL, Flight, or checkpoint wire changes
object bytes retained by cleanup tasks
unbounded cleanup retries, queues, or task lifetime
aborting resumable uploads on ordinary future drop

## Completion Criteria

Scenario: Cleanup ownership has explicit terminal states
  Test:
    Package: lake-objects
    Filter: multipart_cleanup_owner_state_transitions
  Given one ordinary multipart cleanup owner
  When it is dropped, disarmed, or explicitly aborted
  Then drop and explicit abort execute cleanup exactly once while disarm executes none

Scenario: Cancelled ordinary upload is integration wired
  Test:
    Package: lake-objects
    Filter: cancelled_s3_upload_is_aborted_is_wired
  Given the shared LocalStack integration runner
  When it runs ignored lake-objects protocol tests
  Then cancelling a blocked ordinary upload is verified to remove its multipart upload

Scenario: Explicit source failure still aborts
  Test:
    Package: lake-objects
    Filter: interrupted_s3_upload_is_aborted_is_wired
  Given an ordinary upload whose source reader fails
  When the existing LocalStack protocol suite runs
  Then no multipart upload or completed object remains

Scenario: Successful multipart publication remains wired
  Test:
    Package: lake-objects
    Filter: s3_multipart_roundtrip_localstack_is_wired
  Given an ordinary upload that completes normally
  When the shared protocol suite runs
  Then the cleanup owner is disarmed without deleting the published object

## Out of Scope

- Cleanup after process, runtime, VM, or host failure.
- Changing resumable checkpoint or explicit-cancel semantics.
- Retrying abort beyond the AWS SDK policy and the cleanup lifetime cap.
- Adding SDK-wide upload concurrency or a global task supervisor.
