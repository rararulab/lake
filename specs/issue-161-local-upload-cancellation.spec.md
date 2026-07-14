spec: task
name: "local-upload-cancellation"
inherits: project
tags: [objects, local, upload, cancellation, reliability, bounded-resources]
---

## Intent

Keep a cancelled Lake-owned local `FILE` upload from leaking its unpublished
`.<uuid>.uploading` staging file. Today `LocalObjectStore::put_reader` removes
that file only after an ordinary copy error returns; aborting the task drops
the future first. Repeating that sequence for multi-gigabyte video/model
uploads can consume the Lake-owned local-stage disk even though no
`DataLocation` was returned or published.

## Decisions

- The local upload operation owns cleanup from staging-file creation until the
  atomic rename has published the final object. Dropping the caller task must
  cause the unpublished staging file to be removed without retaining source
  bytes or buffering the object.
- Successful upload disarms cleanup only after the existing atomic publish
  path has produced its immutable final object. It must not delete that object
  or change the returned `DataLocation`.
- Ordinary source/read errors retain the current typed error and staging-file
  cleanup behavior. This task does not turn cancellation into a resumable
  upload protocol or a permanent-object garbage collector.
- S3 ordinary-upload cancellation is explicitly excluded: #104/#105 already
  own that behavior with `MultipartCleanupOwner` and a LocalStack regression;
  PR #150 preserves it. This task must not duplicate, remove, or weaken that
  existing cleanup owner.
- Byte flow stays direct from the SDK reader to the local stage. Query and
  Metasrv receive neither object bytes nor cleanup state, and all local copy
  buffering remains bounded.

## Boundaries

### Allowed Changes
crates/lake-objects/src/local.rs
crates/lake-objects/src/lib.rs
docs/design/managed-objects.md
specs/issue-161-local-upload-cancellation.spec.md
verification/issue-161-local-upload-cancellation.md

### Forbidden
crates/lake-objects/src/s3.rs
crates/lake-objects/src/checkpoint.rs
crates/lake-sdk/**
crates/lake-query/**
crates/lake-meta/**
crates/lake-metasrv/**

## Acceptance Criteria

Rule: local-unpublished-staging-cancellation — a dropped local upload never leaves a stage file

Scenario: Cancelling a blocked local upload removes its unpublished staging file
  Test:
    Package: lake-objects
    Filter: cancelled_local_upload_removes_unpublished_staging_file
    Level: unit
    Test Double: deterministic AsyncRead that signals after staging begins and then remains pending
  Given a LocalObjectStore upload whose reader has written enough for its unique
  `.uploading` staging file to exist and is blocked before EOF
  When the owning put_reader task is aborted
  Then within the test's fixed timeout the managed stage contains no
  `.uploading` entry and no unpublished final object

Scenario: A completed local upload remains published and immutable
  Test:
    Package: lake-objects
    Filter: put_file_streams_bytes_and_returns_verified_location
    Level: unit
    Test Double: temporary source and managed-stage directories
  Given a finite local video source
  When LocalObjectStore completes the upload
  Then its DataLocation identifies the final bytes and SHA-256, the published
  object remains readable, and the managed stage has no `.uploading` entry

Scenario: An ordinary local source error remains typed and removes staging
  Test:
    Package: lake-objects
    Filter: local_upload_source_error_removes_unpublished_staging_file
    Level: unit
    Test Double: deterministic AsyncRead returning an injected I/O error
  Given a local upload whose source fails after staging begins
  When put_reader returns
  Then it returns ObjectError::Read and leaves neither a `.uploading` entry nor
  an unpublished final object

## Out of Scope

- S3 multipart cancellation, `AbortMultipartUpload`, and the #104/#105 cleanup owner.
- Resumable path uploads, checkpoints, explicit cancel_upload semantics, or
  process-crash recovery.
- Sweeping old unpublished files, permanent-object GC, deduplication, or
  changing DataLocation, SQL, Flight, Query, or Metasrv interfaces.
