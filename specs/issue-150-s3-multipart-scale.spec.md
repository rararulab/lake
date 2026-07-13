spec: task
name: "s3-multipart-scale"
inherits: project
tags: [objects, s3, upload, file, streaming]
---

## Intent

Let the Rust SDK stream managed video/model `FILE` values far beyond the
current fixed-5-MiB multipart ceiling without issuing an invalid S3 part
number. The data plane must remain a bounded direct SDK-to-S3 stream.

## Decisions

- The default S3 multipart chunk grows from 5 MiB to 64 MiB. Each request-body
  buffer remains bounded while raising the normal 10,000-part stream capacity
  from roughly 48.8 GiB to roughly 625 GiB.
- Before accepting an additional non-empty part after part 10,000, the stage
  returns a typed local error. It never sends part number 10,001 and aborts
  the already-created multipart upload through the existing failure path. The
  same terminal error aborts a resumable upload and removes its checkpoint.
- New resumable V1 checkpoints record 64 MiB parts. Existing V1 checkpoints
  recorded with the former 5 MiB size resume with that persisted size for
  completed-prefix rehashing and the remaining pipeline. Only those explicit
  5 MiB and 64 MiB values are accepted; other checkpoint sizes fail closed.
- The task does not pre-scan or spool an `AsyncRead` to learn its final size:
  that would break the streaming memory/disk boundary. It also does not change
  S3 credentials, Query, Metasrv, Flight, or the `DataLocation` format.

## Boundaries

### Allowed Changes
crates/lake-objects/**
README.md
docs/design/managed-objects.md
docs/plans/2026-07-13-s3-multipart-scale.md
specs/issue-150-s3-multipart-scale.spec.md
verification/issue-150-s3-multipart-scale.md

### Forbidden
crates/lake-cli/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-query/**
crates/lake-sdk/**
DataLocation schema or URI format changes
whole-object memory buffering, disk spooling, or source pre-scans
S3 credential, bucket, prefix, or signing policy changes

## Acceptance Criteria

Rule: s3-multipart-part-boundary — part limits fail locally and deterministically

Scenario: The 10,001st multipart part is rejected before S3 I/O
  Test:
    Package: lake-objects
    Filter: multipart_part_number_limit_rejects_10001st_part
    Level: unit
    Test Double: no S3 service; deterministic part-number boundary helper
  Given an S3 multipart upload that has already accepted part number 10,000
  When the stream contains another non-empty part
  Then the stage returns its typed multipart-limit error rather than producing
  an invalid S3 part number, while part 10,000 itself remains valid

Scenario: A legacy V1 checkpoint resumes without repartitioning its source
  Test:
    Package: lake-objects
    Filter: resumable_checkpoint_accepts_legacy_part_size_when_default_grows
    Level: unit
    Test Double: credential-free V1 checkpoint binding
  Given a valid V1 checkpoint whose completed parts use 5 MiB chunks
  When a store whose default is 64 MiB restores it
  Then it accepts the stored chunk size; a checkpoint size other than 5 MiB or
  64 MiB fails closed

Scenario: A legacy V1 checkpoint partitions its remaining pipeline at 5 MiB
  Test:
    Package: lake-objects
    Filter: resumable_pipeline_keeps_legacy_checkpoint_part_size_for_remaining_input
    Level: unit
    Test Double: in-memory remaining source and UploadPart recorder
  Given an accepted 5 MiB V1 checkpoint and 5 MiB plus one byte still to send
  When recovery reads its first remaining part and drives the pipeline
  Then it emits a 5 MiB part followed by a one-byte part, never one 5 MiB plus
  one-byte part under the 64 MiB default

## Out of Scope

- Supporting the full S3 maximum object size for an arbitrary unknown-length
  stream; that requires an explicit future part-size or source-length contract.
- Changing the existing bounded-concurrency multipart, object-deduplication,
  or abort-on-failure protocols beyond the terminal part limit and the safe
  legacy checkpoint-size migration.
