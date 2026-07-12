spec: task
name: "bounded-s3-upload-pipeline"
inherits: project
tags: [objects, s3, multipart, performance, bounded-memory, recovery]
---

## Intent

Lake's primary objects are multi-gigabyte videos and model checkpoints. Both
ordinary and resumable S3 uploads currently serialize every 5 MiB UploadPart
round trip, leaving bandwidth idle across hundreds of parts. Pipeline a small
number of async S3 requests while retaining immutable publication, exact
identity, bounded memory, and restart-safe checkpoints.

## Decisions

- Default to four in-flight parts and reject zero or more than sixteen before
  any S3 request. At the fixed 5 MiB part size, upload futures retain at most
  20 MiB by default and 80 MiB at the hard maximum.
- Read and SHA-256 hash source bytes strictly in order. Network responses may
  finish out of order, but completion and durable checkpoint publication are
  consumed in contiguous part-number order.
- Use owned async futures rather than detached tasks. Dropping the bounded
  future set cancels all requests owned by the operation; the caller then
  performs one best-effort multipart abort and returns the original error.
- Persist the concurrency that created a resumable checkpoint. A missing field
  means one, matching legacy serial checkpoints. Resume may use a new runtime
  concurrency, but remote uncheckpointed suffix acceptance is bounded by the
  persisted creator window and every suffix part is overwritten from the
  source before completion.
- Keep SDK row/FILE concurrency sequential. Nested concurrency needs a future
  shared byte budget and is not part of this change.

## Boundaries

### Allowed Changes
crates/lake-objects/**
README.md
docs/design/managed-objects.md
docs/guides/cli.md
docs/plans/2026-07-12-bounded-s3-upload-pipeline.md
specs/issue-102-bounded-s3-upload-pipeline.spec.md
verification/issue-102-bounded-s3-upload-pipeline.md

### Forbidden
crates/lake-sdk/**
crates/lake-query/**
crates/lake-metasrv/**
public SQL, Flight, or DataLocation schema changes
parallel SDK rows or FILE columns
unbounded task, buffer, retry, or result collections
trusting uncheckpointed remote suffix identity
browser multipart or cross-host checkpoints

## Completion Criteria

Scenario: Multipart requests overlap within an exact bound
  Test:
    Package: lake-objects
    Filter: bounded_multipart_pipeline_overlaps_with_exact_resource_cap
  Given more full parts than the configured upload concurrency
  When uploads are paused after admission
  Then requests overlap above one, never exceed the configured count, and live request bodies never exceed concurrency times part size

Scenario: Out-of-order responses publish ordered identity
  Test:
    Package: lake-objects
    Filter: multipart_pipeline_orders_parts_and_source_hash
  Given upload responses complete in reverse order
  When the pipeline drains
  Then completed parts are contiguous by number and SHA-256 equals one ordered pass over the source

Scenario: Pipeline failure cancels owned work and aborts publication
  Test:
    Package: lake-objects
    Filter: multipart_pipeline_failure_stops_admission
  Given one bounded in-flight UploadPart fails
  When the pipeline observes that failure
  Then it admits no unbounded suffix, drops every owned future, and the object-store path performs no multipart completion

Scenario: Upload concurrency configuration is finite
  Test:
    Package: lake-objects
    Filter: s3_upload_concurrency_rejects_unbounded_values
  Given zero, default, maximum, and excessive concurrency values
  When an S3 store is configured
  Then only values within one through sixteen are accepted before storage I/O

Scenario: Resumable checkpoints advance through contiguous responses
  Test:
    Package: lake-objects
    Filter: resumable_pipeline_checkpoint_stays_contiguous
  Given out-of-order successful part responses
  When checkpoint publication follows the pipeline
  Then the durable state advances only through the longest contiguous prefix and records the creator concurrency without secrets

Scenario: Crash-left remote suffix is bounded and overwritten
  Test:
    Package: lake-objects
    Filter: resumable_s3_pipeline_overwrites_ambiguous_suffix_localstack
  Given a checkpoint plus several remotely completed suffix parts left by concurrent requests
  When the same source resumes
  Then the suffix count is bounded by the persisted window, every suffix is re-uploaded from verified source bytes, and the final object identity is exact

Scenario: Real S3 pipeline coverage is integration-wired
  Test:
    Package: lake-objects
    Filter: bounded_s3_upload_pipeline_localstack_is_wired
  Given the shared LocalStack integration runner
  When lake-objects ignored protocol tests execute
  Then bounded ordinary, interrupted, and resumable multipart paths are included

## Out of Scope

- Dynamically sizing S3 parts for objects above the fixed-part 10,000 limit.
- Parallelizing distinct objects inside one SQL INSERT batch.
- Retrying failed UploadPart requests beyond the AWS SDK's configured policy.
- Persisting in-flight buffers or sharing upload checkpoints across hosts.
