spec: task
name: "exact-range-reads"
inherits: project
tags: [objects, sdk, range, integrity, streaming]
---

## Intent

Make direct managed `FILE` range reads fail closed when storage ends before
the requested half-open interval completes. Today the SDK validates an interval
against immutable `DataLocation.size_bytes`, but a truncated local file or a
short S3 range body can still reach successful EOF. A video/model consumer can
therefore treat incomplete range bytes as valid input.

## Decisions

- A successful range reader yields exactly `end - start` bytes. A short
  backend stream ends with `InvalidData` whose source is the public typed
  `ObjectIntegrityError::PrematureEof`; its expected and actual values are
  range-stream byte counts.
- The exact-length wrapper caps returned bytes at the requested interval and
  maintains constant memory. It neither reads nor exposes bytes beyond that
  interval.
- Local and S3 direct store APIs enforce this contract. `LakeClient::open_range`
  applies the same unoverrideable guard so an injected or adapted store cannot
  make the SDK silently accept a short range stream.
- The S3 stage must also require its one Range GET response to declare the
  exact requested `Content-Range` and `Content-Length` before exposing its
  body. A proxy that ignores the Range header and returns byte zero is a
  wrong interval even when its returned length happens to match.
- This is not a full-object integrity claim. Per-range checksums, Merkle
  identities, full-object SHA-256 verification, HEAD metadata, and bytes
  through Query or Metasrv remain out of scope.

## Boundaries

### Allowed Changes
crates/lake-objects/src/integrity.rs
crates/lake-objects/src/lib.rs
crates/lake-objects/src/local.rs
crates/lake-objects/src/s3.rs
crates/lake-objects/tests/s3_localstack.rs
crates/lake-sdk/src/lib.rs
crates/lake-objects/AGENT.md
crates/lake-sdk/AGENT.md
README.md
docs/architecture.md
docs/design/managed-objects.md
specs/issue-154-exact-range-reads.spec.md
verification/issue-154-exact-range-reads.md

### Forbidden
crates/lake-common/**
crates/lake-query/**
crates/lake-metasrv/**
buffering an entire object or range
new range checksum or Merkle identity formats
backend HEAD requests as a substitute for streamed byte counts
object bytes through Flight services
claiming full-object SHA-256 verification for range reads

## Completion Criteria

Scenario: exact range readers stream the requested interval
  Test:
    Package: lake-objects
    Filter: exact_range_reader_returns_requested_bytes
  Given a backend stream containing at least one requested range
  When its exact range reader is drained through small caller buffers
  Then it returns exactly the requested bytes, succeeds at EOF, and does not
  buffer the interval

Scenario: a truncated local object fails at range EOF
  Test:
    Package: lake-objects
    Filter: local_range_reader_rejects_truncated_object
  Given a managed local object is truncated after its immutable DataLocation
  was produced while the requested range remains within that DataLocation
  When a caller drains the local range reader
  Then its available prefix is returned and terminal EOF is InvalidData with
  `ObjectIntegrityError::PrematureEof` containing the expected and observed
  range byte counts

Scenario: SDK range reads reject a short adapted storage stream
  Test:
    Package: lake-sdk
    Filter: sdk_open_range_rejects_truncated_stage_without_query
  Given a LakeClient with an injected managed store that returns fewer bytes
  than the already valid requested interval and an unreachable Query channel
  When the caller drains `LakeClient::open_range`
  Then the SDK returns the same typed terminal InvalidData error without
  contacting Query or Metasrv

Scenario: S3 response metadata confirms the requested interval
  Test:
    Package: lake-objects
    Filter: s3_range_response_requires_exact_interval
  Given the one S3 Range GET response metadata for a valid requested interval
  When the response omits, shifts, or changes either Content-Range or
  Content-Length
  Then the S3 stage rejects it before yielding a body; only the exact interval
  and exact range length are accepted

Scenario: the S3 range protocol smoke test remains wired
  Test:
    Package: lake-objects
    Filter: s3_range_read_localstack_is_wired
  Given the ignored LocalStack S3 range test source
  When the focused wiring test runs without Docker
  Then it proves the real protocol test still opens one bounded S3 range GET

## Out of Scope

- Full-object SHA-256 verification from a partial read.
- Per-range checksum or Merkle-tree identity formats.
- Multi-range responses, codec indexes, caching, encryption, or transcoding.
- Query/Metasrv object-byte proxying or storage metadata HEAD calls.
