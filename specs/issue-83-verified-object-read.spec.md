spec: task
name: "streaming-integrity-verified-object-read"
inherits: project
tags: [sdk, objects, file, integrity, sha256, streaming]
---

## Intent

Make full-object reads fail closed when managed bytes no longer match the
immutable `DataLocation` identity stored in SQL. Today upload computes
`size_bytes` and SHA-256, but `LakeClient::open` streams whatever the backend
returns. Replacing or truncating an object therefore feeds corrupt video/model
bytes to the caller without any SDK error.

## Decisions

- `LakeClient::open` becomes integrity-verified by default; callers do not need
  to remember a second safer API.
- Validation remains streaming and constant-memory: bytes are hashed as the
  caller consumes them, never buffered as a whole object.
- A full read is verified only after the caller reaches EOF. Dropping a reader
  early makes no integrity claim and emits no misleading success signal.
- The expected SHA-256 must be exactly 64 hexadecimal characters and is
  validated before opening storage.
- The reader returns a terminal `std::io::ErrorKind::InvalidData` whose source
  is a public typed integrity error for short, long, or same-size hash-mismatch
  objects.
- Reads are capped at the declared size. One private probe byte detects a
  longer backend object without exposing that extra byte to the caller.
- `open_range` remains range-only. A partial interval cannot prove the stored
  full-object SHA-256 and must not claim otherwise.

## Boundaries

### Allowed Changes
crates/lake-objects/**
crates/lake-sdk/**
README.md
docs/architecture.md
docs/design/managed-objects.md
docs/plans/2026-07-12-verified-object-read.md
specs/issue-83-verified-object-read.spec.md
verification/issue-83-verified-object-read.md

### Forbidden
crates/lake-common/**
crates/lake-meta/**
crates/lake-query/**
crates/lake-metasrv/**
buffering an entire object
verification success before EOF
claiming full-object integrity for range reads
object bytes through Query or Metasrv
trusting backend-specific metadata instead of streamed bytes

## Completion Criteria

Scenario: Exact object identity verifies while streaming
  Test:
    Package: lake-objects
    Filter: verified_reader_accepts_exact_identity_while_streaming
  Given a DataLocation whose declared size and SHA-256 match a chunked reader
  When the caller drains the verified reader to EOF
  Then every byte is returned in order and EOF succeeds without whole-object buffering

Scenario: Corrupt or malformed object identity fails closed
  Test:
    Package: lake-objects
    Filter: verified_reader_rejects_invalid_short_long_and_hash_mismatch
  Given malformed SHA-256 metadata, a truncated object, an overlong object, or same-size wrong bytes
  When a verified full read is opened and drained
  Then malformed metadata fails before storage I/O and every byte mismatch ends with a typed InvalidData error

Scenario: Rust SDK full reads verify without metadata traffic
  Test:
    Package: lake-sdk
    Filter: sdk_open_verifies_datalocation_identity_without_query
  Given a LakeClient with an injected managed store and an unreachable Query channel
  When the caller uses the existing open method and drains the object
  Then exact bytes succeed, corrupt bytes fail integrity verification, and Query or Metasrv are not contacted

## Out of Scope

- Per-range checksums or Merkle trees.
- Integrity enforcement inside arbitrary presigned-URL consumers.
- Encryption, malware scanning, or media-format validation.
- Backend HEAD requests as a substitute for reading bytes.
