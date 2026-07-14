spec: task
name: "ipc-decode-memory"
inherits: project
tags: [metasrv, flight, file-append, ipc, reliability, bounded-resources]
---

## Intent

Keep Metasrv's metadata-only `FILE` append decoder inside its existing
per-append control-memory admission. Each incoming FlightData message is
already charged against the finite append-stream budget, but its Arrow IPC
header must also be validated before `FlightRecordBatchStream` can decode a
batch. A malformed declared body or compressed batch must fail before a table
append can begin, without routing object bytes through Query or Metasrv.

## Decisions

- Parse each FlightData IPC header before record-batch decoding. A declared
  body length must be non-negative, fit the local platform size, and exactly
  match the FlightData body already charged to the append stream budget.
- Reject Arrow IPC compression for this control-plane path before Arrow can
  allocate or decompress a record batch. Bounded uncompressed `DataLocation`
  rows remain supported.
- Preserve the existing `InvalidArgument` failure class for malformed client
  control data, payload-digest validation, per-table append semantics, and the
  Query-to-Metasrv forwarding path. Query forwards the status class without
  exposing Metasrv's internal error text.
- Reuse the query async-result IPC framing rules only as local prior art; this
  task does not couple the two data paths or move their helpers across crates.
- S3, managed-object upload/download, SQL results, and the configured append
  admission limits remain unchanged. The metadata service still receives Arrow
  metadata rows only, never video/model bytes.

## Boundaries

### Allowed Changes
crates/lake-metasrv/src/control.rs
crates/lake-query/src/flight.rs
crates/lake-query/tests/file_append_proxy.rs
specs/issue-162-ipc-decode-memory.spec.md
verification/issue-162-ipc-decode-memory.md

### Forbidden
crates/lake-query/src/async_ipc.rs
crates/lake-sdk/**
crates/lake-objects/**
crates/lake-common/**
crates/lake-meta/**
crates/lake-metasrv/src/lib.rs
docs/architecture.md

## Acceptance Criteria

Rule: file-append-ipc-is-validated-before-decode — untrusted IPC cannot expand
the bounded Metasrv append path

Scenario: A declared body length mismatch fails before any table version changes
  Test:
    Package: lake-metasrv
    Filter: file_append_rejects_declared_body_mismatch_before_commit
    Level: unit
    Test Double: real RocksMeta and LanceEngine in a temporary directory
  Given a valid FILE append descriptor followed by a FlightData IPC header whose
  declared body length differs from its supplied body
  When Metasrv processes the append stream
  Then it returns InvalidArgument before record-batch decode and the table stays
  at its original version

Scenario: Compressed FILE append IPC fails before a table append
  Test:
    Package: lake-metasrv
    Filter: file_append_rejects_compressed_ipc_before_commit
    Level: unit
    Test Double: Arrow Flight encoder with ZSTD IPC options plus real local
      Metasrv storage
  Given an otherwise valid compressed Arrow IPC FILE append
  When Metasrv processes it
  Then it returns InvalidArgument without publishing a new version

Scenario: Query-forwarded compressed FILE append is rejected by Metasrv
  Test:
    Package: lake-query
    Filter: query_forwarded_file_append_rejects_compressed_ipc
    Level: integration
    Test Double: real loopback Query and Metasrv Flight services with local
      RocksMeta and LanceEngine
  Given an authenticated client that sends a compressed FILE append through
  Query
  When Query forwards the metadata stream to Metasrv
  Then the client receives InvalidArgument and the table has no new version

Scenario: Bounded uncompressed FILE append remains supported
  Test:
    Package: lake-metasrv
    Filter: file_append_commits_decoded_flight_batches
    Level: unit
    Test Double: real RocksMeta and LanceEngine in a temporary directory
  Given a bounded uncompressed Arrow Flight FILE append
  When Metasrv processes the stream
  Then the existing operation commits exactly one new table version

## Out of Scope

- Changing other Query forwarding paths, async-query result decoding, shared
  global IPC helpers, or gRPC message-size configuration.
- Increasing append stream/buffer budgets, adding compression support, or
  accepting object payloads in Metasrv.
- New tenant/distributed append quotas, resumable append changes, or storage
  lifecycle work.
