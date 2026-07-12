spec: task
name: "bounded-sdk-batch-insert"
inherits: project
tags: [sdk, insert, file, batch, rust]
---

## Intent

Fleet writers currently pay one metadata commit for every episode. Add a
Rust-first bounded multi-row insert that uploads large objects directly to the
managed stage, sends only Arrow metadata through Flight, and publishes the
whole batch as one table version.

## Decisions

- `LakeClient::insert_many` accepts one existing parameterized INSERT shape
  plus owned rows of `InsertValue`.
- A batch contains 1..=10,000 rows. Empty or excessive batches fail before
  schema lookup, storage, or Flight I/O.
- Every row is parameter-count and type validated before the first FILE upload.
- FILE uploads are sequential in the first implementation, keeping object-byte
  memory bounded by the existing storage uploader rather than batch size.
- One successful call encodes one Arrow `RecordBatch`, uses one append
  operation identity, and commits one new table version. `insert` delegates to
  the same preparation path with one row.
- Upload and append failures retain the existing orphan/GC, resumability, and
  idempotent retry semantics; no failed call publishes a table version.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-sdk/**
README.md
docs/architecture.md
docs/plans/2026-07-12-sdk-batch-insert.md
specs/issue-77-sdk-batch-insert.spec.md
verification/issue-77-sdk-batch-insert.md

### Forbidden
crates/lake-common/**
crates/lake-meta/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-engine/**
crates/lake-engine-lance/**
cross-table transactions
unbounded row batches
parallel FILE upload fan-out
FILE bytes in SQL, Flight, or an Arrow batch

## Completion Criteria

Scenario: Multiple FILE rows publish one table version
  Test:
    Package: lake-sdk
    Filter: sdk_batch_insert_commits_multiple_files_as_one_version
  Given a connected Rust SDK client and multiple rows containing different large FILE values
  When insert_many executes one parameterized INSERT shape
  Then every object is directly readable and all rows become visible in one committed table version

Scenario: Invalid rows fail before uploads or commits
  Test:
    Package: lake-sdk
    Filter: sdk_batch_insert_validates_every_row_before_upload
  Given a batch whose later row has an invalid parameter count or type
  When insert_many validates the batch
  Then no object upload starts and no table version is published

Scenario: Batch cardinality is finite before I/O
  Test:
    Package: lake-sdk
    Filter: sdk_batch_insert_rejects_empty_and_excessive_batches
  Given an empty batch or more than 10,000 rows
  When insert_many is called
  Then a typed SDK error is returned before schema, storage, or Flight I/O

## Out of Scope

- Streaming or unbounded row iterators.
- Parallel upload scheduling.
- Multi-table atomicity.
- New SQL grammar or server payload types.
