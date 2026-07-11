spec: task
name: "managed-objects"
inherits: project
tags: [objects, sdk, sql]
---

## Intent

Give the Rust SDK a first-class large-object parameter for a parameterized
single-row `INSERT`. The SDK must upload a local video/model directly to a
Lake-managed object prefix, append an immutable `DataLocation` reference to
the table snapshot, and let callers open that location without routing the
object bytes through query or metadata services.

Without this work, a user who has a multi-gigabyte video must upload it using
a separate object-store client, hand-construct a URI, and risk publishing a
table row that points at a partial or missing object. Reproducer: create an
episode row, start an out-of-band upload, then insert the URI before the
upload finishes; a training worker can resolve the row and fail on a partial
object. The SDK-owned upload and manifest-before-table-commit path eliminate
that visible torn state.

This advances `goal.md`'s signal that writers can commit new snapshots while
readers see only complete snapshots, while preserving disaggregated storage:
the SDK reads and writes object storage directly; Lake servers move only
metadata and RecordBatch streams.

## Decisions

- The first public SDK is Rust. Its client connects only to the query Flight
  endpoint. Query forwards metadata-only
  file append streams to the leader-aware metadata service; Python bindings
  remain follow-up work.
- The accepted SQL subset is one parameterized statement of the form
  `INSERT INTO <namespace>.<table> (<columns...>) VALUES (?, ...)`. Its
  logical large-object type is `FILE`; Rust binds it with
  `InsertValue::File(FileUpload::from_path(...))`. Arbitrary SQL DML remains
  rejected by the public query service.
- A SQL `FILE` is physically represented by a stable, immutable
  `DataLocation`: managed URI, content type, byte size, and SHA-256. Expiring
  signed URLs and tenant authorization are not stored in table rows and are
  deferred until the authenticated remote SDK exists.
- Uploads stream local files into a Lake-managed object prefix; the full file
  is never buffered in memory. The first slice supports the local filesystem
  backend and shares the same storage-location seam as the cloud backend.
- The SDK publishes the row only after the object upload and immutable object
  metadata complete. A failed upload creates no table version; objects left
  after a failed table CAS are unreachable and are explicitly out of scope for
  this slice's GC.
- `DataLocation` has an Arrow struct representation so Lance stores it with
  the episode/model metadata and SQL can return it as a value. The Rust SDK
  provides the direct reader for that value.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-common/**`
- `crates/lake-objects/**`
- `crates/lake-sdk/**`
- `crates/lake-cli/**`
- `crates/lake-query/**`
- `crates/lake-metasrv/**`
- `README.md`
- `docs/architecture.md`
- `docs/design/managed-objects.md`
- `docs/plans/**`
- `specs/**`
- `verification/**`

### Forbidden
- `crates/lake-meta/**`
- `crates/lake-engine/**`
- `crates/lake-engine-lance/**`
- `site/**`
- Adding a server-side endpoint that receives full object bytes (metadata-only
  `DataLocation` append streams are required)
- Accepting arbitrary object-store URIs or credentials in public SQL
- Storing expiring signed URLs in Lance rows
- Changing the registry or table commit protocol

## Completion Criteria

Scenario: SQL FILE insert uploads an object before publishing its DataLocation row
  Test:
    Package: lake-sdk
    Filter: client_connects_only_to_query_for_file_insert
  Given an empty Lance table with an object column and a local video file
  When the Rust SDK executes a parameterized INSERT with
  InsertValue::File(FileUpload::from_path(...))
  Then the row contains the immutable DataLocation and its direct reader yields
  the original bytes without query or metadata services carrying the payload

Scenario: failed object upload does not publish a partial row
  Test:
    Package: lake-sdk
    Filter: failed_upload_does_not_publish_a_table_version
  Given an object source that fails while streaming
  When the Rust SDK executes the INSERT
  Then the operation fails and the table remains at its previous snapshot

Scenario: unsupported INSERT syntax is rejected before any upload
  Test:
    Package: lake-sdk
    Filter: unsupported_insert_syntax_never_starts_an_upload
  Given SQL outside the parameterized single-row INSERT subset
  When the Rust SDK receives an object parameter
  Then it returns a syntax error and the managed object prefix is unchanged

## Out of Scope

- Multipart presigning, resumable cloud uploads, and Python/other language
  SDKs.
- Authentication, tenant authorization, signed `DataLocation` tickets, and
  direct public query-service exposure.
- Object GC, deduplication, cross-table transactions, frame decoding, model
  shard indexing, or SQL functions that inspect object bytes.
