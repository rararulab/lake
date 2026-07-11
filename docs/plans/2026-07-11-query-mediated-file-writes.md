# Query-Mediated FILE Writes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the Rust SDK connect only to a Lake query endpoint while `lake-query` forwards metadata-only FILE appends to the leader-aware metadata service.

**Architecture:** The SDK uploads file bytes directly to its managed stage, then encodes the resulting `DataLocation` row as Arrow Flight `DoPut`. `lake-query` remains stateless: it validates the append descriptor and proxies the Arrow stream to `lake-metasrv`. `lake-metasrv` owns schema lookup, engine append, and registry-version CAS; neither server accepts the original video/model bytes.

**Tech Stack:** Rust 2024, Tokio, Arrow Flight, Flight SQL, Tonic, DataFusion/Arrow, Lance, Snafu.

---

### Task 1: Define the metadata-only append wire contract

**Files:**
- Modify: `crates/lake-common/src/lib.rs`
- Create: `crates/lake-common/src/file_write.rs`
- Test: `crates/lake-common/src/file_write.rs`

**Step 1: Write the failing test**

Encode a `FileAppendRequest { table }` into a Flight descriptor and decode it.
Assert malformed descriptors and non-`FILE` paths are rejected.

**Step 2: Run the test to verify it fails**

Run: `cargo test -p lake-common file_append_request_roundtrip`

Expected: FAIL because the wire value does not exist.

**Step 3: Write minimal implementation**

Add a small serializable value object that maps only
`lake.file.append/<namespace>/<table>` descriptors. It must carry table identity,
not object-store URI, credentials, or object bytes.

**Step 4: Run the test to verify it passes**

Run: `cargo test -p lake-common file_append_request_roundtrip`

Expected: PASS.

**Step 5: Commit**

`jj commit -m "feat(wire): define FILE append descriptor (#4)"`

### Task 2: Add a leader-aware metadata `DoPut` append endpoint

**Files:**
- Modify: `crates/lake-metasrv/src/control.rs`
- Modify: `crates/lake-metasrv/src/lib.rs`
- Test: `crates/lake-metasrv/tests/file_append_forwarding.rs`

**Step 1: Write the failing test**

Start leader and follower metadata Flight services. Send one Arrow RecordBatch
containing a `DataLocation` through `DoPut` to the follower and assert exactly
one new table version is committed by the leader.

**Step 2: Run the test to verify it fails**

Run: `cargo test -p lake-metasrv file_append_forwards_to_the_leader`

Expected: FAIL because `DoPut` is unsupported.

**Step 3: Write minimal implementation**

Decode the descriptor before accepting a stream, forward a follower request to
the elected leader, decode Arrow Flight batches there, validate the table schema,
and call `Metasrv::append`. Return only the committed `Version` in `PutResult`.

**Step 4: Run the test to verify it passes**

Run: `cargo test -p lake-metasrv file_append_forwards_to_the_leader`

Expected: PASS.

**Step 5: Commit**

`jj commit -m "feat(metasrv): accept FILE append streams (#4)"`

### Task 3: Make query a stateless write proxy

**Files:**
- Modify: `crates/lake-query/src/lib.rs`
- Modify: `crates/lake-query/src/flight.rs`
- Modify: `crates/lake-query/AGENT.md`
- Test: `crates/lake-query/src/flight.rs`

**Step 1: Write the failing test**

Configure a query Flight service with a metadata control address, submit a
metadata-only FILE append `DoPut`, and assert the service relays the committed
version without owning an engine or object payload.

**Step 2: Run the test to verify it fails**

Run: `cargo test -p lake-query file_append_is_forwarded_without_payload_proxying`

Expected: FAIL because query `DoPut` is unsupported.

**Step 3: Write minimal implementation**

Inject a `FileWriteForwarder` backed by a Flight channel to metadata. Query
validates the descriptor and proxies Flight frames/acknowledgements. Keep
Flight SQL statement execution read-only; this is a typed side channel for
already-uploaded `DataLocation` rows, not arbitrary DML.

**Step 4: Run the test to verify it passes**

Run: `cargo test -p lake-query file_append_is_forwarded_without_payload_proxying`

Expected: PASS.

**Step 5: Commit**

`jj commit -m "feat(query): proxy FILE append metadata (#4)"`

### Task 4: Replace SDK in-process metadata ownership with a query connection

**Files:**
- Modify: `crates/lake-sdk/Cargo.toml`
- Modify: `crates/lake-sdk/src/lib.rs`
- Modify: `crates/lake-sdk/AGENT.md`
- Test: `crates/lake-sdk/src/lib.rs`

**Step 1: Write the failing test**

Construct `LakeClient::connect(query_endpoint, managed_stage)` against running
Flight services. Assert `INSERT` uploads bytes directly to the stage and the
query endpoint commits only the `DataLocation` row; the SDK has no `Metasrv`,
engine, or metastore dependency.

**Step 2: Run the test to verify it fails**

Run: `cargo test -p lake-sdk client_connects_only_to_query_for_file_insert`

Expected: FAIL because `LakeClient` requires `Arc<Metasrv>`.

**Step 3: Write minimal implementation**

Replace `LakeClient::new(Arc<Metasrv>, ...)` with an async `connect` API over
the query Flight address. Fetch schema through the query endpoint, stream the
metadata-only Arrow batch with `DoPut`, decode the committed version, and keep
`FileUpload` direct I/O against the managed-stage adapter.

**Step 4: Run the test to verify it passes**

Run: `cargo test -p lake-sdk client_connects_only_to_query_for_file_insert`

Expected: PASS.

**Step 5: Commit**

`jj commit -m "refactor(sdk): connect FILE writes through query (#4)"`

### Task 5: Replace the in-process example and document the boundary

**Files:**
- Modify: `crates/lake-sdk/examples/managed_file.rs`
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/design/managed-objects.md`
- Modify: `specs/issue-4-managed-objects.spec.md`
- Modify: `verification/issue-4-managed-objects.md`

**Step 1: Write the failing example assertion**

Make the example call `LakeClient::connect` and fail if it imports `Metasrv`.

**Step 2: Run it to verify failure**

Run: `cargo run -p lake-sdk --example managed_file`

Expected: FAIL because the connection API does not yet exist.

**Step 3: Write minimal documentation and example changes**

Run embedded query/meta Flight services only as test/example deployment
fixtures. The SDK itself receives only the query URL and managed-stage adapter.
Document that query forwards metadata rows while object bytes bypass both
services.

**Step 4: Run it to verify success**

Run: `cargo run -p lake-sdk --example managed_file`

Expected: prints a successful direct read and no SDK `Metasrv` construction.

**Step 5: Commit**

`jj commit -m "docs(objects): document query-mediated FILE writes (#4)"`

### Task 6: Verify the complete path

**Files:**
- Modify: `verification/issue-4-managed-objects.md`

**Step 1: Run focused transport tests**

Run: `cargo test -p lake-common -p lake-metasrv -p lake-query -p lake-sdk file_append`

Expected: all descriptor, forwarding, and SDK connection tests pass.

**Step 2: Run the example and full gates**

Run: `cargo run -p lake-sdk --example managed_file`

Run: `mise run gate`

Run: `mise run spec-lifecycle specs/issue-4-managed-objects.spec.md`

Expected: direct object read succeeds; all quality and bound scenario checks pass.

**Step 3: Record evidence**

Record the SDK query-only connection and metadata-only forwarding evidence in
the verification report.
