# Metadata Hardening and SQL-over-S3 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Restore lake's snapshot, write-coordination, cache-shield, and streaming invariants, then record the supported SQL-over-S3 API shape.

**Architecture:** The registry remains the authority for a table snapshot. Reads open the exact registered version; every engine operation that creates a version returns it for a registry CAS. Metadata writes are serialized per table and destructive operations use conditional state transitions. Query nodes refresh metadata on a bounded background/TTL path and stream both query results and ingest batches. Flight SQL remains the public SQL protocol; large results may be materialized to S3 and exposed through standard Flight endpoint locations.

**Tech Stack:** Rust 2024, Tokio, DataFusion 53.1, Lance 8, Arrow Flight SQL, moka, DynamoDB, RocksDB, S3/object_store, snafu.

---

### Task 1: Make registry versions authoritative for reads

**Files:**
- Modify: `crates/lake-engine/src/engine.rs`
- Modify: `crates/lake-engine-lance/src/lib.rs`
- Modify: `crates/lake-catalog/src/schema.rs`
- Test: `crates/lake-engine-lance/src/lib.rs`

1. Add a regression test that creates v1, appends v2, asks for a v1 provider, and asserts only v1 rows are visible.
2. Run `cargo test -p lake-engine-lance table_provider_reads_requested_snapshot -- --exact` and confirm it fails because `version` is ignored.
3. Change the provider API to return `Result<Arc<dyn TableProvider>>` asynchronously and have Lance checkout the requested version.
4. Propagate provider errors through `LakeSchema::table`.
5. Re-run the focused test and the affected crate tests.

### Task 2: Keep maintenance commits in the registry protocol

**Files:**
- Modify: `crates/lake-engine/src/engine.rs`
- Modify: `crates/lake-engine-lance/src/lib.rs`
- Modify: `crates/lake-metasrv/src/maintenance.rs`
- Test: `crates/lake-metasrv/src/maintenance.rs`

1. Add a test with a real RocksMeta + LanceEngine table whose maintenance compacts fragments and assert registry `current_version` equals the engine version afterward.
2. Verify the test fails because `maintain` returns `()` and the registry is untouched.
3. Make `maintain` return `Option<Version>` (`None` when no commit), and CAS the registration when it returns a version.
4. Treat a CAS conflict as a skipped stale maintenance result, never overwrite a newer registration.
5. Re-run the focused test and metasrv tests.

### Task 3: Serialize per-table writes and make drop conditional

**Files:**
- Modify: `crates/lake-meta/src/store.rs`
- Modify: `crates/lake-meta/src/rocks.rs`
- Modify: `crates/lake-meta/src/dynamo.rs`
- Modify: `crates/lake-meta/src/registry.rs`
- Modify: `crates/lake-metasrv/src/lib.rs`
- Test: `crates/lake-meta/src/registry.rs`
- Test: `crates/lake-metasrv/src/lib.rs`

1. Add a registry test proving a stale conditional delete cannot remove a replacement registration.
2. Add a metasrv concurrency test that pauses a drop, recreates the table, resumes the stale drop, and preserves the replacement.
3. Verify both fail with the unconditional `delete` API.
4. Replace blind registry deletion with compare-and-delete semantics in MetaStore implementations.
5. Add a per-table async mutex map in Metasrv and acquire it around create/append/drop/maintenance mutations.
6. Re-run focused concurrency and backend contract tests.

### Task 4: Enforce lease expiry at the write gate

**Files:**
- Modify: `crates/lake-metasrv/src/leadership.rs`
- Modify: `crates/lake-metasrv/src/control.rs`
- Test: `crates/lake-metasrv/src/leadership.rs`

1. Add a clock-injected test: publish a leader lease, advance beyond expiry without completing another campaign, and assert writes are rejected.
2. Verify it fails because `AtomicBool` remains true.
3. Publish a monotonic local deadline with leadership state and have the write gate check both holder and deadline.
4. Bound campaign I/O by a timeout strictly below the safety margin and demote on timeout.
5. Re-run leadership and two-node forwarding tests.

### Task 5: Turn the catalog into a real cache shield

**Files:**
- Modify: `crates/lake-catalog/src/catalog.rs`
- Modify: `crates/lake-catalog/src/schema.rs`
- Modify: `crates/lake-query/src/lib.rs`
- Modify: `crates/lake-query/src/flight.rs`
- Test: `crates/lake-catalog/src/catalog.rs`
- Test: `crates/lake-query/src/lib.rs`

1. Add a counting MetaStore contract test showing repeated SQL executions do not repeat registry scans inside the TTL.
2. Add a test showing registration backend errors surface rather than becoming `table not found`.
3. Verify both fail with per-request `refresh()` and `.ok()??`.
4. Build moka caches with TTL, coalesce refreshes, keep the last-good snapshot, and remove refresh calls from the request hot path.
5. Refresh once at startup and from a bounded background task.
6. Re-run catalog/query tests.

### Task 6: Preserve streaming on query and ingest paths

**Files:**
- Modify: `crates/lake-query/src/flight.rs`
- Modify: `crates/lake-engine-lance/src/lib.rs`
- Test: `crates/lake-query/src/flight.rs`
- Test: `crates/lake-engine-lance/src/lib.rs`

1. Add a delayed multi-batch stream test proving Flight emits the first batch before the producer completes.
2. Add an ingest test proving Lance consumes a fallible stream without collecting every batch first.
3. Verify both expose the current `collect()` behavior.
4. Feed DataFusion `execute_stream()` directly to `FlightDataEncoderBuilder`.
5. Adapt Lance's writer input to the streaming reader interface supported by Lance; if Lance requires a RecordBatchReader, introduce a bounded bridge with explicit backpressure rather than an unbounded Vec.
6. Re-run focused tests.

### Task 7: Harden Dynamo and S3 lifecycle behavior

**Files:**
- Modify: `crates/lake-meta/src/dynamo.rs`
- Modify: `crates/lake-engine-lance/src/manifest_store.rs`
- Modify: `crates/lake-engine-lance/src/lib.rs`
- Test: `crates/lake-meta/tests/dynamo_localstack.rs`
- Test: `crates/lake-engine-lance/tests/s3_lance_localstack.rs`

1. Extend the S3 integration test to drop and recreate the same location, and assert old manifest state cannot block v1.
2. Verify it fails because manifest keys survive object deletion.
3. Add dataset generations or an explicit manifest-prefix cleanup protocol that cannot collide with a recreated table.
4. Use strongly consistent Dynamo `GetItem` for correctness-bearing keys and avoid read-before-write where a conditional expression can perform the transition directly.
5. Run `mise run test-integration`.

### Task 8: Restrict the exposed SQL surface and document SQL over S3

**Files:**
- Modify: `crates/lake-query/src/lib.rs`
- Modify: `crates/lake-query/src/flight.rs`
- Create: `docs/design/sql-api-over-s3.md`
- Modify: `docs/architecture.md`
- Test: `crates/lake-query/src/lib.rs`

1. Add tests rejecting DDL/DML and arbitrary external object-store locations through the public query API while allowing `SELECT` and `EXPLAIN`.
2. Verify current `SessionContext::sql` accepts an unsafe statement class.
3. Use DataFusion `sql_with_options` to expose a read-only statement surface.
4. Document the API tiers: direct Flight Arrow stream for interactive queries; `PollFlightInfo` plus S3-backed Arrow/Parquet HTTPS endpoints for heavy results.
5. Specify authentication on every call, TLS, query time/memory/concurrency limits, opaque expiring tickets, and bucket/prefix allowlists.

### Task 9: Verify and commit locally

**Files:**
- Create: `verification/report.md`

1. Run every focused regression test and record RED/GREEN evidence.
2. Run `mise run gate`.
3. Run `mise run test-integration`.
4. Run concurrent drop/recreate and expired-leader hostile probes.
5. Run `mise run e2e` from a fresh temporary data directory.
6. Record commands and outcomes in `verification/report.md`.
7. Review the complete diff against `goal.md` and `docs/architecture.md`.
8. Create local conventional commits only; do not push.
