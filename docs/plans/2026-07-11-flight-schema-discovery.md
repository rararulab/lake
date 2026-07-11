# Flight Schema Discovery Cache Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Return exact cached Arrow schemas from tenant-filtered Flight SQL table discovery without request-path metadata I/O.

**Architecture:** Store Arrow IPC schema bytes as an optional opaque field in each registry entry, keeping `lake-meta` Arrow-free and old JSON readable. Add a native key-value prefix scan to each metastore backend so one catalog refresh atomically replaces a process-local table-and-schema snapshot; Flight discovery reads only that snapshot.

**Tech Stack:** Rust, async-trait, RocksDB, DynamoDB, Arrow IPC/Flight SQL, DataFusion, tonic, jj, mise.

---

### Task 1: Add a key-value prefix scan contract

**Files:**
- Modify: `crates/lake-meta/src/store.rs`
- Modify: `crates/lake-meta/src/rocks.rs`
- Modify: `crates/lake-meta/src/dynamo.rs`
- Test: `crates/lake-meta/src/rocks.rs`

**Step 1: Write the failing test**

Add `prefix_entry_scan_returns_stripped_keys_and_values`: write `tbl/a/x`,
`tbl/a/y`, and an unrelated key; assert the scan returns ordered stripped keys
paired with exact bytes.

**Step 2: Verify RED**

Run: `cargo test -p lake-meta prefix_entry_scan_returns_stripped_keys_and_values`
Expected: compile failure because `scan_prefix` is absent.

**Step 3: Implement the minimal contract**

Add:

```rust
async fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
```

RocksDB must consume its prefix iterator once. DynamoDB must paginate a
consistent Scan with projection `pk,val`, preserve only well-formed pairs, strip
the prefix, and sort by stripped key before returning.

**Step 4: Verify GREEN and commit**

Run: `cargo test -p lake-meta prefix_entry_scan_returns_stripped_keys_and_values`
Commit: `feat(meta): add prefix entry scans (#27)`

### Task 2: Persist backward-compatible opaque schema bytes

**Files:**
- Modify: `crates/lake-meta/src/registry.rs`
- Modify: `crates/lake-catalog/Cargo.toml`
- Modify: `crates/lake-catalog/src/ops.rs`
- Test: `crates/lake-meta/src/registry.rs`

**Step 1: Write the failing compatibility test**

Decode the historical JSON shape without a schema field, and round-trip a new
registration containing known opaque bytes. Assert old `schema_ipc()` is `None`
and new bytes are exact.

**Step 2: Verify RED**

Run: `cargo test -p lake-meta registration_schema_payload_is_backward_compatible`
Expected: compile failure because the accessor/field is absent.

**Step 3: Implement encoding**

Add an optional serde-defaulted `schema_ipc: Option<Vec<u8>>` plus constructors
and accessors that prevent unrelated crates from assembling invalid fields.
Encode new schemas with `SchemaAsIpc` and `IpcWriteOptions::default()` in
`lake-catalog::create_table` before registration. Map encoding failure to a
typed `CatalogError` and never register a dataset with missing schema bytes.

**Step 4: Verify and commit**

Run: `cargo test -p lake-meta -p lake-catalog`
Commit: `feat(catalog): persist table schema IPC (#27)`

### Task 3: Replace listing refresh with one registration snapshot

**Files:**
- Modify: `crates/lake-meta/src/registry.rs`
- Modify: `crates/lake-catalog/src/catalog.rs`
- Modify: all test `MetaStore` implementations under `crates/**`
- Test: `crates/lake-catalog/src/catalog.rs`

**Step 1: Write the failing snapshot test**

Seed two registrations with distinct schema bytes, refresh once, then assert
the cached listing and `cached_table_schema(&TableRef)` both match. Count calls
and require exactly one prefix entry scan.

**Step 2: Verify RED**

Run: `cargo test -p lake-catalog catalog_refresh_caches_registration_schemas`
Expected: compile failure because the schema snapshot accessor is absent.

**Step 3: Implement atomic snapshot replacement**

Add `registry::scan_tables` to decode `(TableRef, TableRegistration)` pairs from
one `tbl/` scan. Build names and decoded `SchemaRef`s off-lock, fail refresh on
corrupt IPC, then replace one `CatalogSnapshot` under a single `RwLock` so names
and schemas cannot come from different refresh generations.

**Step 4: Verify and commit**

Run: `cargo test -p lake-meta -p lake-catalog`
Commit: `refactor(catalog): refresh one schema snapshot (#27)`

### Task 4: Return exact schemas from tenant-filtered Flight discovery

**Files:**
- Modify: `crates/lake-query/src/lib.rs`
- Modify: `crates/lake-query/src/flight.rs`
- Test: `crates/lake-query/src/flight.rs`

**Step 1: Write both failing Flight tests**

`flight_table_discovery_returns_cached_real_schema` must decode the table
metadata record batch, convert its schema IPC cell back to Arrow `Schema`, and
assert exact authorized fields plus a zero post-refresh metadata counter.
`flight_table_discovery_rejects_unknown_legacy_schema` must expect
`FailedPrecondition` for `include_schema=true`.

**Step 2: Verify RED**

Run: `cargo test -p lake-query flight_table_discovery_`
Expected: real-schema mismatch (currently empty) and legacy request succeeds.

**Step 3: Implement request-local lookup**

For each authorized table, append the cached real schema. When
`include_schema=false`, keep name discovery available for legacy entries. When
true and any visible schema is missing, return a generic `FailedPrecondition`;
perform no registry or engine call.

**Step 4: Verify and commit**

Run: `cargo test -p lake-query`
Commit: `feat(query): return cached schemas in Flight discovery (#27)`

### Task 5: Documentation and production verification

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`

**Step 1: Document compatibility**

State that new registrations carry schema IPC, legacy entries remain queryable
and listable, while `include_schema=true` requires migration/recreation until a
dedicated backfill command exists.

**Step 2: Run all gates**

Run:

```bash
mise run spec-lifecycle specs/issue-27-flight-schema-discovery.spec.md
mise run gate
mise run test-integration
```

Expected: every selector executes, all local gates pass, and LocalStack tests
pass including the paginated DynamoDB entry scan.

**Step 3: Commit and deliver**

Commit: `docs(catalog): document schema discovery cache (#27)`
Push `issue-27-flight-schema-discovery`, open a PR closing #27, and merge only
after the lifecycle and gate evidence are attached.
