# Managed Objects Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let the Rust SDK execute a parameterized single-row `INSERT` with a local large-file parameter, stream that file into managed storage, and publish a queryable `DataLocation` only after the object is complete.

**Architecture:** Add a small object-domain crate that owns immutable `DataLocation` values, Arrow conversion, direct readers, and streaming local storage. Add a Rust SDK crate that accepts a deliberately narrow INSERT grammar plus typed parameters, uploads `ObjectFile` values directly through that storage layer, builds a one-row RecordBatch, and commits it through the existing per-table `Metasrv::append` path. Object bytes never cross query or metadata services; the existing table-version CAS remains the visibility boundary.

**Tech Stack:** Rust 2024, Tokio, DataFusion/Arrow 58, Lance 8, object_store 0.13, SHA-256, snafu, clap.

---

### Task 1: Define immutable `DataLocation` and its Arrow schema

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/lake-common/src/lib.rs`
- Create: `crates/lake-common/src/data_location.rs`
- Create: `crates/lake-objects/Cargo.toml`
- Create: `crates/lake-objects/AGENT.md`
- Create: `crates/lake-objects/src/lib.rs`
- Test: `crates/lake-objects/src/lib.rs`

**Step 1: Write the failing test**

Add `datalocation_arrow_roundtrip_preserves_identity` for a URI, MIME type,
size, and SHA-256 digest.

**Step 2: Run the test to verify it fails**

Run: `cargo test -p lake-objects datalocation_arrow_roundtrip_preserves_identity -- --exact`

Expected: FAIL because `lake-objects` and the Arrow conversion do not exist.

**Step 3: Write the minimal implementation**

Define the serializable `DataLocation` common type and its Arrow struct field /
array conversion in `lake-objects`; expose no storage backend here.

**Step 4: Run the test to verify it passes**

Run: `cargo test -p lake-objects datalocation_arrow_roundtrip_preserves_identity -- --exact`

Expected: PASS.

**Step 5: Commit**

```bash
jj commit -m "feat(objects): define immutable data locations (#4)"
```

### Task 2: Stream local files into the managed object prefix

**Files:**
- Modify: `crates/lake-objects/src/lib.rs`
- Create: `crates/lake-objects/src/local.rs`
- Test: `crates/lake-objects/src/local.rs`

**Step 1: Write the failing test**

Add `put_file_streams_bytes_and_returns_verified_location`, using a multi-chunk
fixture and asserting the returned digest, size, immutable destination, and
read-back bytes.

**Step 2: Run the test to verify it fails**

Run: `cargo test -p lake-objects put_file_streams_bytes_and_returns_verified_location -- --exact`

Expected: FAIL because no managed-object store exists.

**Step 3: Write the minimal implementation**

Implement a local managed-object store under `<data-dir>/objects`, copying in
bounded chunks while incrementally hashing. Publish only after the copy is
complete; return a `file://` `DataLocation`.

**Step 4: Run the test to verify it passes**

Run: `cargo test -p lake-objects put_file_streams_bytes_and_returns_verified_location -- --exact`

Expected: PASS.

**Step 5: Commit**

```bash
jj commit -m "feat(objects): stream files into managed local storage (#4)"
```

### Task 3: Add the Rust SDK and narrow INSERT binding

**Files:**
- Modify: `Cargo.toml`
- Create: `crates/lake-sdk/Cargo.toml`
- Create: `crates/lake-sdk/AGENT.md`
- Create: `crates/lake-sdk/src/lib.rs`
- Test: `crates/lake-sdk/src/lib.rs`

**Step 1: Write the failing test**

Add `insert_sql_uploads_object_and_queries_datalocation`: create an object
column table, execute `INSERT INTO robots.episodes (...) VALUES (?, ?)` with
`ObjectFile::from_path`, query the row through `QueryEngine`, and open the
returned location to assert byte equality.

**Step 2: Run the test to verify it fails**

Run: `cargo test -p lake-sdk insert_sql_uploads_object_and_queries_datalocation -- --exact`

Expected: FAIL because `LakeClient`, typed parameters, and INSERT binding do
not exist.

**Step 3: Write the minimal implementation**

Implement the typed local `LakeClient`, exact single-row INSERT parser,
`ObjectFile` parameter, scalar/object RecordBatch builder, and direct reader.
Resolve the table schema before upload, reject a mismatch before I/O, upload
object parameters, then call `Metasrv::append` once.

**Step 4: Run the test to verify it passes**

Run: `cargo test -p lake-sdk insert_sql_uploads_object_and_queries_datalocation -- --exact`

Expected: PASS.

**Step 5: Commit**

```bash
jj commit -m "feat(sdk): upload object parameters through insert SQL (#4)"
```

### Task 4: Lock failure and no-I/O semantics

**Files:**
- Modify: `crates/lake-sdk/src/lib.rs`
- Test: `crates/lake-sdk/src/lib.rs`

**Step 1: Write the failing tests**

Add `failed_upload_does_not_publish_a_table_version` and
`unsupported_insert_syntax_never_starts_an_upload`.

**Step 2: Run the tests to verify they fail**

Run: `cargo test -p lake-sdk 'failed_upload_does_not_publish_a_table_version|unsupported_insert_syntax_never_starts_an_upload'`

Expected: FAIL because failure sequencing and grammar validation are absent.

**Step 3: Write the minimal implementation**

Validate grammar, placeholder count, column names, and Arrow types before
opening object files. Keep upload errors before `Metasrv::append`; return
domain errors with context.

**Step 4: Run the tests to verify they pass**

Run: `cargo test -p lake-sdk 'failed_upload_does_not_publish_a_table_version|unsupported_insert_syntax_never_starts_an_upload'`

Expected: PASS.

**Step 5: Commit**

```bash
jj commit -m "test(sdk): cover managed object insert failures (#4)"
```

### Task 5: Wire local CLI construction and document the capability

**Files:**
- Modify: `crates/lake-cli/src/commands/mod.rs`
- Modify: `docs/architecture.md`
- Create: `docs/design/managed-objects.md`
- Test: `crates/lake-sdk/src/lib.rs`

**Step 1: Write the failing test**

Add a construction test proving the local CLI context gives the SDK an object
prefix separate from Lance datasets.

**Step 2: Run the test to verify it fails**

Run: `cargo test -p lake-sdk local_context_uses_a_managed_object_prefix -- --exact`

Expected: FAIL because the context has no object-storage seam.

**Step 3: Write the minimal implementation and documentation**

Expose the local object prefix through the application boundary. Document
DataLocation, SDK upload/reading, snapshot visibility, retention/GC limits,
and the planned cloud ticket path.

**Step 4: Run focused tests and quality gates**

Run: `cargo test -p lake-objects -p lake-sdk`

Expected: PASS.

Run: `mise run spec-lifecycle specs/issue-4-managed-objects.spec.md`

Expected: PASS with all three selectors resolving.

Run: `mise run gate`

Expected: PASS.

**Step 5: Commit**

```bash
jj commit -m "docs(objects): describe SQL-managed large files (#4)"
```
