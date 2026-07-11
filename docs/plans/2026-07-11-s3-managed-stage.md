# S3 Managed Stage Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Let the Rust SDK stream SQL `FILE` values directly to and from a Lake-owned S3 prefix with multipart completion/abort safety.

**Architecture:** Introduce an object-safe `ManagedObjectStore` with boxed Tokio readers and keep it behind `Arc` in `LakeClient`. Implement local and S3 backends; S3 uses AWS SDK multipart operations, persists stable `s3://` identity, validates managed-prefix containment, and aborts incomplete uploads. LocalStack proves data-path behavior without involving query/metasrv object bytes.

**Tech Stack:** Rust 2024, Tokio, async-trait, AWS SDK for Rust S3, Arrow Flight SDK path, LocalStack, Snafu.

---

### Task 1: Extract the managed-object store boundary

**Files:**
- Modify: `crates/lake-objects/Cargo.toml`
- Modify: `crates/lake-objects/src/lib.rs`
- Modify: `crates/lake-objects/src/local.rs`
- Modify: `crates/lake-sdk/src/lib.rs`
- Test: `crates/lake-sdk/src/lib.rs`

**Step 1:** Add `client_accepts_managed_object_store_abstraction`, constructing `LakeClient` through the wished-for trait and running insert/query/decode/open.

**Step 2:** Run `cargo test -p lake-sdk client_accepts_managed_object_store_abstraction`; expect a compile failure because the trait and generic constructor do not exist.

**Step 3:** Add `ObjectReader = Pin<Box<dyn AsyncRead + Send + Unpin>>` and async `ManagedObjectStore::{put_reader,open_reader}`. Implement it for `LocalObjectStore`; store `Arc<dyn ManagedObjectStore>` in `LakeClient` and accept any concrete implementation in `connect`.

**Step 4:** Re-run the focused test and all `lake-objects`/`lake-sdk` tests; expect PASS.

**Step 5:** Commit `refactor(objects): abstract the managed stage (#7)`.

### Task 2: Add S3 identity and containment

**Files:**
- Modify: `Cargo.toml`
- Modify: `crates/lake-objects/Cargo.toml`
- Create: `crates/lake-objects/src/s3.rs`
- Modify: `crates/lake-objects/src/lib.rs`
- Test: `crates/lake-objects/src/s3.rs`

**Step 1:** Add `s3_reader_rejects_locations_outside_managed_prefix` for wrong bucket and sibling prefix using a configured client that would fail if network I/O starts.

**Step 2:** Run the focused test; expect compile failure because `S3ObjectStore` does not exist.

**Step 3:** Add `S3ObjectStore::new(client, bucket, prefix)`, normalize a non-empty managed prefix, create stable object keys, parse `s3://bucket/key`, and reject wrong-bucket/out-of-prefix locations before `GetObject`.

**Step 4:** Re-run the focused test; expect PASS without LocalStack.

**Step 5:** Commit `feat(objects): validate S3 managed locations (#7)`.

### Task 3: Implement multipart upload and abort

**Files:**
- Modify: `crates/lake-objects/src/s3.rs`
- Create: `crates/lake-objects/tests/s3_managed_stage.rs`
- Create: `crates/lake-objects/tests/AGENT.md`
- Modify: `scripts/test-integration.ts`

**Step 1:** Add ignored LocalStack tests `s3_multipart_roundtrip_localstack` and `interrupted_s3_upload_is_aborted`. The round trip exceeds 5 MiB; the failure reader emits one full part then errors. Assert immutable metadata/direct bytes and no completed object respectively.

**Step 2:** Run with the integration environment; expect compile failure because S3 put/open are not implemented.

**Step 3:** Implement create/upload-part/complete with 5 MiB bounded buffers and incremental SHA-256. On read or AWS failure, call abort before returning the original error. Use `GetObject` body as the boxed direct reader.

**Step 4:** Run both LocalStack tests; expect PASS.

**Step 5:** Commit `feat(objects): stream multipart FILE objects to S3 (#7)`.

### Task 4: Prove SDK INSERT over S3 stage

**Files:**
- Modify: `crates/lake-sdk/src/lib.rs`
- Test: `crates/lake-sdk/src/lib.rs`
- Modify: `specs/issue-7-s3-managed-stage.spec.md`

**Step 1:** Add ignored `sdk_file_insert_uses_s3_stage` against LocalStack plus local query/metasrv fixtures. Insert a multipart-sized FILE, query its `s3://` DataLocation, and direct-read through the SDK.

**Step 2:** Run it before wiring the abstraction through all paths; expect failure.

**Step 3:** Make only the compatibility changes required by the test; object bytes must remain absent from query/metasrv.

**Step 4:** Re-run the integration test and existing local SDK suite; expect PASS.

**Step 5:** Commit `test(sdk): prove SQL FILE round trip on S3 (#7)`.

### Task 5: Document, verify, and publish

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/design/managed-objects.md`
- Modify: `crates/lake-objects/AGENT.md`
- Modify: `crates/lake-sdk/AGENT.md`
- Create: `verification/issue-7-s3-managed-stage.md`

**Step 1:** Document S3 client construction, credentials boundary, stable location identity, multipart abort semantics, and remaining presign/auth/GC exclusions.

**Step 2:** Run `cargo clippy -p lake-objects -p lake-sdk --all-targets -- -D warnings`, `mise run spec-lifecycle specs/issue-7-s3-managed-stage.spec.md`, the LocalStack integration suite, and `mise run gate`.

**Step 3:** Record RED/GREEN evidence, push an issue-7 bookmark, open a PR, and wait for every CI job. Do not merge without user authorization.
