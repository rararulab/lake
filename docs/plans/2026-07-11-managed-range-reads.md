# Managed Range Reads Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add exact, bounded byte-range reads to local/S3 managed objects and expose them through the Rust SQL `FILE` SDK.

**Architecture:** Extend the object-safe managed-stage boundary with a half-open `Range<u64>`. Validate it against immutable `DataLocation.size_bytes` before I/O; local seeks and limits a file, while S3 converts it to one inclusive HTTP Range header. `LakeClient` delegates directly to the configured stage, preserving the disaggregated data path.

**Tech Stack:** Rust 2024, Tokio `AsyncRead`/`AsyncSeek`, async-trait, AWS SDK for Rust S3, LocalStack, Snafu.

---

### Task 1: Define and validate range semantics

**Files:**
- Modify: `crates/lake-objects/src/lib.rs`
- Modify: `crates/lake-objects/src/local.rs`
- Test: `crates/lake-objects/src/lib.rs`

**Step 1:** Add failing tests for exact local bytes and empty/reversed/out-of-bounds ranges.

**Step 2:** Run the focused filters; expect compile failure because `open_range` and `InvalidRange` do not exist.

**Step 3:** Add the trait method, one shared validator, and local seek + bounded reader.

**Step 4:** Re-run both focused tests and the lake-objects unit suite; expect PASS.

**Step 5:** Commit `feat(objects): add bounded local range reads (#9)`.

### Task 2: Use S3 Range GET

**Files:**
- Modify: `crates/lake-objects/src/s3.rs`
- Modify: `crates/lake-objects/tests/s3_localstack.rs`

**Step 1:** Add ignored LocalStack `s3_range_read_returns_requested_bytes_localstack` plus its integration-wiring test.

**Step 2:** Compile the integration test; expect failure because S3 does not implement the new trait method.

**Step 3:** Reuse managed-key validation and issue `GetObject.range(bytes=start-endInclusive)`.

**Step 4:** Run the LocalStack object tests; expect exact bytes and EOF.

**Step 5:** Commit `feat(objects): read managed S3 byte ranges (#9)`.

### Task 3: Expose ranges through LakeClient

**Files:**
- Modify: `crates/lake-sdk/src/lib.rs`

**Step 1:** Add `sdk_opens_range_from_queried_datalocation` to the existing end-to-end local fixture.

**Step 2:** Run the filter; expect compile failure because `LakeClient::open_range` does not exist and the test delegate lacks the trait method.

**Step 3:** Add the SDK method and update the test adapter.

**Step 4:** Run all lake-sdk tests; expect PASS.

**Step 5:** Commit `feat(sdk): expose SQL FILE range reads (#9)`.

### Task 4: Document and publish

**Files:**
- Modify: `README.md`
- Modify: `docs/design/managed-objects.md`
- Modify: `docs/architecture.md`
- Create: `verification/issue-9-managed-range-reads.md`

**Step 1:** Document half-open semantics, validation, and a video-decoder-oriented example.

**Step 2:** Run clippy, spec lifecycle, LocalStack integration, and `mise run gate`.

**Step 3:** Record RED/GREEN evidence, commit, push the issue bookmark, and open a PR.
