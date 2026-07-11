# Resumable S3 Managed Uploads Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Resume multi-gigabyte Rust SDK path uploads across restarts without re-uploading completed S3 multipart parts or weakening immutable visibility.

**Architecture:** Add a path-aware resumable method to the object-safe managed-store boundary while retaining `put_reader` for non-seekable streams. `lake-objects` persists a versioned checkpoint under an SDK-configured directory, locks it for one session, verifies and rehashes completed source parts, reconciles them with S3, and uploads only the suffix. The SDK opts path-backed `FILE` values into this method; Query and Metasrv remain unchanged.

**Tech Stack:** Rust, Tokio seek/read APIs, serde JSON, SHA-256, AWS SDK S3 multipart/ListParts/Head/Get, LocalStack, Arrow Flight SDK tests.

---

### Task 1: Versioned checkpoint domain and atomic persistence

**Files:**
- Modify: `crates/lake-objects/Cargo.toml`
- Create: `crates/lake-objects/src/checkpoint.rs`
- Modify: `crates/lake-objects/src/lib.rs`

**Step 1:** Add `resumable_checkpoint_validates_source_and_stage` covering JSON round-trip, source size/mtime identity, bucket/prefix/key/upload id, fixed part size, part ETag/checksum/SHA-256, secret absence, and typed mismatch errors.

**Step 2:** Run `cargo test -p lake-objects resumable_checkpoint_validates_source_and_stage` and verify it fails because checkpoint types do not exist.

**Step 3:** Implement `UploadCheckpointV1`, `SourceIdentity`, `CheckpointPart`, atomic temp-file + rename persistence, restrictive file permissions where supported, and an exclusive checkpoint file lock held for the session. Reject unknown versions and every binding mismatch.

**Step 4:** Run the focused test, `mise run fmt-check`, and `cargo clippy -p lake-objects --all-targets -- -D warnings`.

**Step 5:** Commit `feat(objects): add durable upload checkpoints (#21)`.

### Task 2: Path-aware object-store boundary

**Files:**
- Modify: `crates/lake-objects/src/lib.rs`
- Modify: `crates/lake-objects/src/local.rs`
- Modify: `crates/lake-objects/src/s3.rs`

**Step 1:** Add unit tests proving `put_path(path, content_type, Some(checkpoint))` is object-safe, local storage preserves atomic publication, and reader uploads remain non-resumable.

**Step 2:** Run the tests and verify the missing trait method fails compilation.

**Step 3:** Add `ManagedObjectStore::put_path`; its default opens the path and delegates to `put_reader`. Override local directly and reserve the S3 override for Task 3. Add `cancel_upload(checkpoint)` with a default typed unsupported result rather than guessing backend state.

**Step 4:** Run `cargo test -p lake-objects` and Clippy.

**Step 5:** Commit `feat(objects): add path-aware upload boundary (#21)`.

### Task 3: S3 resume and reconciliation state machine

**Files:**
- Modify: `crates/lake-objects/src/checkpoint.rs`
- Modify: `crates/lake-objects/src/s3.rs`
- Modify: `crates/lake-objects/tests/s3_localstack.rs`

**Step 1:** Add ignored LocalStack tests `resumable_s3_upload_reuses_completed_parts_localstack`, `resumable_s3_upload_rejects_changed_source_localstack`, and `cancel_resumable_s3_upload_aborts_and_removes_checkpoint_localstack`. Add non-ignored wiring tests so lifecycle selectors cannot silently match zero protocol tests.

**Step 2:** Run the LocalStack tests and verify the resume API is missing.

**Step 3:** On first upload, create a random key/upload id and persist before part 1. After each successful part, persist its number, length, ETag, S3 checksum, and SHA-256 atomically. On retry, call `ListParts`, require an exact ordered match, seek/read/hash every completed local part, then continue at the first missing part with one 5 MiB buffer.

**Step 4:** Preserve checkpoint + multipart state on read, UploadPart, ListParts, and ambiguous Complete errors. If the random destination already exists after an ambiguous completion, stream it through SHA-256 and require the expected length/hash before returning a location. Remove checkpoint only after verified completion.

**Step 5:** Implement explicit cancel as checkpoint load/lock → exact stage validation → AbortMultipartUpload → checkpoint removal. Treat an already absent upload as idempotent success only when no destination object exists.

**Step 6:** Run the three ignored tests against LocalStack, all `lake-objects` tests, and Clippy.

**Step 7:** Commit `feat(objects): resume S3 multipart uploads (#21)`.

### Task 4: SDK configuration and FILE insert integration

**Files:**
- Modify: `crates/lake-sdk/src/lib.rs`
- Modify: `crates/lake-sdk/examples/managed_file.rs`

**Step 1:** Add `sdk_resumable_file_insert_uses_checkpoint_directory` with a recording `ManagedObjectStore`: path sources must call `put_path` with a deterministic checkpoint path; reader sources must still call `put_reader`; Query receives only the resulting DataLocation batch.

**Step 2:** Run the focused test and verify it fails because `LakeClientBuilder::with_upload_checkpoint_dir` and path-aware dispatch are absent.

**Step 3:** Add the builder option, create the directory before connection, derive a collision-resistant checkpoint filename from canonical source path plus managed-stage identity, and route only `FileUpload::from_path` through `put_path`. Keep the existing builder behavior when no directory is configured.

**Step 4:** Extend the example with checkpoint configuration and ensure errors never print checkpoint contents, credentials, or bearer values.

**Step 5:** Run `cargo test -p lake-sdk`, the managed-file example check, and Clippy.

**Step 6:** Commit `feat(sdk): resume path-backed FILE uploads (#21)`.

### Task 5: Documentation and complete verification

**Files:**
- Modify: `README.md`
- Modify: `docs/design/managed-objects.md`
- Modify: `docs/architecture.md`
- Modify: `specs/issue-21-resumable-s3.spec.md`
- Create: `verification/issue-21-resumable-s3.md`

**Step 1:** Document checkpoint directory ownership, crash/network retry behavior, explicit cancellation, source mismatch failure, bounded memory, and the unchanged SQL visibility boundary.

**Step 2:** Run `mise run spec-lint specs/issue-21-resumable-s3.spec.md` and fix every gate failure.

**Step 3:** Run LocalStack ignored protocol tests and record the exact command/result in verification evidence.

**Step 4:** Run `mise run spec-lifecycle specs/issue-21-resumable-s3.spec.md` and `mise run gate`.

**Step 5:** Commit `docs(objects): document resumable FILE uploads (#21)`, push the bookmark, create the PR, and merge after the local gates pass.
