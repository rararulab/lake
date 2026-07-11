# Incremental Object Reference GC Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Reclaim old orphan managed objects from local/S3 storage using incremental version journals rather than full episode-table scans.

**Architecture:** A canonical `ObjectReferenceDelta` records parent/new version plus added/removed immutable object identities. Lance extracts additions while consuming append batches and persists a sidecar before returning the new version; maintenance writes an empty lineage edge for compaction. A separate GC worker traverses retained roots, compares the live set with paginated age-gated inventory, emits a deterministic dry-run plan, and applies it through a resumable checkpoint.

**Tech Stack:** Rust, Arrow RecordBatch streaming, Lance version APIs, object_store, Tokio, serde JSON, AWS SDK S3, LocalStack, clap.

---

### Task 1: Canonical reference-delta domain

**Files:**
- Create: `crates/lake-common/src/object_reference.rs`
- Modify: `crates/lake-common/src/lib.rs`

**Step 1:** Write `object_reference_delta_roundtrips_canonically` for ordering, deduplication, version/parent validation, SHA/size identity preservation, unknown format version, and corrupt JSON.

**Step 2:** Run `cargo test -p lake-common object_reference_delta_roundtrips_canonically`; expect missing types/API.

**Step 3:** Implement `ObjectIdentity`, `ObjectReferenceDelta`, versioned wire representation, canonical constructor, `encode`, and fail-closed `decode`. Keep storage credentials and signed URLs outside the type.

**Step 4:** Run the focused test, all `lake-common` tests, fmt, and Clippy.

**Step 5:** Commit `feat(common): add object reference delta domain (#23)`.

### Task 2: Engine lifecycle seam

**Files:**
- Modify: `crates/lake-engine/src/engine.rs`
- Modify: `crates/lake-engine/src/error.rs`
- Modify: engine test doubles under `crates/**`

**Step 1:** Add compile-time tests for `retained_object_references(location, roots)` and define bounded page/result types.

**Step 2:** Run `cargo test -p lake-engine`; expect missing trait methods.

**Step 3:** Add the engine method with no default implementation so every backend must define fail-closed lifecycle behavior. Return deterministic pages, not one unbounded set.

**Step 4:** Update test doubles explicitly and run workspace check/Clippy.

**Step 5:** Commit `feat(engine): expose retained object reference lineage (#23)`.

### Task 3: Lance append and maintenance sidecars

**Files:**
- Modify: `crates/lake-engine-lance/src/lib.rs`
- Test: `crates/lake-engine-lance/src/lib.rs`
- Test: `crates/lake-engine-lance/tests/s3_lance_localstack.rs`

**Step 1:** Write `append_writes_object_reference_delta_without_retaining_batches` and `retained_object_references_follow_version_lineage` before implementation.

**Step 2:** Run both tests; expect missing sidecar/lineage behavior.

**Step 3:** Wrap the append stream with per-batch FILE extraction. Validate the exact DataLocation struct, collect canonical identities only, and release every batch immediately after Lance consumes it.

**Step 4:** After Lance returns a version, put `_lake/object_refs/<version>.json` with create-only/idempotent semantics. The delta parent is the handle's pinned version. Return an error before registry CAS if sidecar persistence fails.

**Step 5:** On version-producing maintenance, write an empty delta from the registered parent to the compacted version before returning it. Traverse `Dataset::versions()` roots with cycle/missing/mismatch checks and bounded reference pages.

**Step 6:** Prove local tests, then S3 protocol behavior with LocalStack; run Lance tests and Clippy.

**Step 7:** Commit `feat(lance): journal managed object references by version (#23)`.

### Task 4: Bounded fail-closed GC planner

**Files:**
- Create: `crates/lake-objects/src/gc.rs`
- Modify: `crates/lake-objects/src/lib.rs`
- Modify: `crates/lake-objects/src/local.rs`
- Modify: `crates/lake-objects/src/s3.rs`

**Step 1:** Write `gc_plan_marks_only_old_unreferenced_managed_objects` with multi-page inventory/live references, young candidates, prefix escape attempts, corrupt lineage, and deterministic output pages.

**Step 2:** Run the focused test; expect missing planner API.

**Step 3:** Add `ObjectInventory` and `ObjectDeleter` seams, candidate metadata (`uri`, size, last-modified), positive non-zero safety horizon, bounded page size, and a plan digest binding stage/horizon/cutoff/objects.

**Step 4:** Implement local and S3 inventory pagination. Reject non-managed keys and do not materialize all pages at once; use sorted spill/merge or page checkpoints for live membership at scale.

**Step 5:** Run object tests and Clippy.

**Step 6:** Commit `feat(objects): build safe orphan GC plans (#23)`.

### Task 5: Checkpointed apply and LocalStack

**Files:**
- Modify: `crates/lake-objects/src/gc.rs`
- Modify: `crates/lake-objects/tests/s3_localstack.rs`

**Step 1:** Add ignored `s3_gc_apply_resumes_from_checkpoint_localstack` plus a non-ignored wiring selector.

**Step 2:** Run it against LocalStack; expect missing apply behavior.

**Step 3:** Persist a versioned apply checkpoint binding the exact plan digest. Delete one bounded page at a time, fsync progress, count NotFound as success, and reject plan/checkpoint mismatch.

**Step 4:** Inject interruption after one page, restart, and verify no live/young object is deleted and completion is idempotent.

**Step 5:** Run all LocalStack integration tests, object tests, and Clippy.

**Step 6:** Commit `feat(objects): apply orphan GC plans resumably (#23)`.

### Task 6: Separate CLI worker, docs, and verification

**Files:**
- Modify: `crates/lake-cli/src/main.rs`
- Create: `crates/lake-cli/src/commands/gc.rs`
- Modify: `crates/lake-cli/src/commands/mod.rs`
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/design/managed-objects.md`
- Create: `verification/issue-23-object-gc.md`

**Step 1:** Write `gc_command_is_dry_run_unless_apply_is_explicit` before the command exists.

**Step 2:** Implement `lake gc` with required positive safety age, explicit `--apply`, checkpoint path for apply, deterministic human/JSON plan output, and no server startup. Refuse apply when any lineage is incomplete.

**Step 3:** Document cost model, retention interaction, dry-run/apply runbook, metrics, rollback limits, and why GC is separate from Metasrv.

**Step 4:** Run spec lint/lifecycle, full LocalStack integration, and `mise run gate`.

**Step 5:** Commit, push, create PR, and merge after all evidence is green.
