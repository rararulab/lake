# Paged Table Maintenance Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:test-driven-development task-by-task.

**Goal:** Give every Metasrv maintenance tick a configurable upper bound on
registry candidates and table operations while preserving generation safety.

**Architecture:** `lake-meta` exposes decoded table-registration pages backed
by the existing opaque metastore cursor. Metasrv stores only the current cursor
in process memory and processes one page per tick, re-reading each candidate
under its table lock. `MaintenanceLimits` enters through the server config and
CLI environment boundary, so interval and page size are immutable and validated
before serving.

**Tech Stack:** Rust, Tokio, Snafu, RocksDB/DynamoDB MetaStore, tonic server
configuration, clap environment parsing, agent-spec.

---

### Task 1: Typed registry pages

**Files:**
- Modify: `crates/lake-meta/src/registry.rs`

1. Add the failing `scan_table_pages_are_bounded_and_resumable` test with five
   registrations and page limit two.
2. Run the focused test and confirm RED because `scan_tables_page` and its
   typed result do not exist.
3. Add `TableRegistrationPage` accessors and `scan_tables_page`, decoding
   `tbl/<namespace>/<name>` entries from `scan_prefix_page` while preserving
   its continuation token.
4. Re-run the focused test and full `lake-meta` tests; commit.

### Task 2: One registry page per maintenance tick

**Files:**
- Modify: `crates/lake-metasrv/src/lib.rs`
- Modify: `crates/lake-metasrv/src/maintenance.rs`

1. Add `table_maintenance_pages_resume_without_full_registry_sweep` using a
   counting engine and metastore wrapper that records page scans, namespace
   listings, and point reads.
2. Run the focused test and confirm RED against the current full sweep.
3. Add a process-local table-maintenance cursor to `MetasrvInner` and a private
   `sweep_table_page` that scans one typed page, publishes the continuation,
   then processes only those candidates.
4. Keep cancellation-before-lock and re-resolve-under-lock semantics; return
   page statistics and emit one tracing event.
5. Re-run the focused test and existing maintenance tests; commit.

### Task 3: Prove generation safety

**Files:**
- Modify: `crates/lake-metasrv/src/maintenance.rs`

1. Add `table_maintenance_reresolves_after_scanned_generation_changes` with a
   scan-notifying metastore wrapper and a held table lock.
2. Replace the registration after scan but before releasing the lock; confirm
   RED if maintenance uses the scanned value.
3. Ensure the implementation point-reads current state under the lock and
   sends only the replacement location/version to the engine.
4. Re-run both page tests and commit.

### Task 4: Deployment configuration

**Files:**
- Modify: `crates/lake-metasrv/src/lib.rs`
- Modify: `crates/lake-metasrv/src/maintenance.rs`
- Modify: `crates/lake-cli/src/commands/limits.rs`
- Modify: `crates/lake-cli/src/commands/serve.rs`
- Modify: `crates/lake-metasrv/AGENT.md`
- Modify: `docs/architecture.md`
- Modify: `docs/guides/cli.md`

1. Add failing `maintenance_limit_values_are_validated_before_serving` cases
   for zero, malformed, over-10000, and valid values.
2. Add `MaintenanceLimits::try_new`, defaults/accessors, and
   `MetasrvServerConfig::with_maintenance_limits`.
3. Parse both environment variables in the CLI, thread limits into the server,
   use the configured interval and page size in the maintenance loop, and
   document defaults/semantics.
4. Run CLI and Metasrv focused/full tests; commit.

### Task 5: Verify and publish

**Files:**
- Create: `verification/issue-59-paged-maintenance.md`

1. Run the task-contract lifecycle, nightly rustfmt, affected crate tests, and
   strict clippy with all targets/features and warnings denied.
2. Run `mise run gate`.
3. Obtain independent correctness review and release verification, record
   evidence, push, open the PR, and merge only after APPROVE/PASS.
