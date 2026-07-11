# Bounded Flight Discovery Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bound Flight SQL schema/table discovery concurrency, row count, and
per-batch allocation while preserving local authorization and immutable catalog
generation pinning.

**Architecture:** Reuse `QueryAdmission` for discovery `DoGet` calls and retain
the permit in `AdmittedFlightStream`. Add a small validated `DiscoveryLimits`
configuration. Represent schema/table discovery as stateful lazy streams that
own the request-pinned `Arc<CatalogGeneration>`, emit at most `batch_rows` per
poll, and stop with `ResourceExhausted` on the first match beyond `max_rows`.

**Tech Stack:** Rust 2024, Tokio semaphore/timeouts, futures `Stream`, Arrow
Flight SQL metadata builders, tonic status streams, jj, agent-spec.

---

### Task 1: Validated discovery limits

**Files:**
- Modify: `crates/lake-query/src/lib.rs`
- Modify: `crates/lake-cli/src/commands/serve.rs`
- Modify: `crates/lake-cli/src/commands/limits.rs`
- Test: `crates/lake-cli/src/commands/limits.rs`

**Step 1: Write the failing test**

Add `discovery_limit_values_are_validated_before_serving`. Assert zero,
malformed, and `batch_rows > max_rows` environment values fail configuration,
while valid values reach `QueryServerConfig`.

**Step 2: Run test to verify it fails**

Run: `cargo test -p lake-cli discovery_limit_values_are_validated_before_serving -- --nocapture`
Expected: FAIL because the discovery environment variables are ignored.

**Step 3: Write minimal implementation**

Add `DiscoveryLimits { max_rows, batch_rows }` with `try_new`, accessors, and
defaults `10_000`/`256`. Add it to `QueryServerConfig` and parse
`LAKE_QUERY_MAX_DISCOVERY_ROWS` / `LAKE_QUERY_DISCOVERY_BATCH_ROWS` once before
binding.

**Step 4: Run test to verify it passes**

Run: `cargo test -p lake-cli discovery_limit_values_are_validated_before_serving -- --nocapture`
Expected: PASS.

**Step 5: Commit**

Run: `jj commit -m "feat(query): configure discovery bounds (#51)" -m "Closes #51"`.

### Task 2: Admission covers discovery stream lifetime

**Files:**
- Modify: `crates/lake-query/src/flight.rs`
- Test: `crates/lake-query/src/flight.rs`

**Step 1: Write the failing test**

Add `flight_discovery_admission_releases_on_stream_drop`: with one slot, retain
the first authenticated schema discovery response, assert the second times out
as `ResourceExhausted`, drop the first stream, and assert a third succeeds.

**Step 2: Run test to verify it fails**

Run: `cargo test -p lake-query flight_discovery_admission_releases_on_stream_drop -- --nocapture`
Expected: FAIL because discovery bypasses `QueryAdmission`.

**Step 3: Write minimal implementation**

Acquire after principal authentication in both discovery `DoGet` methods. Move
the permit and the existing execution deadline into `AdmittedFlightStream`.

**Step 4: Run test to verify it passes**

Run the focused test again. Expected: PASS.

**Step 5: Commit**

Run: `jj commit -m "fix(query): admit Flight discovery streams (#51)" -m "Closes #51"`.

### Task 3: Lazy bounded schema and table batches

**Files:**
- Modify: `crates/lake-query/src/flight.rs`
- Test: `crates/lake-query/src/flight.rs`

**Step 1: Write the failing tests**

Add `flight_table_discovery_streams_bounded_batches`,
`flight_schema_discovery_streams_bounded_batches`, and
`flight_discovery_stops_at_configured_row_limit`. Use tiny limits (2-row
batches, 4-row maximum), authenticated cached generations, and inspect decoded
batch sizes/order plus the raw tonic `ResourceExhausted` stream error.

**Step 2: Run tests to verify they fail**

Run: `cargo test -p lake-query 'flight_.*discovery_.*bounded\|flight_discovery_stops' -- --nocapture`
Expected: FAIL because each response is currently one eager batch and has no
row maximum.

**Step 3: Write minimal implementation**

Replace eager `build_table_discovery` and schema construction with internal
state machines owning `Arc<CatalogGeneration>`, principal/query filters,
cursors, emitted-row count, and `DiscoveryLimits`. Each stream poll builds one
Arrow metadata builder, appends at most `batch_rows`, and returns one batch.
Before appending match `max_rows + 1`, return
`Status::resource_exhausted("discovery row limit reached")`. Preserve the
existing auth/filter-before-schema ordering.

**Step 4: Run tests to verify they pass**

Run all three focused tests. Expected: PASS, with no batch above two rows and
the fifth match rejected.

**Step 5: Commit**

Run: `jj commit -m "perf(query): stream bounded discovery batches (#51)" -m "Closes #51"`.

### Task 4: Documentation and release gate

**Files:**
- Modify: `crates/lake-query/AGENT.md`
- Modify: `crates/lake-cli/AGENT.md`
- Modify: `docs/architecture.md`
- Modify: `docs/guides/cli.md`
- Create: `verification/issue-51-flight-discovery-bounds.md`

**Step 1: Document the limits**

Record shared admission, lazy bounded batches, default maximum/batch size,
environment overrides, and `ResourceExhausted` behavior.

**Step 2: Run task verification**

Run `cargo +nightly fmt --all -- --check`, strict clippy for query/cli,
`cargo test -p lake-query`, `cargo test -p lake-cli`, and
`mise run spec-lifecycle specs/issue-51-flight-discovery-bounds.spec.md`.
Expected: all pass; lifecycle reports 5/5.

**Step 3: Run repository gate**

Run: `mise run gate`
Expected: hooks, workspace tests, e2e, and site checks pass.

**Step 4: Independent review and verification**

Reviewer checks lazy cursor correctness, no false-negative filters, permit
lifetime, exact row/batch bounds, and no authority I/O. Verifier independently
runs selectors, strict clippy, boundary audit, and full gate.

**Step 5: Record evidence and commit**

Write the verification report only after APPROVE/PASS, then commit with
`jj commit -m "docs(query): record discovery bounds verification (#51)" -m "Closes #51"`.
