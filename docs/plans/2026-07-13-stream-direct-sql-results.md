# Stream Direct SQL Results Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make direct `QueryEngine` SQL and local CLI consumption retain only
the currently processed `RecordBatch`, rather than collecting an entire query
result.

**Architecture:** Preserve `QueryEngine`'s catalog refresh, read-only planning,
and DataFusion runtime. Change `execute_sql` to return DataFusion's
`SendableRecordBatchStream` from `DataFrame::execute_stream`; callers own
progress and naturally apply backpressure by polling the next batch. The local
CLI and selftest render/inspect each batch immediately, so their live result
memory is bounded by the DataFusion batch and formatter rather than result
cardinality.

**Tech Stack:** Rust, Tokio, Futures streams, DataFusion record-batch streams,
Arrow pretty formatter, jj, agent-spec.

---

### Task 1: Lock the direct-query streaming contract

**Files:**
- Modify: `crates/lake-query/src/lib.rs: QueryEngine::execute_sql`
- Test: `crates/lake-query/src/lib.rs: direct_sql_results_stream_before_source_completion`

**Step 1: Write the failing test**

Reuse the existing `ShutdownPartition` test provider: it returns one batch,
then waits on `Notify` before yielding the second. Start with a timeout around
`execute_sql` itself, which compiles against both the old collected type and
the intended stream type. The old implementation must time out because it
collects through the blocked second batch. Once the stream return is in place,
expand that same test to pull and assert the first batch, prove the second poll
remains blocked, then release and assert the second batch.

**Step 2: Verify RED**

Run:

```bash
cargo test -p lake-query direct_sql_results_stream_before_source_completion --lib
```

Expected: the assertion fails because the old `collect` implementation does
not return before the blocked source completes.

**Step 3: Implement the minimal stream return**

- Import `datafusion::physical_plan::SendableRecordBatchStream` in production
  code instead of importing `RecordBatch` solely for the collected return.
- Keep the `execute_sql` name but change its return type to
  `Result<SendableRecordBatchStream>`.
- Plan with the existing `plan_sql`, then call `DataFrame::execute_stream()`
  and map its error through `ExecuteSnafu`.
- Do not add a buffer, spawned drain task, or compatibility method that calls
  `collect`.

**Step 4: Verify GREEN**

Run the selector from step 2 and:

```bash
cargo test -p lake-query public_sql_surface_is_read_only --lib
```

Expected: first-batch streaming passes, and DDL/DML remains rejected during
planning before a stream is returned.

### Task 2: Migrate direct consumers to batch-at-a-time ownership

**Files:**
- Modify: `crates/lake-cli/src/commands/sql.rs`
- Modify: `crates/lake-cli/src/commands/selftest.rs`
- Modify: `crates/lake-query/src/lib.rs` tests
- Modify: `crates/lake-sdk/src/lib.rs` test-only direct-engine usage

**Step 1: Update the CLI command**

Replace the collected `batches` value with a mutable stream and consume it via
`TryStreamExt::try_next`. For each batch, call
`pretty_format_batches(&[batch])` and print it immediately. This deliberately
formats one batch per table frame rather than retaining prior batches to build
a global layout.

**Step 2: Update selftest**

Consume the aggregate-query stream in the same loop. Print each formatted
batch, add `batch.num_rows()` to the existing row counter, and keep the final
aggregate assertion. No `Vec<RecordBatch>` may be constructed.

**Step 3: Update in-process tests**

- Tests that only need planning/cache behavior should request a stream and
  drop it; catalog refresh happens during `plan_sql`.
- Tests that inspect a row (including the SDK `DataLocation` test) should pull
  only the needed first batch with `try_next`.
- Keep collection only inside test helpers where a test explicitly asserts
  multiple complete batches; production direct consumers must not collect.

**Step 4: Verify consumer migration**

Run:

```bash
cargo test -p lake-query --lib
cargo test -p lake-sdk
cargo test -p lake-cli
mise run e2e
```

Expected: all direct callers compile, selftest still completes ingest → SQL,
and no production call site invokes `DataFrame::collect` for direct SQL.

### Task 3: Bind the task contract and record evidence

**Files:**
- Modify: `specs/issue-128-stream-direct-sql-results.spec.md`
- Create: `verification/issue-128-stream-direct-sql-results.md`

**Step 1: Complete the spec**

State that the streaming test uses a blocked second batch, retains the
read-only scenario, and forbids Flight/async/object wire-format changes.

**Step 2: Record red/green evidence**

Record the initial compile failure from Task 1, the first-batch timing result,
consumer migration commands, and the full quality gate.

**Step 3: Final verification**

Run:

```bash
mise run spec-lint specs/issue-128-stream-direct-sql-results.spec.md
mise run spec-lifecycle specs/issue-128-stream-direct-sql-results.spec.md
mise run gate
git diff --check
```

Expected: every scenario selector executes at least one passing test and the
workspace has no formatting, lint, test, E2E, site, or whitespace error.
