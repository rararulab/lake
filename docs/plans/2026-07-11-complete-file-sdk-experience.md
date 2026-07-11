# Complete FILE SDK Experience Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Complete the public Rust experience so a user can create a `FILE` column, insert a large object, query its `DataLocation` through the SDK, and open it directly without importing internal Lake crates.

**Architecture:** Keep SQL statement execution read-only and stream query results through Arrow Flight. Add `file` to the existing administrative schema DSL, represented by the same Arrow `DataLocation` struct already stored in Lance. The SDK owns Flight query execution and `DataLocation` decoding while raw object bytes continue to bypass query and metadata services.

**Tech Stack:** Rust 2024, Tokio, Arrow/Arrow Flight SQL, Tonic, clap, Lance, Snafu.

---

### Task 1: Expose `FILE` in local and remote table DDL

**Files:**
- Modify: `crates/lake-cli/Cargo.toml`
- Modify: `crates/lake-cli/src/commands/table.rs`
- Modify: `crates/lake-cli/src/commands/client.rs`
- Modify: `crates/lake-metasrv/Cargo.toml`
- Modify: `crates/lake-metasrv/src/control.rs`
- Test: `crates/lake-cli/src/commands/table.rs`
- Test: `crates/lake-metasrv/src/control.rs`

**Step 1: Write the failing tests**

Add `local_schema_dsl_accepts_file` and `remote_schema_dsl_accepts_file`.
Each parses `video:file` and asserts equality with
`lake_objects::data_location_field("video", false)`.

**Step 2: Run tests to verify RED**

Run: `cargo test -p lake-cli local_schema_dsl_accepts_file`

Run: `cargo test -p lake-metasrv remote_schema_dsl_accepts_file`

Expected: both fail because `file` is an unknown schema type.

**Step 3: Implement the minimum schema mapping**

Add the `lake-objects` dependency to CLI and metasrv. Map `file` to the
non-nullable Arrow struct field returned by `data_location_field`; keep the
existing scalar mappings and error behavior.

**Step 4: Run tests to verify GREEN**

Run both focused commands again; expected PASS.

**Step 5: Commit**

`jj commit -m "feat(objects): expose FILE in table DDL (#4)"`

### Task 2: Add streaming SDK queries and DataLocation decoding

**Files:**
- Modify: `crates/lake-sdk/src/lib.rs`
- Test: `crates/lake-sdk/src/lib.rs`

**Step 1: Write the failing acceptance test**

Add `sdk_queries_datalocation_and_opens_file`. Use only public SDK methods
after fixture startup: `insert`, `query`, `data_location`, and `open`. Consume
the Flight stream with `try_next`, decode the `video` column, and compare the
direct reader bytes with the source file.

**Step 2: Run test to verify RED**

Run: `cargo test -p lake-sdk sdk_queries_datalocation_and_opens_file`

Expected: compile failure because `LakeClient::query` and the SDK
`data_location` helper do not exist.

**Step 3: Implement the minimum streaming API**

Add `LakeClient::query(&str) -> Result<FlightRecordBatchStream>`. Execute the
read-only statement with `FlightSqlServiceClient`, require exactly one usable
endpoint ticket, and call Flight `DoGet`. Add a public `data_location` helper
that resolves a named `StructArray` column and delegates to the existing
Arrow decoder. Add contextual SDK errors for missing endpoint/ticket/column
and object-value decoding.

**Step 4: Run test to verify GREEN**

Run the focused SDK test; expected PASS.

**Step 5: Commit**

`jj commit -m "feat(sdk): stream SQL queries and decode FILE values (#4)"`

### Task 3: Make the public example use only the SDK data path

**Files:**
- Modify: `crates/lake-sdk/examples/managed_file.rs`
- Modify: `README.md`
- Modify: `docs/design/managed-objects.md`
- Modify: `crates/lake-sdk/AGENT.md`
- Modify: `specs/issue-4-managed-objects.spec.md`

**Step 1: Add a source-level regression assertion**

Add `managed_file_example_queries_through_sdk`, which reads the example source
with `include_str!` and requires `.query(` plus `data_location(` while rejecting
direct `QueryEngine::execute_sql` use in the user path.

**Step 2: Run test to verify RED**

Run: `cargo test -p lake-sdk managed_file_example_queries_through_sdk`

Expected: FAIL because the example still queries through `QueryEngine`.

**Step 3: Update example and docs**

Keep embedded query/metasrv processes only as deployment fixtures. Perform
insert, select, `DataLocation` decode, and direct open through `LakeClient`.
Document `file` DDL and the streaming SDK query API.

**Step 4: Run example and test to verify GREEN**

Run: `cargo test -p lake-sdk managed_file_example_queries_through_sdk`

Run: `cargo run -p lake-sdk --example managed_file`

Expected: PASS and direct-read success output.

**Step 5: Commit**

`jj commit -m "docs(objects): complete the SDK FILE walkthrough (#4)"`

### Task 4: Verify and publish the completed vertical slice

**Files:**
- Modify: `verification/issue-4-managed-objects.md`

**Step 1: Run focused quality checks**

Run: `cargo test -p lake-cli -p lake-metasrv -p lake-sdk`

Run: `cargo clippy -p lake-cli -p lake-metasrv -p lake-sdk --all-targets -- -D warnings`

**Step 2: Run governed gates**

Run: `mise run spec-lint specs/issue-4-managed-objects.spec.md`

Run: `mise run spec-lifecycle specs/issue-4-managed-objects.spec.md`

Run: `mise run gate`

**Step 3: Record evidence and publish**

Append RED/GREEN and final gate evidence to the issue verification report,
commit it, move `issue-4-managed-objects`, push, and watch PR #5 CI to a
terminal result. Do not merge without user confirmation.
