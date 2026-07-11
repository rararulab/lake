# Managed Stage Discovery Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make `LakeClient::connect(query_endpoint)` discover and construct the managed `FILE` stage while preserving direct SDK-to-storage object I/O.

**Architecture:** Define a versioned credential-free descriptor in `lake-common`. Query exposes it through one immutable custom Flight action configured at startup; the SDK performs discovery once during connection and creates a local or S3 `ManagedObjectStore`, using the AWS default credential chain for S3. The explicit injected-store constructor remains available for tests and embedding.

**Tech Stack:** Rust 2024, Arrow Flight SQL, tonic, serde JSON, Tokio, AWS SDK for Rust, LocalStack, Snafu.

---

### Task 1: Define the wire descriptor

**Files:**
- Create: `crates/lake-common/src/managed_stage.rs`
- Modify: `crates/lake-common/src/lib.rs`
- Modify: `crates/lake-common/Cargo.toml`

**Step 1:** Write `managed_stage_descriptors_roundtrip_without_credentials` and `managed_stage_rejects_unsupported_protocol_version` against wished-for constructors and JSON wire methods.

**Step 2:** Run the focused test; expect unresolved descriptor/action symbols.

**Step 3:** Implement protocol version 1, tagged backend variants, action constant, and JSON encode/decode error handling without secret fields.

**Step 4:** Re-run lake-common tests; expect PASS.

**Step 5:** Commit `feat(common): define managed stage discovery protocol (#11)`.

### Task 2: Serve discovery from query

**Files:**
- Modify: `crates/lake-query/Cargo.toml`
- Modify: `crates/lake-query/src/flight.rs`
- Modify: `crates/lake-query/src/lib.rs`

**Step 1:** Write `managed_stage_action_returns_configured_descriptor` against `do_action_fallback`, including unknown-action and single-result assertions.

**Step 2:** Run the focused test; expect missing service configuration/action implementation.

**Step 3:** Add immutable optional descriptor state, custom action listing/fallback, and `serve_with_metadata_and_stage`; preserve existing serve helpers.

**Step 4:** Run all lake-query tests; expect PASS.

**Step 5:** Commit `feat(query): serve managed stage discovery (#11)`.

### Task 3: Make query-only connect the SDK default

**Files:**
- Modify: `crates/lake-sdk/Cargo.toml`
- Modify: `crates/lake-sdk/src/lib.rs`

**Step 1:** Add `client_discovers_local_stage_from_query`, exercising insert/query/full open/range open with only a query endpoint.

**Step 2:** Run the test; expect the one-argument constructor and discovery path to be absent.

**Step 3:** Rename the injected constructor, add one-time Flight discovery, validate protocol version/result cardinality, and asynchronously construct local/S3 stores.

**Step 4:** Update existing tests to the explicit constructor and run the complete SDK suite.

**Step 5:** Commit `feat(sdk): discover managed stage from query (#11)`.

### Task 4: Wire process configuration and S3 proof

**Files:**
- Modify: `crates/lake-cli/src/commands/mod.rs`
- Modify: `crates/lake-cli/src/commands/serve.rs`
- Modify: `crates/lake-sdk/src/lib.rs`
- Modify: `crates/lake-sdk/examples/managed_file.rs`

**Step 1:** Add CLI configuration tests and ignored `sdk_discovers_s3_stage_and_streams_directly_localstack` plus its integration wiring test.

**Step 2:** Run focused tests; expect query startup and SDK fixtures not to provide descriptors.

**Step 3:** Derive local/S3 descriptors in `Context`, use the stage-aware query server, and update the public example to one-argument connect.

**Step 4:** Run local example and full LocalStack integration; expect PASS.

**Step 5:** Commit `feat(cli): wire managed stage discovery configuration (#11)`.

### Task 5: Document, verify, and publish

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/design/managed-objects.md`
- Modify: crate `AGENT.md` cards
- Create: `verification/issue-11-stage-discovery.md`

**Step 1:** Document the primary query-only API, descriptor security boundary, AWS workload identity, and explicit constructor.

**Step 2:** Run clippy, spec lifecycle, LocalStack integration, public example, and `mise run gate`.

**Step 3:** Record RED/GREEN evidence, rebase onto latest main, push, open PR, verify checks, and merge.
