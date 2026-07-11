# Server-Authoritative Table Placement Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Ensure remote DDL cannot select or escape table storage locations; Metasrv derives every remote table location from trusted configuration.

**Architecture:** Add a storage-neutral `TablePlacement` domain value in `lake-metasrv`, inject it into the Flight control service through `MetasrvServerConfig`, and have both the CLI context and remote DDL share that policy. This keeps `lake-common` limited to thin shared identifiers while the metadata authority owns placement. Keep the existing explicit-location engine and trusted library APIs unchanged.

**Tech Stack:** Rust, Tokio, Arrow Flight/tonic, serde, clap, snafu, agent-spec.

---

### Task 1: Define safe placement as a domain value

**Files:**
- Create: `crates/lake-metasrv/src/placement.rs`
- Modify: `crates/lake-metasrv/src/lib.rs`
- Test: `crates/lake-metasrv/src/placement.rs`

1. Add failing tests for deterministic local/S3 derivation and every unsafe identifier class.
2. Run `cargo test -p lake-metasrv table_placement_` and confirm the tests fail because the API is absent.
3. Implement `TablePlacement`, explicit validation, and a typed `PlacementError` without storage I/O.
4. Run the focused tests and rustdoc for `lake-metasrv`.

### Task 2: Make Flight create-table use server placement

**Files:**
- Modify: `crates/lake-metasrv/src/lib.rs`
- Modify: `crates/lake-metasrv/src/control.rs`
- Modify: `crates/lake-metasrv/tests/two_node_forwarding.rs`
- Test: `crates/lake-metasrv/src/control.rs`

1. Add failing Flight tests proving location derivation and rejection of the legacy `location` field before mutation.
2. Run the two named `lake-metasrv` tests and record the red result.
3. Extend `MetasrvServerConfig` with explicit placement and thread it into `MetasrvFlightService`.
4. Remove location from `CreateTableReq`, reject unknown fields, derive the trusted location, and map placement errors to `invalid_argument`.
5. Update all server constructors and forwarding tests, then run the focused tests.

### Task 3: Unify CLI placement and remove remote location input

**Files:**
- Modify: `crates/lake-cli/src/main.rs`
- Modify: `crates/lake-cli/src/commands/mod.rs`
- Modify: `crates/lake-cli/src/commands/client.rs`
- Modify: `crates/lake-cli/src/commands/serve.rs`
- Modify: `crates/lake-cli/src/commands/table.rs`
- Modify: `crates/lake-cli/src/commands/ingest.rs`
- Test: relevant CLI command modules

1. Add a failing clap contract test for location-free remote creation and rejection of `--location`.
2. Replace CLI-local placement string construction with the common policy.
3. Pass the policy into Metasrv serving and remove `--location` from the remote client payload and arguments.
4. Run `cargo test -p lake-cli remote_create_table_has_no_location_argument` and the full CLI crate tests.

### Task 4: Document and verify the contract

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `specs/issue-31-server-table-placement.spec.md`
- Create: `verification/issue-31-server-table-placement.md`

1. Update remote DDL examples and document the server-authoritative placement boundary.
2. Run `mise run spec-lint specs/issue-31-server-table-placement.spec.md`.
3. Run `mise run spec-lifecycle specs/issue-31-server-table-placement.spec.md`.
4. Run `mise run gate` and the production LocalStack suite.
5. Record exact commands and outcomes in the verification report.

### Task 5: Review, publish, and merge

1. Commit with `refactor(storage): make table placement server-authoritative (#31)` and `Closes #31`.
2. Obtain independent correctness/security and verification verdicts.
3. Push the bookmark, open the PR, watch all CI checks, and merge only after they pass.
