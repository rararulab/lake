# Configurable Lance Retention Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make Lance's count-based snapshot retention an operator-configurable, startup-validated production policy.

**Architecture:** `lake-engine-lance` owns a small immutable `LanceMaintenancePolicy` because retention is specific to that backend and must not leak through the swappable engine trait. `lake-cli` parses `LAKE_LANCE_RETAIN_VERSIONS` before opening any storage and supplies the same policy to local and cloud constructors. `maintain` remains compaction → Lance cleanup → external manifest reconciliation.

**Tech Stack:** Rust 2024, Lance 8 cleanup policies, SNAFU, Tokio tests, clap application boundary, agent-spec.

---

### Task 1: Bind the policy contract with failing engine tests

**Files:**
- Modify: `crates/lake-engine-lance/src/lib.rs`

**Step 1:** Add `maintenance_policy_rejects_unbounded_retention`, expressing the wished-for `LanceMaintenancePolicy::try_new`, accessors, maximum, and default API.

**Step 2:** Run `cargo test -p lake-engine-lance maintenance_policy_rejects_unbounded_retention` and confirm RED because the policy type does not exist.

**Step 3:** Add `maintenance_uses_configured_version_retention` using a real temporary Lance dataset, more versions than the configured window, `LanceEngine::with_maintenance_policy`, and `Dataset::versions()` after maintenance.

**Step 4:** Re-run that selector and confirm RED because the engine builder and policy plumbing do not exist.

### Task 2: Implement the immutable engine policy

**Files:**
- Modify: `crates/lake-engine-lance/Cargo.toml`
- Modify: `crates/lake-engine-lance/src/lib.rs`

**Step 1:** Add the workspace SNAFU dependency and a public typed configuration error.

**Step 2:** Implement `LanceMaintenancePolicy` with default 10, maximum 10000, `try_new`, and `retained_versions`.

**Step 3:** Store the policy in `WriteConfig`, add consuming `LanceEngine::with_maintenance_policy`, and replace `RETAIN_VERSIONS` in `maintain` with the configured value.

**Step 4:** Run both engine selectors and all `lake-engine-lance` tests; expect PASS.

### Task 3: Validate and wire CLI configuration before storage

**Files:**
- Modify: `crates/lake-cli/src/commands/limits.rs`
- Modify: `crates/lake-cli/src/commands/mod.rs`

**Step 1:** Add failing `lance_retention_values_are_validated_before_storage_open` cases for missing, valid, zero, 10001, numeric overflow, and non-numeric input.

**Step 2:** Run the selector and confirm RED because the parser does not exist.

**Step 3:** Implement `lance_maintenance_policy_from_env/from_value`, parse once at the start of `Context::open`, and pass it into both local and cloud engine construction.

**Step 4:** Re-run the CLI selector and package tests; expect PASS.

### Task 4: Document the operator contract

**Files:**
- Modify: `README.md`
- Modify: `crates/lake-engine-lance/AGENT.md`
- Modify: `crates/lake-cli/AGENT.md`
- Modify: `docs/architecture.md`
- Modify: `docs/guides/cli.md`

**Step 1:** Document `LAKE_LANCE_RETAIN_VERSIONS`, its default/range, tagged-version behavior, and startup failure semantics.

**Step 2:** Remove the obsolete fixed-retention ponytail and record the new immutable-policy invariant.

**Step 3:** Run rustfmt, clippy for the two changed crates, and `git diff --check`.

### Task 5: Lifecycle verification and delivery

**Files:**
- Create: `verification/issue-85-configurable-lance-retention.md`

**Step 1:** Run `mise run spec-lint specs/issue-85-configurable-lance-retention.spec.md`; expect score at least 0.7.

**Step 2:** Run `mise run spec-lifecycle specs/issue-85-configurable-lance-retention.spec.md`; expect all three selectors to execute at least one passing test.

**Step 3:** Commit the fixed candidate, run `mise run gate` and strict docs, then obtain independent correctness review and verifier PASS.

**Step 4:** Push the fixed candidate, open a PR closing #85, and merge only after all evidence is green.
