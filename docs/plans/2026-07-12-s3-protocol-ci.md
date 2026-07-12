# Managed S3 Protocol CI Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make local and GitHub CI run the same ignored managed-S3 protocol suite, including a real presigned Range GET.

**Architecture:** `scripts/test-integration.ts` remains the single owner of the four-package nextest invocation. Its default mode owns checkout-scoped Docker lifecycle; `--external` consumes CI's existing LocalStack service and never starts or stops it. A `lake-objects` ignored test exercises the full HTTP capability against LocalStack, while non-ignored wiring selectors lock the runner and workflow together.

**Tech Stack:** Rust 2024, AWS SDK for Rust, Tokio, reqwest (test-only), LocalStack, Bun, GitHub Actions, cargo-nextest.

---

### Task 1: Lock the desired integration wiring with failing tests

**Files:**
- Modify: `crates/lake-objects/tests/s3_localstack.rs`

**Step 1: Write the failing tests**

Add `s3_presigned_range_get_localstack_is_wired`, which requires the ignored
protocol test name and CI external-runner marker, and
`managed_s3_integration_runner_is_shared_with_ci`, which requires all four
packages in the script and requires the workflow to call that script with
`--external` instead of a duplicated `cargo nextest` command.

**Step 2: Verify RED**

Run these selectors separately because Cargo filters are substrings, not
regular expressions:

- `cargo test -p lake-objects s3_presigned_range_get_localstack_is_wired`
- `cargo test -p lake-objects managed_s3_integration_runner_is_shared_with_ci`

Expected: both new selectors fail because CI does not invoke the shared runner
and the protocol test does not yet exist.

### Task 2: Add external LocalStack runner mode

**Files:**
- Modify: `scripts/test-integration.ts`
- Modify: `mise.toml`
- Modify: `.github/workflows/ci.yml`
- Modify: `scripts/AGENT.md`
- Modify: `docs/guides/mise-ci.md`

**Step 1: Implement the minimal shared runner**

Extract the existing `cargo nextest` spawn into one function. In default mode,
retain `test-env.ts up`/`down`; in `--external` mode, require the existing
`LAKE_DYNAMODB_ENDPOINT` and `LAKE_S3_ENDPOINT` environment and only run the
suite. Preserve proxy exclusions in both modes.

**Step 2: Point CI at the shared runner**

Expose `bun scripts/test-integration.ts --external` as the named mise task
`test-integration-external`, then replace the duplicated workflow package
command with `mise run test-integration-external`. Update runner documentation.

**Step 3: Verify GREEN**

Run the two Task 1 selectors. Expected: both pass.

**Step 4: Commit**

Commit the spec, plan, runner, workflow, docs, and wiring tests as one coherent
CI contract change.

### Task 3: Prove presigned Range GET against LocalStack

**Files:**
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Modify: `crates/lake-objects/Cargo.toml`
- Modify: `crates/lake-objects/tests/s3_localstack.rs`

**Step 1: Write the ignored protocol test**

Add `s3_presigned_range_get_localstack`: upload deterministic bytes through
`S3ObjectStore`, mint a 60-second capability, copy all required headers into a
reqwest GET, add `Range: bytes=100-199`, and assert HTTP 206 plus the exact 100
bytes. Use a no-proxy test client.

**Step 2: Verify RED**

Run the ignored selector against LocalStack before adding the direct reqwest
dependency. Expected: compilation fails because the HTTP test client is not a
declared dependency.

**Step 3: Implement the minimal dependency change**

Add reqwest as a workspace dependency and a `lake-objects` dev-dependency with
only the features required for this HTTP test.

**Step 4: Verify GREEN**

Run `mise run test-integration`. Expected: all ignored tests in all four
packages pass, including the new HTTP Range test.

### Task 4: Full verification and review

**Files:**
- Verify all allowed files only

**Step 1: Run focused quality checks**

Run strict clippy for `lake-objects`, the spec lifecycle, and `git diff --check`.

**Step 2: Run the full gate**

Run `mise run gate`. Expected: success.

**Step 3: Fix one candidate revision and obtain independent review**

Commit any final fixes, keep the workspace clean, then ask reviewer and verifier
to evaluate the same commit. Address findings and repeat verification without
revision drift.

**Step 4: Publish and merge**

Push the jj bookmark, open a PR closing #81, merge after APPROVE/PASS, and
confirm the issue is closed.
