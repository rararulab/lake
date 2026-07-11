# Lease-Fenced Mutations Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Provide an atomic metastore mutation primitive guarded by a monotonic metadata lease epoch, so stale leaders cannot publish after takeover.

**Architecture:** Extend the backend-neutral `MetaStore` contract with a fail-closed guarded mutation, implement it atomically in RocksDB and DynamoDB, then extend Metasrv election records with a backward-compatible epoch. Metasrv mutation integration remains a separate follow-up so this PR is a reviewable durable foundation.

**Tech Stack:** Rust, async-trait, RocksDB WriteBatch, AWS SDK DynamoDB TransactWriteItems, serde, snafu, LocalStack.

---

### Task 1: Specify guarded mutation behavior

**Files:** `crates/lake-meta/src/store.rs`, `crates/lake-meta/src/rocks.rs`, `crates/lake-meta/src/error.rs`

1. Add failing Rocks tests for current-guard create/update/delete and stale-guard no-op behavior.
2. Add the backend-neutral guarded mutation signature and typed unsupported/invalid errors.
3. Implement Rocks atomically under the existing writer lock with a write batch.
4. Run the two focused `lake-meta` tests and strict Clippy.

### Task 2: Implement the Dynamo transaction

**Files:** `crates/lake-meta/src/dynamo.rs`, `crates/lake-meta/tests/dynamo_localstack.rs`

1. Add an ignored LocalStack behavior test plus a non-ignored wiring test.
2. Implement `ConditionCheck` plus conditional `Put`/`Delete` in one `TransactWriteItems` call.
3. Distinguish conditional mismatch from retryable/backend cancellation.
4. Run the wiring test and `mise run test-integration`.

### Task 3: Add monotonic lease epochs

**Files:** `crates/lake-metasrv/src/election.rs`, `crates/lake-metasrv/tests/two_node_forwarding.rs`

1. Add a failing legacy/new lease test covering acquire, renew, takeover, and exhaustion.
2. Add serde-default epoch 0 compatibility and fail-closed checked increment.
3. Preserve epoch through resign and expose it in `LeaseStatus::Leader`.
4. Run focused election and two-node tests.

### Task 4: Document and verify

**Files:** `docs/architecture.md`, `specs/issue-33-lease-fenced-mutations.spec.md`, `verification/issue-33-lease-fenced-mutations.md`

1. Document the guarded mutation foundation and explicit remaining integration boundary.
2. Run spec lint/lifecycle with the jj candidate change set.
3. Run strict Clippy, full clean gate, rustdoc, and LocalStack integration.
4. Obtain independent reviewer APPROVE and verifier PASS before publishing.
