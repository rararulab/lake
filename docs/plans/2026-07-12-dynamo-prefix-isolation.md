# Dynamo prefix isolation implementation plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Replace mixed-table Dynamo scans with strongly consistent, sharded
prefix queries while migrating existing v1 tables without losing CAS or fence
semantics.

**Architecture:** Keep logical `MetaStore` keys unchanged. Add a companion v2
table keyed by `(family#shard, full_key)`, atomically dual-write it with v1,
backfill old records in bounded pages, and switch reads only after an exact
verified completion marker.

**Tech Stack:** Rust 2024, async-trait, AWS SDK for DynamoDB transactions and
strongly consistent Query, SHA-256 stable sharding, Clap, LocalStack.

---

### Task 1: Lock the physical-key and cursor model

**Files:**
- Create: `crates/lake-meta/src/dynamo_layout.rs`
- Modify: `crates/lake-meta/src/lib.rs`
- Test: `crates/lake-meta/src/dynamo_layout.rs`

1. Add RED tests for family parsing, 64-way stable shard derivation, full-key
   preservation, cursor version/family binding, invalid cursor rejection, and
   shard advancement.
2. Run `cargo test -p lake-meta dynamo_v2_layout_is_stable_and_family_isolated`
   and confirm the module/API is absent.
3. Implement `PhysicalKey { bucket, logical_key }` and a versioned cursor that
   contains family, prefix digest, shard, and optional last key.
4. Run the focused tests and `cargo clippy -p lake-meta --all-targets -- -D warnings`.

### Task 2: Add the strongly consistent v2 backend primitives

**Files:**
- Modify: `crates/lake-meta/src/dynamo.rs`
- Modify: `crates/lake-meta/src/error.rs`
- Test: `crates/lake-meta/tests/dynamo_localstack.rs`

1. Extend `DynamoMeta` with legacy/v2 table names and create the v2 HASH+RANGE
   table idempotently.
2. Add point `GetItem`, conditional put/delete, and `Query` helpers using
   `consistent_read(true)` and key conditions rather than filters.
3. Make one prefix-page call query only the cursor shard with Dynamo `Limit`;
   when a shard is exhausted, return a cursor for the next shard without
   evaluating another family.
4. Add LocalStack tests with many unrelated keys and consumed-capacity/evaluated
   count assertions; prove size-one pages cross shards exactly once.
5. Run the two prefix selectors with the checkout-scoped LocalStack runner.

### Task 3: Preserve CAS and guarded mutation under dual write

**Files:**
- Modify: `crates/lake-meta/src/dynamo.rs`
- Test: `crates/lake-meta/tests/dynamo_localstack.rs`

1. Add RED tests for dual create/update/delete CAS and stale expected bytes.
2. Build cross-table `TransactWriteItems` operations that condition the v1
   authority and require v2 to be absent or exact before writing both copies.
3. Expand guarded mutations to condition the exact v1 guard/target and update
   both target copies in the same transaction; any unknown cancellation reason
   remains an error, not a false conflict.
4. Prove stale guard and concurrent target changes move neither copy.

### Task 4: Implement bounded idempotent backfill and verification

**Files:**
- Create: `crates/lake-meta/src/dynamo_migration.rs`
- Modify: `crates/lake-meta/src/dynamo.rs`
- Modify: `crates/lake-meta/src/lib.rs`
- Test: `crates/lake-meta/tests/dynamo_localstack.rs`

1. Define a migration page result with scanned count, copied count, conflicts,
   and durable continuation.
2. Scan at most the configured v1 evaluated-item limit. For each record,
   conditionally create v2; equal existing bytes converge, differing bytes
   trigger a strongly consistent v1 reload and retry without stale overwrite.
3. Persist the migration cursor only after every item in the page converges.
4. Verify v1/v2 exact key/value parity in bounded passes and CAS-publish the
   completion marker only from an exact verified generation.
5. Add crash/replay and concurrent dual-writer tests.

### Task 5: Add the explicit migration command and startup modes

**Files:**
- Create: `crates/lake-cli/src/commands/dynamo_migrate.rs`
- Modify: `crates/lake-cli/src/commands/mod.rs`
- Modify: `crates/lake-cli/src/main.rs`
- Modify: `crates/lake-cli/src/commands/limits.rs`
- Test: `crates/lake-cli/src/main.rs`

1. Add `lake dynamo-migrate --page-size <N> [--finalize] --json`, dispatched
   before normal `Context` construction.
2. Validate endpoint/table/layout configuration and finite page bounds before
   network mutation.
3. Make normal cloud startup select v1 authority before the marker and v2
   authority after it; dual writes remain enabled through the rollback horizon.
4. Refuse `--finalize` without an explicit rollout acknowledgement and exact
   verification result.

### Task 6: Document rollout and prove the complete lifecycle

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/guides/kubernetes.md`
- Modify: `docs/guides/local-deploy.md`
- Modify: `deploy/kubernetes/lake.yaml`
- Create: `verification/issue-94-dynamo-prefix-isolation.md`

1. Document v1→dual→backfill→finalize→v2 rollout, rollback limits, metrics,
   and the prohibition on v1-only writers after finalization.
2. Add Kubernetes environment/config examples without embedding credentials.
3. Run `mise run spec-lifecycle specs/issue-94-dynamo-prefix-isolation.spec.md`.
4. Run all lake-meta tests, LocalStack ignored tests, strict clippy, full
   `mise run gate`, and `mise run doc`.
5. Freeze one commit, obtain independent correctness and release verification,
   open a PR closing #94, and merge only after both approve.
