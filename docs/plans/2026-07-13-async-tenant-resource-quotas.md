# Async Tenant Resource Quotas Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Bound each tenant's durable outstanding asynchronous jobs and each
job's retained result bytes across Query replicas and worker restarts.

**Architecture:** AsyncQueryStore gains one bounded CAS tenant index addressed
by a domain-separated SHA-256 digest. Submission reserves capacity before
object upload; expired reservations reconcile by point-reading their query
records, and cleanup releases after objects/state disappear. Schema-v2 query
records persist the result ceiling and opaque reservation token, while the worker
uses that immutable value for streaming part admission.

**Tech Stack:** Rust, Tokio, SHA-256, serde JSON, MetaStore CAS, Arrow IPC
streaming, Flight SQL, DynamoDB/RocksDB, agent-spec, jj.

---

### Task 1: Freeze resource configuration and record compatibility

**Files:**
- Modify: crates/lake-query/src/async_query.rs
- Modify: crates/lake-query/src/lib.rs
- Test: crates/lake-query/src/async_query.rs

1. Write failing async_resource_v1_records_remain_compatible tests that
   deserialize representative schema-v1 queued/completed/cleaning records and
   assert no reservation plus the legacy hard result limit.
2. Write failing constructor tests for 1..=128 outstanding jobs and
   64 MiB..=256 GiB result bytes.
3. Run both selectors; require failure because schema-v2 resource fields and
   limits do not exist.
4. Add AsyncResourceLimits, schema-v2 optional/default-compatible fields,
   strict v1/v2 validation, and immutable accessors. Do not change ticket,
   object, part, manifest, or DataLocation encodings.
5. Run selectors, all async state-machine tests, fmt, and strict Query clippy.

### Task 2: Add the bounded durable tenant index

**Files:**
- Modify: crates/lake-query/Cargo.toml
- Modify: crates/lake-query/src/async_query.rs
- Test: crates/lake-query/src/async_query.rs

1. Write failing async_tenant_quota_is_durable_and_isolated and
   async_tenant_quota_reclaims_stale_reservations tests with two
   AsyncQueryStore instances over one RocksMeta.
2. Require concurrent same-tenant reservations to stop exactly at the limit,
   another tenant to succeed, stale missing owners to disappear, and an expired
   index entry with a live record to refresh to the record expiry.
3. Add domain-separated SHA-256 keys, versioned bounded index/entry DTOs, at
   most eight CAS attempts, a five-minute initial grace, and bounded point-read
   reconciliation. Return a distinct quota-exhausted store error.
4. Run both selectors repeatedly, the MetaStore Rocks/Dynamo CAS tests, fmt,
   git diff --check, and strict Query clippy.

### Task 3: Integrate reservation, cleanup, and idempotent submission

**Files:**
- Modify: crates/lake-query/src/async_query.rs
- Modify: crates/lake-query/src/flight.rs
- Modify: crates/lake-query/src/telemetry.rs
- Test: crates/lake-query/src/async_query.rs
- Test: crates/lake-query/src/flight.rs

1. Write failing async_cleanup_releases_exact_tenant_reservation plus a
   real Flight quota-status test. Include two jobs for one tenant and
   concurrent idempotent submission IDs.
2. Reserve before job object upload, create the schema-v2 record, then confirm
   the reservation through job expiry. On ambiguous failure retain capacity
   until bounded reconciliation rather than risking under-count.
3. After fenced object deletion and exact state deletion, CAS-remove only the
   cleaned query ID. Missing/already-released entries are idempotent.
4. Map exhaustion to ResourceExhausted and fixed quota-rejection metric reason
   `outstanding_jobs`; never attach identity or misclassify it as a scheduler event.
5. Run focused tests plus submission retry, cancellation, expiry cleanup,
   failover, telemetry-label, and Flight SQL suites.

### Task 4: Enforce the persisted result ceiling

**Files:**
- Modify: crates/lake-query/src/async_query.rs
- Test: crates/lake-query/src/async_query.rs

1. Write failing async_result_limit_is_immutable_across_worker_restart with
   a small persisted limit, multiple encoded batches, partial object
   instrumentation, and a second worker constructed under a larger limit.
2. Thread the record limit into write_part, manifest validation, and
   completion transition. Preserve 64-MiB part and 4,096-part hard ceilings.
3. Reject before starting an encoder when no bytes remain; validate returned
   DataLocation bytes and manifest total again before publication.
4. Run focused tests plus async IPC memory/backpressure, manifest, resume,
   deadline, cancellation, and SDK async roundtrip tests.

### Task 5: Wire CLI, Kubernetes, documentation, and verification

**Files:**
- Modify: crates/lake-cli/src/commands/limits.rs
- Modify: crates/lake-cli/src/commands/serve.rs
- Modify: crates/lake-cli/AGENT.md
- Modify: crates/lake-query/AGENT.md
- Modify: deploy/kubernetes/lake.yaml
- Modify: crates/lake-cli/tests/kubernetes_manifests.rs
- Modify: README.md
- Modify: docs/architecture.md
- Modify: docs/guides/kubernetes.md
- Create: verification/issue-126-async-tenant-resource-quotas.md

1. Add failing CLI boundary/default tests and require both new ConfigMap keys in
   the Kubernetes selector.
2. Parse LAKE_ASYNC_MAX_OUTSTANDING_PER_TENANT and
   LAKE_ASYNC_MAX_RESULT_BYTES before server bind; pass them through
   AsyncQueryConfig::try_with_resource_limits.
3. Document defaults, ranges, durable over-count-on-crash invariant, retained
   storage bound, non-goals, and identity-free telemetry.
4. Run all package suites, strict clippy, fmt, docs checks, and
   mise run spec-lifecycle specs/issue-126-async-tenant-resource-quotas.spec.md.

### Task 6: Review and ship

**Files:**
- Update: verification/issue-126-async-tenant-resource-quotas.md

1. Run mise run gate and record exact counts/timing.
2. Request independent correctness/security and release/ops reviews; resolve
   every P0/P1 and rerun affected selectors plus the full gate.
3. Commit feat(query): add durable async tenant resource quotas (#126),
   push, open the PR, merge only after APPROVE, and verify #126 closes.
