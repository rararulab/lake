spec: task
name: "query-memory-spill"
inherits: project
tags: [query, datafusion, memory, spill]
---

## Intent

Lake's north star requires stateless Query replicas to absorb large embodied-AI
scans without making process survival depend on input size. Today Query uses
DataFusion's unbounded default runtime. Reproducer: start one Query replica,
run a memory-intensive sort or hash operation over data larger than available
RAM, and observe that admission/deadline limits do not stop DataFusion from
exhausting the process before the result stream can apply backpressure.

DataFusion 53 already provides `FairSpillPool`, `RuntimeEnvBuilder`, and a
size-limited `DiskManager`; Lake must configure those upstream mechanisms, not
build a second allocator or spill format. Issue #17 explicitly left
DataFusion memory-pool/spill tuning out of scope, so this issue completes that
previously deferred boundary rather than reversing an earlier decision.

## Decisions

- Add immutable `QueryResources` configuration separate from request-level
  `QueryLimits`.
- Use one process-wide DataFusion `FairSpillPool` shared by all sessions and
  concurrent queries in the replica.
- Use an explicit operator-owned spill root and DataFusion's disk manager with
  a hard aggregate byte limit; DataFusion owns randomized child directories
  and removes them when the runtime is dropped.
- Carry a minimal vendored patch for DataFusion 53.1's spill-quota accounting:
  reserve growth atomically before accepting it so an over-budget write cannot
  poison the shared runtime's disk counter. Remove the patch when the pinned
  Lance/DataFusion line contains the upstream fix.
- Keep `QueryEngine::new` as a bounded-default convenience constructor and add
  a fallible constructor for deployment configuration/startup validation.
- Configure Rust and `lake query` first through environment variables; do not
  introduce a new wire protocol or materialize query results in memory.

## Boundaries

### Allowed Changes
crates/lake-query/**
crates/lake-cli/**
third_party/datafusion-execution/**
Cargo.toml
Cargo.lock
docs/architecture.md
docs/guides/local-deploy.md
docs/design/sql-api-over-s3.md
docs/plans/2026-07-11-query-memory-spill.md
specs/issue-39-query-memory-spill.spec.md
verification/issue-39-query-memory-spill.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-engine/**
crates/lake-engine-lance/**
crates/lake-sdk/**

## Completion Criteria

Scenario: Invalid resource budgets fail before serving
  Test:
    Package: lake-query
    Filter: query_resources_reject_invalid_budgets
  Given a zero memory budget, zero spill budget, or unusable spill root
  When Query constructs its DataFusion runtime
  Then construction returns a typed startup error instead of an unbounded fallback

Scenario: Query uses one bounded fair runtime
  Test:
    Package: lake-query
    Filter: query_engine_uses_bounded_fair_spill_runtime
  Given valid memory, spill, and directory budgets
  When a QueryEngine is constructed
  Then its shared DataFusion runtime reports the finite memory and spill limits

Scenario: A memory-intensive query spills and cleans up
  Test:
    Package: lake-query
    Filter: memory_intensive_sort_spills_and_cleans_up
  Given a sort larger than the configured in-memory execution pool
  When DataFusion executes it through the Query runtime
  Then execution spills under the configured root, returns correct sorted rows, and releases memory and spill files

Scenario: Exceeding spill quota does not poison the shared runtime
  Test:
    Package: lake-query
    Filter: spill_budget_error_does_not_poison_runtime
  Given a query whose external sort exceeds the aggregate spill budget
  When DataFusion returns a resource error and drops the failed query
  Then disk accounting and files return to zero and a later query on the same runtime succeeds

Scenario: Deployment values are parsed before Query startup
  Test:
    Package: lake-cli
    Filter: query_resource_values_are_validated_before_serving
  Given memory bytes, spill bytes, and a spill directory from the environment boundary
  When lake-cli constructs Query resources
  Then valid values are preserved and zero or malformed values are rejected

## Out of Scope

- Per-tenant memory accounting, scan-byte limits, result-byte limits, or egress quotas.
- A custom Lake spill format, distributed spill, or durable query results.
- Automatically deriving budgets from cgroups or machine RAM.
- Changing Lance write/compaction memory behavior.
- A broad DataFusion upgrade or modifications outside the execution crate.
