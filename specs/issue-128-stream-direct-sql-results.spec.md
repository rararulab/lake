spec: task
name: "stream-direct-sql-results"
inherits: project
tags: [query, streaming, cli, resource, large-result]
---

## Intent

Make in-process SQL consumption follow the same bounded streaming model as
Flight and durable async results. `QueryEngine::execute_sql` currently
collects every DataFusion `RecordBatch` into a `Vec`, so a local caller or
`lake sql` can retain an arbitrarily large result. Return DataFusion's native
record-batch stream and consume it one batch at a time.

## Decisions

- Keep `execute_sql` as the direct-query entry point, but change its successful
  result to `SendableRecordBatchStream`; its name still describes execution
  while the type makes ownership and backpressure explicit.
- Planning, catalog freshness, snapshot resolution, read-only validation, and
  DataFusion execution errors retain their current boundaries. Only result
  materialization changes from `collect` to `execute_stream`.
- `lake sql` and `lake selftest` consume one successful batch at a time. The
  CLI formats each batch independently rather than retaining prior batches.
- Keep Flight SQL, async query state/tickets/objects, SDK direct `DataLocation`
  reads, and table commit behavior unchanged.

## Boundaries

### Allowed Changes
crates/lake-query/src/lib.rs
crates/lake-cli/src/commands/sql.rs
crates/lake-cli/src/commands/selftest.rs
crates/lake-sdk/src/lib.rs
docs/plans/2026-07-13-stream-direct-sql-results.md
specs/issue-128-stream-direct-sql-results.spec.md
verification/issue-128-stream-direct-sql-results.md

### Forbidden
collecting direct SQL results before returning them
Flight SQL protocol ticket async-query or DataLocation wire-format changes
new query connection metadata authority or object-store paths
changes to SQL read-only policy catalog staleness or snapshot pinning
unbounded client-side render queues or background result-draining tasks

## Completion Criteria

Scenario: Direct SQL exposes a live record-batch stream
  Test:
    Package: lake-query
    Filter: direct_sql_results_stream_before_source_completion
  Given a table source that emits one batch and blocks before its next batch
  When in-process SQL starts reading that table through QueryEngine
  Then the first batch is available before the source is released and the
  second batch arrives only after release without a collected result vector

Scenario: Direct SQL retains the public read-only boundary
  Test:
    Package: lake-query
    Filter: public_sql_surface_is_read_only
  Given an in-process QueryEngine
  When a caller attempts DDL or DML through the direct SQL API
  Then planning rejects the mutation before stream execution

## Out of Scope

- A new CLI output format or cross-batch pretty-table layout.
- Streaming DataFusion execution to external SDK callers; that remains Flight
  SQL and its existing direct-object `DataLocation` APIs.
- Changing per-query memory/spill limits or durable async quotas.
