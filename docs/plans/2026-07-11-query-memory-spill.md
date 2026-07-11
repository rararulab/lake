# Query memory and spill budgets

Issue: #39

## Outcome

Every Query replica builds DataFusion with a finite, process-wide execution
memory pool and a finite spill manager rooted in an operator-selected local
directory. Large spilling operators degrade to disk or a typed resource error
instead of relying on the process OOM killer.

## Implementation

1. Add `QueryResources` with validated memory bytes, spill bytes, and spill
   root. Build a `RuntimeEnv` with `FairSpillPool` and `DiskManagerBuilder`.
2. Add a fallible `QueryEngine` constructor that injects the runtime into
   `SessionContext`; retain bounded defaults for library callers.
3. Parse `LAKE_QUERY_MEMORY_BYTES`, `LAKE_QUERY_SPILL_BYTES`, and
   `LAKE_QUERY_SPILL_DIR` at the CLI boundary before starting the server.
4. Add focused runtime, spill/cleanup, and CLI validation tests.
5. Document sizing, local disk ownership, and the distinction between
   execution memory and streamed result backpressure.

## Verification

- `mise run spec-lifecycle specs/issue-39-query-memory-spill.spec.md`
- `cargo test -p lake-query query_resources_reject_invalid_budgets`
- `cargo test -p lake-query query_engine_uses_bounded_fair_spill_runtime`
- `cargo test -p lake-query memory_intensive_sort_spills_and_cleans_up`
- `cargo test -p lake-cli query_resource_values_are_validated_before_serving`
- `cargo clippy -p lake-query -p lake-cli --all-targets -- -D warnings`
- `mise run gate`
