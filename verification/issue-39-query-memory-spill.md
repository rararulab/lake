# Verification: Query memory and spill budgets

Issue: #39

## Candidate

- Base: `6f85c06efa19b1e28fc3bd0b7835a6fe5108e814`
- Workspace: `.worktrees/issue-39-query-memory-spill`
- Allowed-change paths: 35

## Evidence

- `query_resources_reject_invalid_budgets` rejects zero/undersized budgets
  and a spill root that is an existing file with typed startup errors.
- `query_engine_uses_bounded_fair_spill_runtime` observes a finite
  `FairSpillPool`, exact memory limit, exact aggregate disk limit, and a
  DataFusion child directory under the configured root.
- `memory_intensive_sort_spills_and_cleans_up` sorts 262,144 strings with a
  16 MiB execution pool, observes non-zero spill metrics, verifies every row
  is ordered, and proves memory reservations, disk accounting, spill files,
  and the runtime child directory are released.
- `query_resource_values_are_validated_before_serving` preserves valid CLI
  deployment values and rejects zero, malformed, and empty values.
- Vendored DataFusion 53.1 applies the spill-growth delta with one atomic
  quota reservation. `test_disk_limit_rejection_does_not_leak_usage` proves a
  rejected write leaves the global counter at zero and the full quota reusable.
- `spill_budget_error_does_not_poison_runtime` drives a real external sort past
  the 16 MiB disk ceiling, receives the resource error, observes zero disk
  accounting/files, reserves quota again, and executes a later SQL query on
  the same runtime.
- `cargo test -p lake-query --lib`: 20/20 PASS.
- `cargo clippy -p lake-query -p lake-cli --all-targets -- -D warnings`: PASS.
- `mise run spec-lifecycle specs/issue-39-query-memory-spill.spec.md`: 4/4
  scenarios PASS with every selector matching at least one test.
- Clean `mise run gate`: PASS, including workspace tests, e2e self-check,
  and site check/build.

The macOS linker emits its existing `__eh_frame` size warning for the large
Lance/DataFusion test binary; strict Rust clippy is warning-free.
