# Verification: Dynamo prefix isolation

## Failure evidence

The current Dynamo v1 implementation uses strongly consistent table `Scan`
with a `begins_with(pk, :prefix)` filter for `list_prefix`, `scan_prefix`, and
`scan_prefix_page`. Dynamo applies the page limit to evaluated items before
the filter, so registry reads evaluate unrelated append-operation, manifest,
lease, and tombstone keys. Retained operation volume therefore amplifies
catalog and maintenance authority work.

## Required evidence

- Stable sharded physical-key and cursor unit tests.
- Strongly consistent LocalStack Query tests with evaluated-item accounting.
- Atomic dual CAS and guarded-mutation race tests.
- Backfill crash/replay and concurrent-writer convergence tests.
- Exact verification/finalization failure tests.
- Full v1→dual→v2 migration roundtrip.

## GREEN evidence

- `mise run spec-lifecycle specs/issue-94-dynamo-prefix-isolation.spec.md`:
  8/8 scenarios passed and every selector executed at least one test.
- `cargo test -p lake-meta dynamo_ -- --nocapture`: 9 focused unit tests
  passed, including layout/cursor, dual CAS, guarded mutation, backfill
  conditions, and finalization admission.
- Checkout-scoped LocalStack:
  `cargo test -p lake-meta --test dynamo_localstack -- --ignored --nocapture`
  passed the complete v1 → dual write → bounded backfill → exact finalize →
  v2 strongly-consistent prefix-query lifecycle.
- The first LocalStack run exposed Dynamo's reserved `bucket` identifier in a
  projection and key condition. The projection now omits the unused field and
  the key condition uses an expression-name alias; the exact lifecycle then
  passed.
- `cargo clippy -p lake-meta -p lake-cli --all-targets -- -D warnings` passed.
- `mise run gate` passed: workspace all-target tests, self-check, repository
  hooks, and site typecheck/tests/build all completed successfully.
- `mise run doc` passed and generated documentation for all public crates.
- Independent frozen-head verification is recorded below when run.
