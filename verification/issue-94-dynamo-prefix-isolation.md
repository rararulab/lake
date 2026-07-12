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
- Independent review of the first frozen commit found a forged-cursor shard
  omission, runtime `CreateTable` IAM mismatch, non-resumable documented
  finalize flow, global generation hot item, and fixed 64-query registry
  fan-out. The corrected implementation binds cursor keys to their derived
  shard, opens pre-provisioned runtime tables without `CreateTable`, finalizes
  without restarting backfill, replaces the hot generation with a durable
  write barrier, and uses 8/32/64 family-specific shard counts.
- The corrected LocalStack lifecycle seeds a genuine v1-only key, backfills
  it, proves a stale dual node is blocked by finalization, refreshes that node
  to v2 authority, and then proves writes resume.
- `cargo clippy -p lake-meta -p lake-cli --all-targets -- -D warnings` passed.
- The first `mise run gate` passed: workspace all-target tests, self-check, repository
  hooks, and site typecheck/tests/build all completed successfully.
- `mise run doc` passed and generated documentation for all public crates.
- Corrected `mise run test-integration` passed all 15 LocalStack integration
  tests from a fresh checkout-scoped container.
- Corrected `mise run gate` passed all workspace tests, e2e, hooks, and site
  checks; `mise run doc` passed with rustdoc warnings denied.
- Independent correctness/security review: **APPROVE**. It confirmed shard-bound
  cursors, pre-provisioned runtime IAM, barrier linearization, exact parity,
  marker publication, and stale-node fail-closed behavior.
- Independent performance/architecture review: **APPROVE**. It confirmed the
  global generation write hotspot is gone, barrier checks end for
  v2-authoritative nodes, and adaptive shard counts are consistent across
  point and prefix paths.
- Independent release verification: **PASS** on frozen head `b73339b0`, with
  cursor regression, dual CLI acknowledgements, runtime wiring, fresh
  LocalStack integration 15/15, clean diff, and clean workspace rechecked.
