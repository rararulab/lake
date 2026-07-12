# Verification: Catalog directory generations

## Required evidence

- Registry DDL and its generation signal are atomic in RocksDB and DynamoDB.
- Fenced metadata publication cannot move either value under a stale lease.
- Stable and append-only refreshes avoid full scans after explicit authority.
- Legacy-writer compatibility, moving-generation rejection, and finalizer
  acknowledgements are executable lane-1 scenarios.
- Full workspace, e2e, lint, docs, and real LocalStack paths pass.

## GREEN evidence

- All eight lane-1 scenarios passed with non-zero selector matches.
- TDD captured the pre-change steady-state two-scan behavior and the missing
  fenced signaled-mutation implementation before the production changes.
- RocksDB tests cover atomic create/delete signaling, conflict neutrality, and
  version neutrality. The stale-leader test proves a replaced lease moves
  neither registration nor generation.
- The shared LocalStack v1→dual→v2-authoritative lifecycle passed the real
  DynamoDB transaction path, including target/generation parity in both
  layouts and no movement on failed target conditions.
- Catalog tests cover one-scan steady state, append-version churn, unsignaled
  legacy writes, bounded retry under concurrent DDL, last-good preservation,
  and point-read failure health accounting.
- CLI parsing rejects missing or partial rollout acknowledgements and accepts
  only the complete `catalog-finalize` invocation. Durable finalization is
  idempotent in registry tests.
- Strict workspace Clippy with warnings denied passed.
- Workspace rustdoc passed with no dependency documentation build.
- `mise run gate` passed all workspace/all-target tests, e2e self-check,
  hooks, and site typecheck/tests/build on the final code.
- Review found and fixed decorator regressions caused by the new fail-closed
  metastore primitive, then audited all in-scope `MetaStore` wrappers used by
  registry mutation tests. Review also added refresh-health accounting for
  generation point-read failures.
- Final correctness/performance/security review found no remaining P0-P2
  issues. The documented residual risk is operational: old registry writers
  are forbidden after the acknowledged monotonic finalization boundary.
