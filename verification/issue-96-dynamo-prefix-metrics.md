# Verification: Dynamo prefix metrics

## Required evidence

- Recorder tests prove finite labels and hostile identity non-disclosure.
- LocalStack proves physical v1/v2 request counters follow the real lifecycle.
- Lane-1 scenarios, strict clippy, full gate, and docs pass.
- Independent review confirms protocol behavior is unchanged.

## GREEN evidence

- Four lane-1 scenarios passed with non-zero selector matches.
- Focused `lake-meta` Dynamo/telemetry tests passed, including recorder
  installation after Dynamo construction and hostile identity non-disclosure.
- Strict `cargo clippy -p lake-meta --all-targets -- -D warnings` passed.
- Checkout-scoped LocalStack v1→v2 lifecycle passed and rendered real v1 Scan,
  v2 Query, authority, and barrier series.
- `mise run gate` passed all workspace tests, e2e, hooks, and site checks.
- `mise run doc` passed with rustdoc warnings denied.
- Initial correctness review found that a telemetry-only barrier read could
  change startup errors; the corrected implementation is best-effort, retains
  last-known state, and has a hard 100 ms timeout with a 30-second refresh
  interval.
- Initial performance/release review found repeated metric descriptions and
  invalid PromQL. Descriptions now register once after recorder installation,
  hot calls only perform an atomic deadline check, and the documented queries
  aggregate matching label sets over explicit ranges.
- The corrected full gate and documentation check passed after these fixes.
- The final frozen code head passed the lane-1 lifecycle again (4/4), and the
  docs-only follow-up passed `mise run site-check`.
- Correctness/security review approved the best-effort refresh, state timing,
  and finite identity-free labels.
- Performance and release review confirmed the implementation and
  amplification queries, then found that global `absent_over_time` could miss
  one absent pod in a multi-pod deployment. Rollout docs now reconcile each
  `up` target with the authority series via `unless on (service, instance)` and
  separately alert on authority `== 0`.
- Performance re-review approved the per-target inventory reconciliation and
  explicit v1 predicate; release/operations re-review passed the docs-only
  frozen head.
