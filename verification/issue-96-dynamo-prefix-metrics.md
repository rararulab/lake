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
- Independent corrected-head review is pending.
