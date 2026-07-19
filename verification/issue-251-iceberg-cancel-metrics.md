# Issue #251 verification

## Delivered contract

- The deterministic cancelled-snapshot-leader handoff now also scrapes the
  real local Prometheus recorder.
- The regression proves exactly one bounded `cancelled` snapshot outcome,
  followed by one `loaded` replacement outcome and exactly two external table
  loads.
- The rendered metrics omit the configured namespace, table, endpoint,
  warehouse, and a credential-looking warehouse component.

## Regression evidence

- Red: before the test existed, `mise run spec-lifecycle
  specs/issue-251-iceberg-cancel-metrics.spec.md` rejected its selector because
  it matched zero tests.
- Green: `cancelled_snapshot_leader_metrics_preserve_handoff_visibility` now
  resolves the replacement within one second after cancellation, observes both
  bounded outcomes, preserves the two-load handoff contract, and rejects the
  identity strings from the Prometheus scrape.

## Verification

- `cargo test -p lake-iceberg cancelled_snapshot_leader_metrics_preserve_handoff_visibility -- --exact` — PASS (one catalog integration test).
- `mise run spec-lifecycle specs/issue-251-iceberg-cancel-metrics.spec.md` —
  PASS.
- `mise run ship` — PASS (full local CI, Conventional Commit validation, and
  Jujutsu push gate).
