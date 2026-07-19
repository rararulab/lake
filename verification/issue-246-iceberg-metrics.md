# Issue #246 verification

## Delivered contract

- Query's existing Prometheus recorder now describes three Iceberg federation
  counters: current-snapshot resolution, exact catalog operations, and OAuth
  refresh outcomes.
- Every emitted label is a fixed operation or state value. Namespace, table,
  endpoint, warehouse, SQL, tenant, principal, URI, token, and credential
  values remain absent from metrics.
- Counters observe existing control flow only; they add no retry, timeout,
  cache, cancellation, listener, or configuration behavior.

## Regression evidence

- Red: before the implementation, the cache/load metric test did not compile
  because `lake_iceberg::describe_metrics` did not exist.
- Green: the cache test now observes one `loaded`, one `cache_hit`, and one
  successful `table_load`, then rejects the external identity strings from the
  Prometheus scrape.
- Green: the failure test observes one `error` resolution and one failed
  `table_load`; it also proves the counter did not create a retry by asserting
  exactly one external load.
- Green: the OAuth renewal integration test observes one `started` and one
  successful refresh alongside the original failed-then-successful exact table
  loads, and rejects the OAuth token and table identity from the scrape.

## Verification

- `cargo +nightly fmt --all --check` — PASS.
- `cargo clippy -p lake-iceberg --all-targets --all-features -- -D warnings` —
  PASS.
- `cargo test -p lake-iceberg` — PASS (17 catalog tests and 3 configuration
  tests).
- `cargo test -p lake-query telemetry::tests::query_metrics_cover_admission_and_catalog_refresh -- --exact` —
  PASS (existing Query telemetry registration test).
- `mise run spec-lint specs/issue-246-iceberg-metrics.spec.md` — PASS (100%).
- `mise run spec-lifecycle specs/issue-246-iceberg-metrics.spec.md` — PASS.
- `mise run ship` — PASS (full local CI, Conventional Commit validation, and
  Jujutsu push gate).
