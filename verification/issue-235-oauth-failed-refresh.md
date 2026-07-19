# Issue #235 verification

## Delivered contract

- OAuth client-credential REST sessions coordinate one in-flight renewal per
  observed token generation without holding a lock across the external token
  exchange.
- Followers receive the leader's successful or failed renewal result. A failed
  result is not retained after publication, so a later independent bounded
  read can make a fresh attempt without a timer, backoff, or circuit breaker.

## Red/green evidence

- Before the change,
  `concurrent_oauth_refresh_failure_is_single_flight` failed with
  `left: 8, right: 1`: eight concurrent distinct-table readers caused eight
  failed client-credential renewals.
- The test uses a real loopback Axum REST catalog and the connector's real
  Reqwest client. Startup obtains one valid OAuth token; exact table loads then
  return unauthorized while the first failing token renewal is held so the
  other readers join it.
- After the change, every reader returns only `IcebergError::Catalog` and the
  token endpoint observes exactly one renewal. The exact test passed ten
  consecutive runs.

## Verification

- `cargo +nightly fmt --all --check` — PASS.
- `cargo test -p lake-iceberg` — PASS (13 catalog integration tests and 3
  configuration tests).
- `cargo test -p lake-iceberg --test catalog
  concurrent_oauth_refresh_failure_is_single_flight -- --exact` — PASS; ten
  consecutive runs passed.
- `cargo clippy -p lake-iceberg --all-targets --all-features -- -D warnings`
  — PASS.
- `mise run spec-lint specs/issue-235-oauth-failed-refresh.spec.md` — PASS
  (100%).
- `mise run spec-lifecycle specs/issue-235-oauth-failed-refresh.spec.md` —
  PASS.
- `mise run gate` — PASS (hooks, workspace tests, e2e, ADBC interoperability,
  and site build).
- `mise run ship` — PASS (full local CI, Conventional Commit validation, and
  Jujutsu push gate).
