# Issue #233 verification

## Delivered contract

- The real loopback Axum/Reqwest REST integration test waits until the
  unresponsive configuration handler has accepted the request.
- It then pauses Tokio time and advances 26 ms past the configured 25 ms
  client deadline; the connection must return `IcebergError::Catalog`.
- `tokio`'s `test-util` feature is scoped to `lake-iceberg` dev-dependencies.
  Production timeout values, retries, and catalog behavior are unchanged.

## Red/green evidence

- The prior full `mise run ship` failed at the old
  `Instant::elapsed() < 500 ms` assertion while competing with a cold Rust
  documentation build. The same test then passed five isolated runs, showing
  that host scheduling—not the REST timeout path—was the unstable signal.
- The first virtual-time attempt started the whole runtime paused and hung
  before the real HTTP request reached Axum. Moving `pause()` until after the
  handler's `Notify` observation preserves the live I/O setup and makes only
  the timeout deadline deterministic.
- The repaired exact test passes after advancing the configured deadline.

## Verification

- `cargo test -p lake-iceberg --test catalog
  rest_catalog_timeout_bounds_unresponsive_startup -- --exact --nocapture`
  — PASS after the repair.
- The exact test was repeated 10 times consecutively — PASS on all 10 runs.
- `cargo +nightly fmt --all --check` — PASS.
- `mise run spec-lint specs/issue-233-rest-timeout-test.spec.md` — PASS
  (100%).
- `mise run spec-lifecycle specs/issue-233-rest-timeout-test.spec.md` — PASS.
- `cargo test -p lake-iceberg` — PASS (12 catalog and 3 configuration tests).
- `cargo clippy -p lake-iceberg --all-targets --all-features -- -D warnings`
  — PASS.
- `mise run gate` — PASS (hooks, workspace tests, ADBC interoperability,
  end-to-end ingest/commit/SQL, and site build).
- `mise run ship` — PASS (strict Rust docs, dependency audit, all workspace
  tests, ADBC, and 21 LocalStack integration tests).
