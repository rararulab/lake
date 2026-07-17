# Verification: issue #154 exact managed FILE range reads

## RED evidence

- `mise run spec-lifecycle specs/issue-154-exact-range-reads.spec.md` initially
  failed its zero-match guard: the three new range-integrity selectors had no
  matching tests.

## GREEN evidence

- `cargo test -p lake-objects exact_range_reader_returns_requested_bytes`:
  PASS. Small caller buffers receive exactly the bounded stream prefix and EOF
  succeeds without buffering the interval.
- `cargo test -p lake-objects local_range_reader_rejects_truncated_object`:
  PASS. A local object truncated after `DataLocation` creation returns its
  available range prefix, then `InvalidData` with
  `ObjectIntegrityError::PrematureEof { expected: 6, actual: 3 }`.
- `cargo test -p lake-sdk sdk_open_range_rejects_truncated_stage_without_query`:
  PASS. An injected short range stream fails with the same typed error while
  the Query endpoint is unreachable.
- `cargo test -p lake-objects s3_range_read_localstack_is_wired`: PASS. The
  source-level smoke test keeps the ignored real LocalStack bounded range GET
  in the shared integration runner.

## Final gates

- `mise run spec-lifecycle specs/issue-154-exact-range-reads.spec.md`: PASS,
  all four selectors executed.
- `cargo +nightly fmt --all -- --check`: PASS.
- `cargo clippy -p lake-objects -p lake-sdk --all-targets -- -D warnings`:
  PASS.
- `cargo nextest run -p lake-objects -p lake-sdk`: PASS, 94 tests passed and
  16 LocalStack/Docker tests skipped by their existing configuration.
- `mise run gate`: PASS. Workspace tests, CLI self-check, upstream ADBC
  Flight SQL interop, repository hooks, and site checks/build all passed. The
  macOS linker emitted its existing compact-unwind performance warning only.
- `mise run test-integration`: BLOCKED before any Lake test ran because Docker
  points to the missing OrbStack socket
  `/Users/ryan/.orbstack/run/docker.sock`. The focused source-level S3 wiring
  test passed, but the ignored real LocalStack protocol suite requires the
  local Docker daemon to be restored.

## Range-response repair

### RED evidence

- `cargo test -p lake-objects s3_range_response_requires_exact_interval`
  initially ran zero tests before the response-metadata contract existed.

### GREEN evidence

- `cargo test -p lake-objects s3_range_response_requires_exact_interval`:
  PASS. Only the exact `Content-Range` and `Content-Length` for the requested
  interval pass; missing, shifted, end/total-size, and length-mismatched
  responses fail before body delivery.

### Final gates

- `mise run spec-lifecycle specs/issue-154-exact-range-reads.spec.md`: PASS,
  all five selectors executed.
- `cargo +nightly fmt --all -- --check`: PASS.
- `cargo clippy -p lake-objects --all-targets -- -D warnings`: PASS.
- `cargo nextest run -p lake-objects`: PASS, 44 tests passed and 14 existing
  LocalStack tests skipped by configuration.
- `mise run gate`: PASS. Workspace tests, CLI self-check, upstream ADBC Flight
  SQL interop, repository hooks, and site checks/build all passed. The macOS
  linker emitted its existing compact-unwind performance warning only.

## Current rebase validation

The historical evidence above predates the final rebase onto the merged
credentialless direct-reader. The final candidate is conflict-free and has
these fresh results:

- Base: `8343b270d5c7c886c2c75ac6a9dd983024b90bc7` (#143).
- `mise run spec-lifecycle specs/issue-154-exact-range-reads.spec.md`: 5/5
  scenarios passed.
- `cargo nextest run -p lake-objects -p lake-sdk`: 113 passed; 17 existing
  LocalStack-only tests skipped.
- `cargo clippy -p lake-objects -p lake-sdk --all-targets -- -D warnings`,
  nightly formatting, and `git diff --check`: passed.
- Final `mise run ship`: passed, including dependency policy, workspace
  tests, rustdoc, E2E, upstream ADBC interop, site checks, and the live
  LocalStack suite (20/20 passed; one existing Nextest leaky-process
  diagnostic). The checkout-scoped LocalStack container was removed.
