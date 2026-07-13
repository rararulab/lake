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
