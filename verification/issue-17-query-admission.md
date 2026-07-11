# Verification: issue #17 Query admission control

## RED evidence

- `cargo test -p lake-query query_admission_rejects_when_saturated_and_releases_on_drop`
  failed because `QueryLimits` and stream-held admission did not exist.
- `cargo test -p lake-query query_execution_deadline_terminates_slow_stream`
  timed out after one second because a stalled DataFusion stream had no
  execution deadline.
- `cargo test -p lake-query oversized_sql_and_ticket_are_rejected_before_planning`
  failed because oversized SQL reached planning instead of being rejected.
- `cargo test -p lake-cli query_limit_values_are_validated_before_serving`
  failed because startup limit parsing did not exist.

## Focused GREEN evidence

- Saturation at concurrency one returns `ResourceExhausted`; dropping the live
  response stream releases its owned semaphore permit for the next query.
- A stream that emits initial Flight data and then stalls returns
  `DeadlineExceeded` at the configured wall-clock deadline and releases its
  permit.
- Oversized GetFlightInfo SQL and DoGet ticket handles return
  `ResourceExhausted` while a counting metastore observes zero catalog scans.
- The existing delayed-partition test still proves DoGet returns before its
  producer finishes.
- Query's complete test suite passes with 9 tests; strict Query clippy passes.
- CLI rejects zero/malformed values and constructs exact durations/counts for
  valid values.

## Final gates

- `mise run spec-lint specs/issue-17-query-admission.spec.md`: PASS, quality
  100%.
- `mise run spec-lifecycle specs/issue-17-query-admission.spec.md`: PASS, all
  five scenarios, explicit boundaries, and zero-match guard.
- `cargo clippy -p lake-query -p lake-cli --all-targets -- -D warnings`: PASS.
- `mise run test-integration`: PASS, 7/7 real LocalStack S3/DynamoDB tests.
- `mise run gate`: PASS in 34.77s, including all workspace targets/tests,
  hooks, local self-check, and site checks/build.

The existing macOS debug-linker `__eh_frame section too large` warning remains
non-fatal and did not affect strict linting or test behavior.
