# Verification: Bounded S3 multipart upload pipeline

## Required evidence

- Ordinary and resumable uploads overlap `UploadPart` requests without
  exceeding the configured per-object request/body bound.
- Source hashing, multipart completion, and durable checkpoint publication
  remain ordered when S3 responses complete out of order.
- A failed request cancels the remaining owned futures before multipart abort.
- Legacy and current checkpoints bound, reject, and overwrite crash-left
  remote suffixes without trusting their identity.
- Full workspace, e2e, lint, docs, and real LocalStack paths pass.

## RED/GREEN evidence

- The initial overlap test failed to compile because no multipart pipeline
  existed. Reverse-order, finite-configuration, checkpoint-compatibility, and
  creator-window tests likewise failed against their missing contracts before
  implementation.
- The first ordered-future implementation timed out the fail-fast regression:
  a blocked part one hid part two's failure. Replacing it with unordered
  polling plus a bounded ordered metadata buffer made the failure observable
  immediately while preserving contiguous publication.
- Review found that ordinary uploads retained pending futures while awaiting
  multipart abort. The drain loop now explicitly drops the pipeline before
  abort, preventing a late `UploadPart` from racing cleanup.
- All seven lane-1 scenarios passed with non-zero selector matches.
- `cargo test -p lake-objects --lib s3::pipeline_tests` passed all five focused
  pipeline tests.
- The real LocalStack ignored suite passed 12/12 tests, including a six-part
  ordinary round trip, interrupted abort, resumable reuse, ambiguous
  completion recovery, gapped suffix overwrite, and rejection outside the
  persisted creator window.
- `cargo clippy --workspace --all-targets -- -D warnings` passed.
- `cargo doc --workspace --no-deps` passed.
- `mise run gate` passed hooks, e2e, all workspace/all-target tests, and site
  checks before the final review fix; the final-code gate is recorded below.

## Review result

- No remaining P0-P2 correctness, security, performance, or removal findings.
- The pipeline owns all request futures, bounds pending plus ready results by
  concurrency, and never detaches work.
- The default request-body bound is 20 MiB per object (four 5 MiB parts); the
  validated hard maximum is 80 MiB (sixteen parts).
- Residual scope is explicit: fixed 5 MiB parts still rely on S3 to reject
  objects beyond the 10,000-part protocol limit. Adaptive part sizing belongs
  to a separate change.

## Final gate

- `mise run gate` passed on the reviewed production tree in 97.67 seconds:
  hooks, e2e, all workspace/all-target tests, and site checks completed with
  zero failures.
