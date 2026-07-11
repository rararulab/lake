# Issue #53 verification: append admission

## Candidate

- Base: `a7417b8d`
- Implementation head: `bbad129e`
- Scope: metasrv append concurrency, buffered metadata admission, and CLI configuration

## Contract evidence

- `mise run spec-lifecycle specs/issue-53-append-admission.spec.md`: 5/5 scenarios passed.
- All five spec selectors were absent on the base, present exactly once on the candidate,
  and passed as focused tests.
- The two-node test pauses the leader inside `append_reserved`, proves a second append
  through the follower is rejected with `ResourceExhausted`, then proves permits are
  released after the first commit and a later append succeeds.
- The configured one-byte stream limit rejects an oversized append before engine work;
  the registry remains at `Version(1)`.
- CLI validation covers zero and malformed values, a stream budget larger than the
  process buffer budget, semaphore-weight overflow, and valid configuration.

## Quality gates

- `cargo test -p lake-metasrv`: 53 passed, 1 ignored LocalStack test.
- `cargo test -p lake-metasrv --test two_node`: 5 passed.
- `cargo test -p lake-cli`: 16 passed.
- Strict clippy for metasrv and CLI with all targets/features and `-D warnings`: passed.
- Independent `mise run gate`: passed (workspace tests, e2e, site, and hooks).
- Workspace and issue boundary checks: clean; no out-of-scope changes.

## Review

- Independent correctness/security review: APPROVE, no blocker or high-severity finding.
- Independent release verifier: PASS.
- Admission uses owned RAII permits under one queue timeout, reserves the full per-stream
  byte ceiling before polling the first Flight message, and holds permits across follower
  forwarding or leader validation, decode, commit, and response construction.
- This change does not alter the Flight wire contract or durable metadata layout.
