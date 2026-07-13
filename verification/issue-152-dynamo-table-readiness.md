# Verification: DynamoDB table readiness

## Required evidence

- Bootstrap observes `DescribeTable` until both the legacy and `_prefix_v2`
  physical tables report `ACTIVE`.
- `CREATING`, `UPDATING`, and the short post-create `NOT_FOUND` propagation
  are bounded retry states; no ready path can busy-loop.
- A permanently non-ready table and an unavailable status fail with the table
  name and latest status rather than waiting forever or allowing data-plane
  traffic.
- Runtime `open_tables` remains DescribeTable-only, so pre-provisioned Query
  and Metasrv identities need no CreateTable permission.

## RED/GREEN evidence

- The initial three readiness tests failed to compile against the baseline:
  the readiness state policy, bounded poller, and diagnostic error variants
  did not exist.
- The green tests inject real AWS `TableStatus` values: `CREATING ->
  UPDATING -> ACTIVE` observes the configured retry cadence; perpetually
  `CREATING` stops exactly at the observation bound with its final status; and
  `DELETING` fails immediately with table and status.
- `cargo nextest run -p lake-meta` passed 36/36 tests (one ignored LocalStack
  roundtrip), including the three new deterministic tests and the LocalStack
  wiring check.
- `mise run spec-lifecycle specs/issue-152-dynamo-table-readiness.spec.md`
  passed all four scenarios with non-zero selector matches.
- `mise run gate` passed hooks, workspace tests, CLI selftest, ADBC interop,
  and site checks on the final tree. The existing linker compact-unwind
  warning is emitted by macOS `ld` for large debug binaries and is not a test
  or lint failure.

## LocalStack evidence

- The real ignored DynamoDB roundtrip remains wired through `ensure_table`
  and a second `open_tables` call. Its focused wiring test passes.
- `mise run test-integration` could not start LocalStack in this environment:
  Docker points at a missing OrbStack socket
  `/Users/ryan/.orbstack/run/docker.sock`. This is an external test-runtime
  prerequisite failure before any lake test executes, not a product failure.

## Review result

- `open_tables` no longer relies on the generated AWS waiter, whose only
  retry acceptor is `ResourceNotFoundException` and which otherwise rejects
  `CREATING` as `NoAcceptorsMatched`.
- The replacement is shared by both physical tables, makes only
  `DescribeTable` calls, retries at one-second intervals for at most 120
  observations, and treats all states other than ACTIVE/CREATING/UPDATING as
  explicitly unavailable.
- No DynamoDB SDK type crosses the `lake-meta` public boundary. No schema,
  object path, SQL, or permission scope changed.
- Residual risk: real LocalStack execution must be rerun after Docker/OrbStack
  is available. The deterministic state-policy tests prevent the prior
  `CREATING` waiter regression independently of that environment.
