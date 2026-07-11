# Verification: bounded Flight discovery

Issue: #51
Head: `d97e5916`

## Result

PASS. Independent correctness review approved the lazy cursor semantics,
filtering, exact row/batch bounds, and admission lifecycle after two reported
P1 findings were fixed. Independent verification passed all selectors,
boundaries, and repository gates on the final head.

## Evidence

- `mise run spec-lifecycle specs/issue-51-flight-discovery-bounds.spec.md`:
  5/5 passed; every selector was absent on base and executed one test on head.
- `cargo test -p lake-query --lib`: 31 passed.
- `cargo test -p lake-cli --bin lake`: 15 passed.
- Strict clippy for Query/CLI, all targets and features: passed.
- `mise run gate`: workspace tests, e2e, hooks, and site
  typecheck/test/build passed locally and under the independent verifier.
- Boundary audit: every changed path is allowed; forbidden paths: zero.

## Resource-bound regressions

- One-slot discovery rejects a second stream at the queue timeout and releases
  the permit when the first response is dropped.
- Reading a terminal stream error releases admission immediately even while
  the failed stream object remains alive.
- Table and schema discovery emit batches no larger than the configured size;
  schema tests also verify every authorized namespace arrives once and in
  catalog order.
- With `max_rows=3`, `batch_rows=2`, and four matches, the client receives
  batches `[2, 1]` followed by the original tonic `ResourceExhausted`; the
  fourth row is not allocated.
- Invalid zero, malformed, or batch-greater-than-maximum deployment values
  fail before the Flight listener binds.

## Release notes

No durable metadata or Flight SQL wire-schema changes. New environment
variables are optional and default to 10,000 matching rows and 256 rows per
batch. Requests exceeding the row maximum now terminate with
`ResourceExhausted`; operators can raise the validated limit when a deployment
intentionally exposes more catalog rows.
