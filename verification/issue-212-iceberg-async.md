# Issue #212 verification

Candidate base: `5e01f219a5637b10aafeac89ba3408c941f1b2e9`

## Delivered contract

- `PollFlightInfo` accepts configured, read-only `iceberg.<namespace>.<table>`
  scans and persists the existing encrypted statement ticket through the
  ordinary bounded async-job lifecycle.
- The worker reconstructs each external provider by the ticket's exact
  namespace, table, and immutable snapshot ID before planning. It neither
  enumerates the catalog nor substitutes the current Iceberg head.
- If Iceberg retention has removed that snapshot, the worker fails the durable
  job before result publication. The caller receives the existing generic async
  execution failure and no completed result manifest exists.
- Native Lake async behavior, submission idempotency, tenant quotas, leases,
  cancellation, result-part bounds, and state/ticket schemas are unchanged.
- The README and federation design describe the durable execution topology and
  its read-only, exact-snapshot boundary.

## Adversarial evidence

- The asynchronous Iceberg test submits a job whose snapshot has one row,
  advances the external catalog to add a second row before the worker runs,
  then reads the standard poll result. The result remains `[1]`, proving that
  execution did not fall forward.
- The unavailable-snapshot test seals a durable statement referencing an absent
  snapshot ID. The worker returns a query error, leaves the record failed, and
  publishes no result manifest.
- The external success path uses `EmptyMeta`; it completes without a native
  Lake table or registry lookup. The ticket is opened identity-bound before
  the worker receives the durable job.

## Verification

- `cargo +nightly fmt --all` — PASS.
- Focused exact selectors — PASS:
  `flight::tests::async_iceberg_submission_executes_pinned_snapshot`,
  `flight::tests::async_iceberg_worker_rejects_unretained_snapshot`, and
  `flight::tests::poll_flight_info_submits_identity_bound_pinned_job`.
- `mise run spec-lifecycle specs/issue-212-iceberg-async.spec.md` — PASS; all
  four scenario selectors matched at least one test.
- `mise run gate` — PASS: hooks, full workspace tests (including all 105
  `lake-query` library tests), local selftest E2E, upstream ADBC
  interoperability checks, and site checks.
- Independent diff/security review — PASS; no P0/P1 findings. The review
  confirmed the allowed file boundary, retained encrypted ticket format,
  exact-snapshot API path, fail-closed error propagation, and absence of new
  credentials, mutable external operations, async state fields, or resource
  policy changes.
