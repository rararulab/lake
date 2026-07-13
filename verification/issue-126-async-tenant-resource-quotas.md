# Issue #126 verification

Candidate base: `361463312524bfb84ce396a353a792ad3ad79d94`

## Delivered contract

- Async submission reserves a bounded, SHA-256-domain-separated tenant index
  entry in the shared `MetaStore` before uploading the encrypted job object.
  The index is CAS-authoritative across replicas, holds no raw tenant identity,
  and retries at most eight times.
- A confirmed schema-v2 record persists both its tenant-reservation token and
  immutable result-byte ceiling. Schema-v1 records remain decodable, runnable,
  pollable, and cleanable with the legacy hard ceiling and without an invented
  reservation.
- A stale pre-record reservation receives a five-minute grace. Reconciliation
  point-reads only the bounded index entries: missing owners are reclaimed;
  extant records remain counted until exact fenced cleanup deletes state and
  releases their own `(query ID, reservation token)` pair. Crashes can over-count
  temporarily but cannot under-count a live durable job; a late cleanup cannot
  release a deterministic re-submission's new reservation.
- An old schema-v1 replica winning a deterministic state-create race cannot
  poison the v2 tenant index: the v2 coordinator releases its exact losing
  token before resuming the legacy record. If that bounded release is
  unavailable, post-grace reconciliation discards only the unmatched v1 entry;
  a token persisted by a v2 record remains fail-closed.
- Workers, completion transitions, and manifest verification enforce the
  persisted ceiling rather than current process configuration. Flight maps
  durable quota exhaustion to an identity-free `ResourceExhausted`; the metric
  is the fixed `lake_query_async_quota_rejections_total{reason="outstanding_jobs"}`.
- Query startup parses durable resource bounds before bind. The Kubernetes
  reference declares `LAKE_ASYNC_MAX_OUTSTANDING_PER_TENANT=8` and
  `LAKE_ASYNC_MAX_RESULT_BYTES=17179869184`; docs describe their ranges and
  retained-storage, not CPU/memory/fairness, scope.

## Red/green evidence

- The v1 compatibility and resource-bound selectors initially failed to
  compile because resource fields, accessors, and limit validation did not
  exist. The former now drives queued, completed, and cleaning v1 records
  through the current worker, poll capability, and cleanup path; the latter
  restarts against the same state with a looser process limit and rejects a
  64-MiB encoded payload before part or manifest publication.
- The tenant-index selectors initially failed to compile because there was no
  durable reservation API. They now exercise three stores over one Rocks
  authority, exact same-tenant capacity, cross-tenant isolation, missing-owner
  recovery, preservation of an extant record, and the v1/v2 deterministic
  create interleaving that must release the losing token.
- The Flight selector admits one retained job, then confirms a second distinct
  submission receives only `ResourceExhausted` with the fixed public message;
  the returned status contains no tenant identity.
- The quota-rejection metric has a dedicated fixed `outstanding_jobs` reason;
  it is not recorded as a scheduler event. Its telemetry test rejects actual
  tenant labels and representative tenant values.
- The final all-target run exposed a backpressure-test accounting omission:
  input can simultaneously occupy the bounded channel, one sender-held chunk,
  and one decoder-owned chunk. The `#[cfg(test)]` observation bound now records
  that finite `C + 2` window; production stream limits are unchanged.

## Verification

- `mise run doctor` — PASS in the #126 jj workspace.
- `mise run spec-lint specs/issue-126-async-tenant-resource-quotas.spec.md` —
  PASS, quality 100%.
- `mise run fmt` and `mise run fmt-check` — PASS.
- `mise run clippy` — PASS, workspace/all-targets/all-features with warnings
  denied.
- `cargo test -p lake-query --lib` — PASS, 89 tests.
- `cargo test -p lake-query --all-targets -- --quiet` — PASS: 89 library
  tests, one ADBC interoperability test (three explicitly ignored without its
  external fixture), and one file-append proxy test.
- `cargo test -p lake-cli` — PASS, 36 unit, 1 Kubernetes, and 4 logging tests.
- Focused CLI resource and Kubernetes selectors — PASS.
- Focused Flight quota, v1 compatibility, tenant-index, cleanup, and immutable
  result-limit selectors — PASS.
- `mise run spec-lifecycle specs/issue-126-async-tenant-resource-quotas.spec.md`
  — PASS; all eight scenario selectors executed at least one test.
- `mise run gate` — PASS: formatting/hooks, full workspace tests, local
  selftest E2E, three upstream ADBC interoperability tests, and site checks.
- Final independent correctness/security and release/operations re-reviews —
  PASS; no P0/P1 findings.
