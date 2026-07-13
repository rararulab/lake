# Issue #124 verification

Candidate base: `8b902e06407063bac8ee2e9173de324662972d40`

## Delivered contract

- Served `lake query` dispatches before the all-in-one `Context` and constructs
  a minimal `QueryContext` containing only the storage engine and managed-stage
  descriptor. Local Query creates no Rocks catalog state or local Metasrv.
- Cloud storage validates distinct bounded DynamoDB names before I/O. Catalog,
  manifest, and enabled async bases plus every `_prefix_v2` companion must be
  pairwise disjoint. Catalog authority remains on `LAKE_DYNAMODB_TABLE`;
  physical Lance manifests move to `LAKE_MANIFEST_DYNAMODB_TABLE`.
  Metadata/admin opens the first two existing pairs, while Query opens only
  manifest plus independently validated async state and loads each v2 marker.
- Query uses a read-only external-manifest adapter. Current fixed-pointer reads
  work; put/update/delete/cleanup fail before MetaStore mutation, and missing
  legacy latest pointers fail closed with a metadata-migration requirement.
- The Kubernetes reference and operator docs define separate catalog,
  manifest, async-state, and object authorities. Query IAM has no registry
  access and only `DescribeTable`/`GetItem` access on manifest tables.

## Red/green evidence

- The first CLI authority selector failed to compile because `QueryContext`
  and `CloudStoragePlan` did not exist.
- The read-only manifest selector failed to compile because
  `MetaManifestStore::new_read_only` did not exist.
- Alias/empty table validation runs against a pure plan before Dynamo/S3 client
  construction. The plan exposes metadata `[registry, manifest]` and Query
  `[manifest]` authority sets.
- A local Query context leaves `<data-dir>/meta` absent and dispatch source is
  locked to bypass `Context::open`.
- A legacy history-only manifest remains unchanged after a read-only latest
  lookup; current fixed-pointer reads succeed, while put and delete reject.

## Verification

- `mise run doctor` — PASS in the #124 jj workspace.
- `mise run spec-lint specs/issue-124-query-manifest-authority.spec.md` — PASS,
  quality 100%.
- `cargo test -p lake-engine-lance --lib` — PASS, 40/40.
- `cargo test -p lake-cli --all-targets` — PASS: 35 unit, 1 Kubernetes, and 4
  logging tests.
- Affected strict clippy (`lake-engine-lance`, `lake-cli`, all targets,
  `-D warnings`) — PASS.
- `mise run spec-lifecycle specs/issue-124-query-manifest-authority.spec.md` —
  PASS, all five selectors executed at least one test.
- `mise run gate` — PASS on the post-review candidate in 104.31s: workspace
  tests, local selftest E2E,
  three upstream ADBC interoperability tests, hooks, and site checks all pass.
- Correctness/security review initially found base/companion and async authority
  alias gaps. The final candidate expands all enabled authority groups to
  physical table sets before connecting, carries the validated async plan in
  `QueryContext`, and was re-reviewed: APPROVE, no remaining P0/P1/P2.
- Deployment review initially found overbroad Query IAM and an incomplete
  manifest-v2 cutover. The final runbook uses only `DescribeTable`/`GetItem`
  for Query and specifies independent bounded backfill, exact verification,
  finalization, rollout, retention, and rollback. Re-review: APPROVE.
