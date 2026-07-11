# Issue #37 Verification

Candidate base: `d1b936daef1e` (merged issue #35)

## Contract

- Explicit candidate `agent-spec lifecycle`: **8/8 PASS**.
- Spec lint: **100%** determinism, testability, and coverage.
- All 14 candidate paths are inside the declared boundaries.

## Protocol evidence

- Drop persists one immutable, incarnation-bound tombstone before the exact
  registry delete or any engine removal.
- Per-table recovery uses the fixed `drop/<namespace>/<name>` point key, not a
  filtered DynamoDB scan. A LocalStack regression inserts 12 unrelated
  metadata keys and still resumes the target tombstone.
- A registry-delete probe verifies the tombstone is already durable, then an
  injected same-incarnation version advance wins between resolve and exact
  delete; registry and dataset remain intact while the tombstone is retained.
- A successor resumes a crash immediately after tombstone publication and
  converges registry, dataset, and tombstone to absent.
- An injected cleanup removes one real Lance object and fails; retry drains the
  remaining dataset idempotently without restoring the registry.
- A stale cleanup paused in object deletion cannot touch a same-name replacement
  because each server-derived dataset uses a UUIDv7-qualified physical prefix.
- After lease takeover, the former leader can finish old-prefix object removal
  but its tombstone finalization is rejected by the fenced metastore; the new
  leader then completes the same tombstone.
- A live two-node Flight test pauses the first leader after registry detach,
  forces standby takeover, observes stale tombstone finalization fail, and then
  retries through the successor with no registry, dataset, or tombstone left.
- Leader maintenance processes at most one configured tombstone page and
  retains the continuation for the next sweep.
- Registry create/drop, append, maintenance, operation GC, and tombstone
  mutations continue to record zero ordinary production CAS/delete calls.

## Verification commands

- `cargo test -p lake-metasrv --lib --tests --no-fail-fast`: **55 PASS**
  (51 unit plus 4 live two-node tests; one LocalStack test ignored by default).
- `cargo clippy -p lake-cli -p lake-metasrv --all-targets -- -D warnings`:
  **PASS**.
- Clean `rm -rf data && mise run gate`: **PASS**; workspace tests, hooks,
  ingest/commit/SQL selftest, and site checks all passed.
- LocalStack DynamoDB roundtrip: **1 PASS**.
- LocalStack Dynamo tombstone recovery with unrelated metadata: **1 PASS**.
- LocalStack S3 + Dynamo external-manifest Lance roundtrip: **1 PASS**.

The only emitted warning is the existing macOS linker compact-unwind size
warning; strict clippy with warnings denied passes.
