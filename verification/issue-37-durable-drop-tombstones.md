# Issue #37 Verification

Candidate base: `d1b936daef1e` (merged issue #35)

## Contract

- Explicit candidate `agent-spec lifecycle`: **8/8 PASS**.
- Spec lint: **100%** determinism, testability, and coverage.
- All 13 candidate paths are inside the declared boundaries.

## Protocol evidence

- Drop persists one immutable, incarnation-bound tombstone before the exact
  registry delete or any engine removal.
- A successor resumes a crash immediately after tombstone publication and
  converges registry, dataset, and tombstone to absent.
- An injected cleanup removes one real Lance object and fails; retry drains the
  remaining dataset idempotently without restoring the registry.
- A stale cleanup paused in object deletion cannot touch a same-name replacement
  because each server-derived dataset uses a UUIDv7-qualified physical prefix.
- After lease takeover, the former leader can finish old-prefix object removal
  but its tombstone finalization is rejected by the fenced metastore; the new
  leader then completes the same tombstone.
- A live two-node Flight test drops through one leader, hands leadership to the
  standby, and repeats drop successfully with no registry, dataset, or tombstone
  remaining.
- Leader maintenance processes at most one configured tombstone page and
  retains the continuation for the next sweep.
- Registry create/drop, append, maintenance, operation GC, and tombstone
  mutations continue to record zero ordinary production CAS/delete calls.

## Verification commands

- `cargo test -p lake-metasrv --lib --tests --no-fail-fast`: **52 PASS**
  (48 unit plus 4 live two-node tests).
- `cargo clippy -p lake-cli -p lake-metasrv --all-targets -- -D warnings`:
  **PASS**.
- Clean `rm -rf data && mise run gate`: **PASS**; workspace tests, hooks,
  ingest/commit/SQL selftest, and site checks all passed.
- LocalStack DynamoDB roundtrip: **1 PASS**.
- LocalStack S3 + Dynamo external-manifest Lance roundtrip: **1 PASS**.

The only emitted warning is the existing macOS linker compact-unwind size
warning; strict clippy with warnings denied passes.
