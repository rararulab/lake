# Verification: operation-fenced reference-stage lifetime

## Failure evidence

The #90 full gate exposed a same-operation race where terminal cleanup removed
staging while another exact persister still needed it. Header-last publication
closed the live success-path race, but review found a remaining crash window: a
persister could create a non-header chunk and crash before publishing header
zero, leaving an invisible object with no future exact cleanup.

## Fix

- Keep each exact stage for the full durable append-operation lifetime.
- Remove terminal cleanup from Lance append/reconcile success paths.
- Add `TableHandle::expire_append`; Lance boundedly deletes the exact stage.
- Have Metasrv operation GC invoke that hook under the table lock before it
  deletes the operation record. Any engine error leaves the record retryable.
- Skip exact cleanup for absent/recreated registrations whose old dataset is
  already governed by drop-table cleanup.
- Directly cover the crash-before-header window with two non-header residue
  chunks and prove expiry's header-missing path drains both under the bound.

## Results

- `mise run spec-lifecycle specs/issue-91-header-last-stage-delete.spec.md`:
  PASS, 6/6 selectors executed at least one test, including the headerless
  multi-chunk crash residue.
- `cargo test -p lake-engine-lance -p lake-metasrv --all-targets`: PASS;
  Lance 38/38 unit + wiring, Metasrv 63/63 unit with the existing LocalStack
  cases ignored by design, and the two-node integration suite passed.
- After adding the exact crash-residue regression,
  `cargo test -p lake-engine-lance --all-targets`: PASS, 39/39 unit + wiring;
  two LocalStack tests remain ignored by design. Lance clippy remains PASS.
- `cargo clippy -p lake-engine -p lake-engine-lance -p lake-metasrv
  --all-targets -- -D warnings`: PASS.
- `mise run gate`: PASS; workspace tests, e2e selftest, hooks, and site checks
  all completed successfully.
- `mise run doc`: PASS with rustdoc warnings denied.
- The conservative non-coordinator `TableHandle::append` path now serializes
  its history-check/commit window per engine. Metasrv keeps using
  `append_reserved`, so production throughput remains governed by its durable
  per-table fence rather than this fallback lock.

Fixed-head review and independent verification remain pending.
