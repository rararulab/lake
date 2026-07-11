# Manifest history cleanup implementation plan

Issue: #42

## Outcome

After Lance successfully applies its tag/branch-aware retention policy, Lake
reconciles a bounded page of external manifest records against physical object
existence. Obsolete records disappear over repeated sweeps without guessing a
version cutoff or touching current state.

## Steps

1. Extend `MetaManifestStore` with an incarnation-bound durable cleanup cursor
   and a bounded `scan_prefix_page` reconciliation API.
2. For each page entry, parse the stored incarnation/path, HEAD that exact path,
   and guarded-delete only confirmed-absent objects.
3. Retain `MetaManifestStore` in object-store `LanceEngine` configuration and
   invoke reconciliation only after `cleanup_with_policy` succeeds.
4. Add deterministic RocksDB tests for bounds, retained records, cursor resume,
   incarnation fencing, and unchanged latest bytes.
5. Extend the LocalStack S3+Dynamo test to prove physical manifest deletion and
   external-record reclamation agree.
6. Run strict clippy, spec lifecycle, full gate, and independent review/verify.

## Safety properties

- Object absence after successful Lance cleanup is the deletion proof.
- Every metadata mutation is guarded by exact latest bytes and exact target
  bytes; a recreate or concurrent writer invalidates stale work.
- Cursor advancement happens only after every entry in the page was decided.
  Crash before advancement merely replays idempotent checks.
