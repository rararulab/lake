# Catalog directory generation implementation plan

## Goal

Replace every Query replica's steady five-second full registry scan with one
strongly consistent generation point read, without weakening mixed-version
rollout safety or immutable discovery generations.

## Architecture

`lake-meta` owns an opaque generation plus a monotonic authority marker.
Conditional registry create/delete and generation replacement are one atomic
backend operation; version-only writes remain independent. Before explicit
rollout finalization, catalogs always scan. After finalization they compare the
point-read generation, scan only on change, and validate the generation again
before publishing a candidate `Arc<CatalogGeneration>`.

## Tasks

1. Add RED contract tests for atomic signaled mutations in RocksDB and Dynamo,
   leader fencing, authority finalization, steady point-read refresh, append
   churn, legacy compatibility, and scan races.
2. Add a typed signaled conditional mutation to `MetaStore`; implement it as a
   RocksDB write batch and DynamoDB transaction, and compose it with the
   existing lease guard in `FencedMetaStore`.
3. Route registry create/delete through the atomic generation signal while
   keeping version and incarnation updates generation-neutral.
4. Add explicit monotonic finalization with both rollout acknowledgements.
5. Teach `LakeCatalog` to cache authority/generation, skip unchanged scans, and
   reject a scan candidate when its generation moves before publication.
6. Document rollout, rollback boundary, cost model, metrics interpretation,
   and Kubernetes procedure.
7. Run lane-1 lifecycle, LocalStack integration, strict Clippy, full gate,
   rustdoc, and independent correctness/performance/release review.
