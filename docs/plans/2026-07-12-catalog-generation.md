# Immutable catalog generation implementation plan

Issue: #49

## Outcome

Catalog discovery and DataFusion listing pin one immutable Arc containing both
table names and schemas. Refresh atomically replaces the Arc, eliminating
mixed-generation responses and full-listing clones.

## Steps

1. Replace mutable snapshot contents with `RwLock<Arc<CatalogGeneration>>` and
   expose read-only generation accessors.
2. Publish a newly built generation only after successful complete refresh.
3. Route Query/Flight schema and table discovery through one pinned Arc.
4. Add deterministic pointer-identity, refresh-isolation, failure, and Flight
   response tests.
5. Run strict clippy, spec lifecycle, full gate, review, and verification.

## Safety properties

- Old generations remain valid for in-flight readers until their Arc drops.
- Names and schemas are immutable and share one publication boundary.
- Failed refresh cannot mutate or replace the last-good allocation.
- Pointer cloning is O(1); iteration cost follows the requested response.
