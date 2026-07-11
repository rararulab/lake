# Provider generation cache implementation plan

Issue: #45

## Outcome

Repeated SQL planning in one query replica reuses a bounded, immutable
`TableProvider` for the exact registry-visible table generation. Concurrent
misses perform one storage open/provider build, while append and recreate
select fresh providers.

## Steps

1. Add a typed provider-generation key and bounded async cache to the shared
   catalog state.
2. Resolve providers through one fallible singleflight initializer; never
   cache storage failures or missing tables.
3. Add deterministic fake-engine tests for concurrency, version and
   incarnation separation, failure retry, and capacity.
4. Document the query-replica cache boundary and generation identity.
5. Run strict clippy, spec lifecycle, full gate, and independent review and
   verification.

## Safety properties

- Providers remain pinned to the registry version used in their key.
- Location and incarnation prevent reuse across physical replacement and
  legacy/recreated registration transitions.
- Cache eviction changes performance only; immutable providers remain safe for
  in-flight readers holding an `Arc`.
- Initializer errors and missing engine handles do not poison future reads.
- A per-table local registration epoch makes write invalidation linearizable
  against earlier in-flight cache fills.
