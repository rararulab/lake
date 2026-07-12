# Bounded SDK schema cache implementation plan

**Goal:** Remove per-row schema planning RPCs without weakening drop/recreate
correctness or adding unbounded client state.

## Task 1: Lock configuration and ownership

1. Add validated builder settings for finite capacity and TTL.
2. Construct one cache per connected client and share it across clones.
3. Expose per-table and full-cache invalidation.

## Task 2: Singleflight schema resolution

1. Route typed insert schema resolution through the cache.
2. Coalesce concurrent same-table misses while allowing different tables to
   resolve independently.
3. Insert only successful schema responses; preserve typed SDK errors.

## Task 3: Prove lifecycle behavior

1. Count real Flight schema requests across repeated calls and clones.
2. Exercise failure recovery, TTL expiry, per-table invalidation, and clear.
3. Verify capacity eviction remains bounded.

## Task 4: Operate and ship

1. Document defaults, stale-schema horizon, and explicit invalidation.
2. Run spec lifecycle, strict clippy, full gate, independent review, and
   independent verification before merge.
