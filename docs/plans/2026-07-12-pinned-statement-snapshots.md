# Pinned Flight SQL statement snapshots

Issue: #108

## Failure being removed

`GetFlightInfo` currently plans through `LakeCatalog`, emits the planned
schema, and encrypts only SQL and identity. `DoGet` decrypts the SQL and plans
again through the live catalog. The registration cache may observe a later
commit or a replacement table, so one standard Flight SQL capability can
describe two different inputs across its two phases.

## Protocol

The encrypted payload becomes `{identity, lifetime, sql, snapshots[]}`. Each
snapshot contains the SQL name plus engine kind, exact object location,
incarnation id, and engine version. Claims are canonicalized, unique, and
bounded before sealing and after opening. The envelope version advances; old
replicas cannot decode and ignore the new fields.

## Planning path

1. Parse and authorize the read-only SQL, returning every physical lake table
   reference outside CTE aliases.
2. Resolve each reference once through the bounded registration cache and
   capture an immutable `TableSnapshot`.
3. Open/cache providers by the exact snapshot generation.
4. Build a request-local DataFusion catalog containing only those providers
   while sharing the replica's bounded runtime/spill resources.
5. Plan `GetFlightInfo` through that catalog and seal the same snapshots.
6. `DoGet` validates identity, SQL/reference equality, and all bounds, then
   reconstructs the same request-local catalog directly from the claims.

No per-ticket server state is retained. A different replica can execute the
ticket; neither replica needs a current-version metastore read during DoGet.

## Failure semantics and rollout

If an exact engine snapshot is missing, DoGet returns an execution failure and
does not consult or fall forward to the registry's latest pointer. Drop and
recreate are isolated by both unique location and incarnation. Because older
replicas would otherwise ignore new claims, the inner ticket protocol is
versioned incompatibly; deploy with blue/green or drain/cutover rather than
mixed-version cross-routing.

## Verification order

Add RED tests for encrypted claim bounds, append drift, recreate/retention,
complete reference extraction, and legacy protocol rejection. Implement the
catalog snapshot API, request-local planning context, and Flight integration;
then run lane-1 lifecycle, workspace clippy/docs, the full project gate, and a
review focused on fail-forward paths, unbounded allocation, and replica-local
state.
