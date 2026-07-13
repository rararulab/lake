# lake-query

The query layer: stateless SQL compute. `QueryEngine` wires a DataFusion
`SessionContext` to `LakeCatalog` and executes SQL over
`lake.<namespace>.<table>`, reading data files straight from the engine.

## Invariants

- **Stateless.** No durable state; scale by running N instances behind a
  load balancer. All persistence is in the metastore + object storage.
- Reads go directly to storage (disaggregated compute/storage) — the query
  layer never asks a datanode.
- Typed SQL `FILE` writes are metadata-only Flight streams proxied to metasrv;
  query does not persist rows or receive the original object bytes.
- Query derives delegated tenant scope from the authenticated principal and
  preserves the operation descriptor byte-for-byte while forwarding.
- Managed-stage discovery returns one versioned, credential-free descriptor;
  it never proxies object bytes or exposes SDK process credentials.
- Production `serve_with_config` authenticates every inbound Flight RPC and
  uses one TLS/auth `ClientSecurity` for Query-to-Metasrv forwarding.
- Served Query constructs its catalog only from the authenticated remote
  `CatalogSource` and connects before bind. It never falls back to direct
  registry reads; in-process SQL/selftest may use the explicit local adapter.
- Caches the catalog with a bounded-staleness refresh window; concurrent
  refreshes coalesce and the server refreshes in the background so metadata
  scans stay off the per-query hot path.
- Startup waits for the first catalog generation. Runtime SQL planning uses
  stale-while-revalidate and never awaits authority I/O; shutdown aborts and
  joins any request-triggered catalog task.
- Reuses a capacity-bounded `TableProvider` per exact immutable table
  generation; concurrent planning in one replica performs one storage open.
- Flight schema/table discovery pins one immutable catalog generation, so
  names and schemas share a publication boundary and requests avoid a full
  listing deep clone. Table filters run before schema resolution and row
  allocation. Discovery shares Query admission, emits lazy bounded batches,
  and rejects matches beyond its configured row maximum.
- `QueryLimits` bounds concurrency, queue wait, execution duration, and SQL
  bytes per replica. A DoGet permit lives until its stream completes, expires,
  or is dropped; admission never calls the metadata tier.
- Statement tickets keep the standard Flight SQL outer type but encrypt SQL in
  a versioned AEAD envelope bound to exact principal, tenant, audience, and
  expiry. The encrypted payload carries every referenced table's engine,
  unique location, incarnation, and exact version; both Flight phases plan
  through request-local pinned catalogs and DoGet never re-resolves current
  pointers. Non-loopback replicas require one shared bounded rotation key
  ring; validation happens before admission or planning and never logs key
  material.
- `QueryResources` gives the replica one shared DataFusion `FairSpillPool` and
  one size-limited local spill manager. Every constructor is bounded; the CLI
  validates deployment overrides before binding Flight.
- Durable async submission reserves one bounded tenant-index entry before
  object upload. The index is CAS-authoritative across replicas; raw tenant
  identity and its digest never become output, logs, or metric labels.
- New async records persist an immutable result byte ceiling. Workers and
  manifest validation use the record value after restart; schema-v1 records
  keep the legacy hard ceiling without fabricating a reservation.

## Layout

- `lib.rs` — read-only `QueryEngine` plus server wiring with optional metadata forwarding
- `flight.rs` — streaming Flight SQL reads, typed FILE `DoPut` proxy, and
  managed-stage discovery action
