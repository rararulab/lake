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
- Caches the catalog with a bounded-staleness refresh window; concurrent
  refreshes coalesce and the server refreshes in the background so metadata
  scans stay off the per-query hot path.
- `QueryLimits` bounds concurrency, queue wait, execution duration, and SQL
  bytes per replica. A DoGet permit lives until its stream completes, expires,
  or is dropped; admission never calls the metadata tier.

## Layout

- `lib.rs` — read-only `QueryEngine` plus server wiring with optional metadata forwarding
- `flight.rs` — streaming Flight SQL reads, typed FILE `DoPut` proxy, and
  managed-stage discovery action
