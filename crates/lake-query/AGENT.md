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
- Managed-stage discovery returns one versioned, credential-free descriptor;
  it never proxies object bytes or exposes SDK process credentials.
- Caches the catalog with a bounded-staleness refresh window; concurrent
  refreshes coalesce and the server refreshes in the background so metadata
  scans stay off the per-query hot path.

## Layout

- `lib.rs` — read-only `QueryEngine` plus server wiring with optional metadata forwarding
- `flight.rs` — streaming Flight SQL reads, typed FILE `DoPut` proxy, and
  managed-stage discovery action
