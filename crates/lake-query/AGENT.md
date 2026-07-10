# lake-query

The query layer: stateless SQL compute. `QueryEngine` wires a DataFusion
`SessionContext` to `LakeCatalog` and executes SQL over
`lake.<namespace>.<table>`, reading data files straight from the engine.

## Invariants

- **Stateless.** No durable state; scale by running N instances behind a
  load balancer. All persistence is in the metastore + object storage.
- Reads go directly to storage (disaggregated compute/storage) — the query
  layer never asks a datanode.
- Caches the catalog with a bounded-staleness refresh window; concurrent
  refreshes coalesce and the server refreshes in the background so metadata
  scans stay off the per-query hot path.

## Layout

- `lib.rs` — read-only `QueryEngine` (`new`/`refresh`/`execute_sql`) + `serve`
- `flight.rs` — streaming Flight SQL statement path (`GetFlightInfo`/`DoGet`)
