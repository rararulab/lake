# lake-query

The query layer: stateless SQL compute. `QueryEngine` wires a DataFusion
`SessionContext` to `LakeCatalog` and executes SQL over
`lake.<namespace>.<table>`, reading data files straight from the engine.

## Invariants

- **Stateless.** No durable state; scale by running N instances behind a
  load balancer. All persistence is in the metastore + object storage.
- Reads go directly to storage (disaggregated compute/storage) — the query
  layer never asks a datanode.
- Caches the catalog and calls `refresh()` before executing so new tables
  are visible; this is the shield that keeps the metadata authority cold.

## Layout

- `lib.rs` — `QueryEngine` (`new`/`refresh`/`execute_sql`) + `serve`
  (v1 Flight SQL wire is a ponytail stub)
