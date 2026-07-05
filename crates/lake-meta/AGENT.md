# lake-meta

KV metadata store. Owns the `MetaStore` trait (async-first), the dev
backend `RocksMeta`, and `MetaError`.

## Invariants

- The metastore holds ONLY tiny mutable pointers (`ptr/<table>` ->
  version). If you are storing anything else here, the design is wrong —
  see `docs/architecture.md`.
- Backend types (RocksDB today, DynamoDB later) never leak out of this
  crate. Consumers see `MetaStore` / `MetaStoreRef` only.
- `cas` is the only mutation primitive. No blind puts in the public API.

## Layout

- `store.rs` — `MetaStore` trait + `MetaStoreRef` alias
- `rocks.rs` — RocksDB impl (dev); CAS emulated with a process-local mutex
- `error.rs` — `MetaError` + `Result<T>`
