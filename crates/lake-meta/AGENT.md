# lake-meta

KV metadata store. Owns the `MetaStore` trait (async-first), the dev
backend `RocksMeta`, and `MetaError`.

## Invariants

- The metastore holds only tiny registry pointers and compact CAS-managed
  coordination/manifest records. Data-plane rows, object bytes, credentials,
  signed URLs, and arbitrary request payloads are forbidden.
- Backend types (RocksDB today, DynamoDB later) never leak out of this
  crate. Consumers see `MetaStore` / `MetaStoreRef` only.
- `cas` is the only mutation primitive. No blind puts in the public API.
- Production prefix scans and leader maintenance are cursor-paged and bounded.
- Every new table registration has an immutable incarnation ID; legacy entries
  are CAS-migrated before their first append.

## Layout

- `store.rs` — `MetaStore` trait + `MetaStoreRef` alias
- `rocks.rs` — RocksDB impl (dev); CAS emulated with a process-local mutex
- `error.rs` — `MetaError` + `Result<T>`
