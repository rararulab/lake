# lake-catalog

DataFusion catalog over the metastore. Table resolution: KV version
pointer -> immutable manifest -> parquet file list.

## Invariants

- This crate is read-path only — it never mutates the metastore.
- SQL surface is DataFusion; wire-protocol direction is Arrow Flight SQL
  (see `docs/architecture.md`), not MySQL protocol.
- `table_names` / `table_exist` bridge DataFusion's sync trait methods
  with `futures::executor::block_on` — fine for RocksDB (ready futures),
  must be revisited (cached table list) before a network-bound backend.

## Layout

- `catalog.rs` — `LakeCatalog` (one `public` schema)
- `schema.rs` — `LakeSchema` (`SchemaProvider`: resolution + schema
  inference)
