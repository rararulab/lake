# lake-catalog

The db→table catalog: resolves table names to DataFusion tables over the
registry + storage engine. The cache shield in front of the metadata
authority.

## Invariants

- DataFusion's sync listing methods (`schema_names`, `table_names`) read an
  in-memory snapshot only — they must NEVER block on the metastore (doing so
  panics inside the async runtime). Refresh the snapshot with
  `LakeCatalog::refresh`.
- Per-table lookups hit the moka cache before the registry.
- DataFusion providers are cached per exact table generation (name, engine,
  location, incarnation, version); concurrent misses coalesce and the cache is
  capacity-bounded.
- Listing and registration caches have bounded staleness; refreshes coalesce
  so concurrent queries cannot stampede the metastore.
- Read-only over the engine; table creation is an explicit `ops::create_table`
  call the metadata layer makes.

## Layout

- `catalog.rs` — `CatalogState` + `LakeCatalog` (`CatalogProvider`, snapshot,
  registration/provider caches, `refresh`)
- `schema.rs` — `LakeSchema` (`SchemaProvider`: snapshot listing + live
  `table()` resolution)
- `ops.rs` — `create_table` (engine create + registry register)
