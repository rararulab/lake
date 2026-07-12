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
- `TableSnapshot` loading uses its claimed engine, unique location,
  incarnation, and version directly. It never consults a current registration
  or substitutes the handle's latest version.
- Listing names and schemas are published together as one immutable
  `Arc<CatalogGeneration>`; request readers pin the Arc and never mix refresh
  generations.
- Listing and registration caches have bounded staleness; refreshes coalesce
  so concurrent queries cannot stampede the metastore.
- Initial listing warm is synchronous and fail-closed. After warm, stale SQL
  checks serve last-good immediately and trigger one tracked revalidation;
  failed refreshes never replace the published snapshot.
- Read-only over the engine; table creation is an explicit `ops::create_table`
  call the metadata layer makes.

## Layout

- `catalog.rs` — `CatalogState` + `LakeCatalog` (`CatalogProvider`, immutable generation,
  registration/provider caches, `refresh`)
- `schema.rs` — `LakeSchema` (`SchemaProvider`: snapshot listing + live
  `table()` resolution)
- `ops.rs` — `create_table` (engine create + registry register)
