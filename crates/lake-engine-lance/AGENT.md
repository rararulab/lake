# lake-engine-lance

The Lance implementation of `lake_engine::TableEngine`. One lake table = one
Lance dataset.

## Invariants

- **The ONLY crate allowed to name a `lance::` type.** Everything else
  programs against the `lake-engine` traits, so Lance stays swappable.
- All Arrow types come through `datafusion::arrow`, never a direct `arrow`
  dep — the engine and query layer must share one Arrow version (datafusion
  53.1 + lance 8 both resolve to arrow 58).
- v0 uses Lance's default commit (atomic on local FS). The
  `ExternalManifestStore` adapter over `MetaStore` (for S3) is v1.

## Layout

- `lib.rs` — `LanceEngine` + `LanceTable` handle (create/open/append/version,
  `LanceTableProvider` for reads)
