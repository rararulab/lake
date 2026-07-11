# lake-engine-lance

The Lance implementation of `lake_engine::TableEngine`. One lake table = one
Lance dataset.

## Invariants

- **The ONLY crate allowed to name a `lance::` type.** Everything else
  programs against the `lake-engine` traits, so Lance stays swappable.
- All Arrow types come through `datafusion::arrow`, never a direct `arrow`
  dep — the engine and query layer must share one Arrow version (datafusion
  53.1 + lance 8 both resolve to arrow 58).
- Append commits disable automatic rebase and persist tenant, operation ID,
  digest, and reference-stage identity in Lance transaction properties.
- Reference lineage is staged before manifest visibility and repaired from
  transaction history before a recovered version is returned.

## Layout

- `lib.rs` — `LanceEngine` + `LanceTable` handle (create/open/append/version,
  `LanceTableProvider` for reads)
