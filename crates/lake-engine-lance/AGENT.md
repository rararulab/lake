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
- The external manifest store resolves current state through one fixed
  `{version,path}` pointer. Advancing it archives the exact prior pointer, then
  CASes latest; legacy per-version-only datasets scan once to install latest.
- Drop fences the fixed key through durable `deleting` and `deleted` markers;
  recreate replaces `deleted`, so stale migration cannot win an ABA CAS.
- Historical record creation is atomically guarded by the exact fixed-pointer
  bytes, preventing a writer that read before drop from publishing afterward.
- Fixed pointers and delete markers carry a UUIDv7 incarnation. Recreate gets a
  new one, so identical version/path bytes cannot produce cross-cycle ABA.

## Layout

- `lib.rs` — `LanceEngine` + `LanceTable` handle (create/open/append/version,
  `LanceTableProvider` for reads)
- `manifest_store.rs` — generic MetaStore-backed Lance commit arbitration,
  O(1) latest pointer, immutable historical version records
