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
- Reference extraction skips null parent FILE cells used by logical table rows,
  but any present FILE with a null identity child fails before publication.
- Reference staging lives until the durable coordinator operation expires.
  Expiry holds the same table lock as append, cleans the exact stage first,
  and only then permits the operation record to be deleted.
- Stage persistence creates non-header chunks before publishing chunk zero;
  expiry cleanup withdraws chunk zero first, then bounded-prefix drains. A
  crash before header publication leaves a headerless prefix that expiry
  drains under the per-append chunk bound.
- The external manifest store resolves current state through one fixed
  `{version,path}` pointer. Advancing it archives the exact prior pointer, then
  CASes latest; legacy per-version-only datasets scan once to install latest.
- Drop fences the fixed key through durable `deleting` and `deleted` markers;
  recreate replaces `deleted`, so stale migration cannot win an ABA CAS.
- Historical record creation is atomically guarded by the exact fixed-pointer
  bytes, preventing a writer that read before drop from publishing afterward.
- Fixed pointers and delete markers carry a UUIDv7 incarnation. Recreate gets a
  new one, so identical version/path bytes cannot produce cross-cycle ABA.
- Historical records carry the same incarnation (legacy path-only records are
  upgraded lazily), so finalize convergence cannot cross a recreate boundary.
- `LanceMaintenancePolicy` is immutable and bounded to 1..=10000 recent
  versions (default 10); cleanup preserves Lance tags and referenced branches.
- After successful Lance retention cleanup, maintenance reconciles at most 256
  history records by exact manifest existence and advances a durable cursor;
  deletes and cursor writes are guarded by exact incarnation-bound latest.

## Layout

- `lib.rs` — `LanceEngine` + `LanceTable` handle (create/open/append/version,
  `LanceTableProvider` for reads)
- `manifest_store.rs` — generic MetaStore-backed Lance commit arbitration,
  O(1) latest pointer, immutable historical version records
