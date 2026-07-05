# lake-engine

The pluggable storage-engine boundary: `TableEngine` / `TableHandle` traits.
Pure abstraction, no implementation.

## Invariants

- This crate must NOT depend on any concrete engine (no `lance`). It defines
  the seam; implementations live in sibling crates (`lake-engine-lance`).
- The trait exposes only what the catalog + metadata layers call — keep it
  minimal. Per-table manifest/versioning is the engine's business, not
  exposed here beyond an opaque `Version`.

## Layout

- `engine.rs` — `TableEngine` (create/open) + `TableHandle`
  (schema/current_version/table_provider/append)
- `error.rs` — `EngineError` + constructors (`backend`, `already_exists`)
