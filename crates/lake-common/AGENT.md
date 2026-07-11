# lake-common

Shared identifiers used across every tier. Foundation crate — depends on
nothing in the workspace, so everyone can depend on it.

## Invariants

- Thin newtypes only (over `String` / `u64`). No I/O, no tier-specific deps.
- `Version` is opaque: the registry stores and compares it, never interprets
  it. Each engine decides what it encodes.

## Layout

- `ids.rs` — `Namespace`, `TableName`, `TableRef`, `Version`
- `location.rs` — `TableLocation` (a table's dataset URI)
- `file_write.rs` — transport-neutral FILE append command payload
