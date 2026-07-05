# lake-manifest

Immutable table snapshots and the manifest-then-CAS commit protocol.

## Invariants

- Manifests are written once at `<table>/_manifests/v<N>.json` and NEVER
  rewritten — this is what makes unbounded reader-side caching safe.
- Commit order is fixed: manifest file first, THEN CAS the version
  pointer. Losers of the race fail cleanly (`Exists` / `CommitConflict`)
  and the caller retries.
- Readers must never be able to observe a half-written snapshot.

## Layout

- `model.rs` — `Manifest` struct + on-disk path layout
- `commit.rs` — `current_version` / `load_current` / `commit` (async,
  tokio::fs)
- `error.rs` — `ManifestError` + `Result<T>`
