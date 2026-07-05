# Architecture

System design for lake. `goal.md` says why; this file says how. Agent
entry points (`AGENT.md`, `CLAUDE.md`) are catalogs — the substance lives
here.

## Design ethos

**Immutability over coordination.** The KV metastore holds only tiny
version pointers; everything readers touch is immutable and cacheable.
Read traffic is DDoS-like — fleets of training/eval nodes hammer the same
tables concurrently — so the design keeps per-query KV load at zero. When
in doubt, choose the design that scales reads through caching, not
through a bigger central store.

## Read path

```
SQL (DataFusion; wire-protocol direction: Arrow Flight SQL)
        │
   LakeCatalog / LakeSchema          crates/lake-catalog
        │  table name
        ▼
   KV version pointer  ptr/<table> -> N        crates/lake-meta
        │  (tiny, mutable, cacheable with short TTL)
        ▼
   Immutable manifest  <table>/_manifests/vN.json   crates/lake-manifest
        │  (immutable => cacheable forever on every reader node)
        ▼
   Parquet data files (object store / local fs)
```

The KV store sees O(1) traffic per version change, not O(N) per query —
that is the load-bearing property of the whole system.

## Commit protocol

1. Writer computes `next = current + 1`.
2. Writer writes the immutable manifest file `_manifests/v<next>.json`.
   If it already exists, another writer is ahead — fail cleanly.
3. Writer CAS-swaps the version pointer `ptr/<table>: current -> next`.
   CAS failure = lost the race — fail cleanly, caller retries.

Readers never observe a half-written snapshot: the pointer only ever
moves to a manifest that is already fully written.

## Invariants

These are load-bearing. Do not violate them without an explicit decision:

1. **The KV metastore holds only tiny mutable pointers** (`ptr/<table>` ->
   current version). Nothing else is mutable.
2. **Manifests are immutable.** Written once, never rewritten. This is
   what makes reader-side caching safe and unbounded.
3. **Commit = manifest first, then CAS.** Losers fail cleanly and retry.
4. **Backend types stay inside `lake-meta`.** RocksDB (dev) and DynamoDB
   (prod, conditional put = CAS) live behind the `MetaStore` trait; no
   other crate names a backend type.
5. **SQL surface is DataFusion.** Wire protocol direction is Arrow Flight
   SQL, not MySQL protocol.

## Crate map

| Crate | Owns | Depends on |
|-------|------|------------|
| `lake-meta` | `MetaStore` trait, `RocksMeta`, `MetaError` | — |
| `lake-manifest` | `Manifest`, commit protocol, `ManifestError` | lake-meta |
| `lake-catalog` | `LakeCatalog`/`LakeSchema` (DataFusion providers) | lake-meta, lake-manifest |
| `lake-cli` | `lake` binary: end-to-end self-check (`mise run e2e`) | all of the above |

Errors are per-crate snafu enums (`{CrateName}Error` + `Result<T>`
alias); `anyhow` only in `lake-cli`. Workspace lints/deps live in the
root `Cargo.toml`; every crate sets `[lints] workspace = true`.

Crate conventions: **thin lib** (`lib.rs` is module docs + re-exports
only; logic lives in sub-files) and **async-first** (`MetaStore` and the
manifest ops are async; the prod metastore is network-bound). Each crate
carries an `AGENT.md` card with its own invariants and layout.

## Deliberate simplifications (ponytail markers)

Grep for `ponytail:` to find shortcuts with known ceilings:

- No manifest/provider caching yet — every query re-reads pointer +
  manifest. Manifests are immutable, so a `(table, version) -> provider`
  cache is safe to add when read QPS matters.
- `RocksMeta::cas` uses a process-local mutex; DynamoDB conditional put
  replaces it in prod.

## Direction (not yet built)

- DynamoDB `MetaStore` impl (prod CAS).
- Arrow Flight SQL server in front of the catalog.
- Lance format for multimodal blobs (images, video, pointclouds) once
  blob workloads land; Parquet stays for tabular sensor data.
