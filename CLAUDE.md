# CLAUDE.md — Lake Development Guide

## Communication
- 用中文与用户交流

## What Lake Is

A lakehouse for embodied-AI data (robot episodes: images, video, pointclouds,
sensor streams), in the spirit of LanceDB. Read traffic is DDoS-like: fleets
of nodes hammer the same tables concurrently, so metadata must scale reads
without a hot central store.

## Architecture Invariants

These are load-bearing. Do not violate them without an explicit decision:

1. **The KV metastore holds only tiny mutable pointers** (`ptr/<table>` ->
   current version). Nothing else is mutable.
2. **Manifests are immutable.** Written once at
   `<table_root>/<table>/_manifests/v<N>.json`, never rewritten. This is
   what makes reader-side caching safe and unbounded.
3. **Commit protocol**: write the immutable manifest file first, then CAS
   the version pointer. Losers of the race fail cleanly and retry.
4. **Backends**: RocksDB for dev, DynamoDB (conditional put = CAS) for prod.
   Both live behind the `MetaStore` trait — no backend types outside
   `src/meta.rs`.
5. **SQL surface is DataFusion.** Tables resolve through
   `LakeCatalog`/`LakeSchema` (KV pointer -> manifest -> parquet file list).
   Wire protocol direction is Arrow Flight SQL, not MySQL protocol.

## Style

Follows the rara conventions (see `../rara/docs/guides/rust-style.md`):

- Edition 2024; `rustfmt.toml` / `clippy.toml` / lint table copied from rara.
- `snafu` in domain code (`LakeError` + per-crate `Result<T>` alias);
  `anyhow` only at application boundaries (`main.rs`).
- Propagation: `.context(XxxSnafu)?`; `.expect("context")` over `unwrap()`.
- Trait objects: `pub type XxxRef = Arc<dyn Xxx>`.
- Apache-2.0 license header on every source file.
- Functional-first, iterator chains, early returns with `?`.

## Quality Gate

Pre-commit hooks (prek) run: `cargo check`, `cargo +nightly fmt --check`,
`cargo clippy -D warnings`, `cargo +nightly doc -D warnings`. Commit
messages follow Conventional Commits (`scripts/check-conventional-commit.sh`).
CI (`.github/workflows/ci.yml`) runs the same plus `cargo test` and the
`cargo run` end-to-end self-check.

## Commands

```bash
cargo run             # end-to-end self-check: ingest -> commit -> SQL query
cargo clippy --all-targets --all-features --no-deps -- -D warnings
cargo +nightly fmt --all
prek run --all-files  # run all pre-commit hooks manually
```
