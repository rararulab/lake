spec: project
name: "lake"
---

## Intent

lake is a lakehouse for embodied-AI data — see `goal.md` at the repo root
for the full north star, including what lake is, what lake is NOT, and the
observable signals that define "working".

This project spec defines the technical and process constraints that every
task spec inherits. It is not the place to argue about product direction;
that lives in `goal.md`.

## Constraints

### Architecture invariants

- The KV metastore holds only tiny mutable pointers (`ptr/<table>` ->
  current version). Nothing else is mutable.
- Manifests are immutable: written once at
  `<table_root>/<table>/_manifests/v<N>.json`, never rewritten.
- Commit protocol: write the immutable manifest file first, then CAS the
  version pointer. Losers of the race fail cleanly and retry.
- Backend types (RocksDB, DynamoDB) never leak outside `src/meta.rs`;
  everything else programs against the `MetaStore` trait.
- SQL surface is DataFusion; wire-protocol direction is Arrow Flight SQL.

### Style and toolchain

- Errors: `snafu` exclusively in domain code (`LakeError` + `Result<T>`
  alias). `anyhow` only at application boundaries (`main.rs`, bootstrap).
  Never `thiserror` or hand-rolled `impl Error`.
- Construction: `#[derive(bon::Builder)]` for any struct with 3 or more
  fields crossing module boundaries. Struct literals within the defining
  module are fine.
- Async: `#[async_trait]` + `Send + Sync` on async trait definitions.
- No wildcard imports (`use foo::*`).
- `.expect("context")` over `unwrap()` in non-test code.
- Apache-2.0 license header on every source file.

### Code text

- All source comments and doc comments in English.
- New or modified `pub` items require `///` doc comments.
- Inline comments explain *why*, not *what*. Skip comments that restate code.

### Process

- Conventional Commits, enforced by local `commit-msg` hook. Format:
  `<type>(<scope>): <description> (#N)` with `Closes #N` in body.
- Allowed types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`,
  `perf`, `style`, `build`, `revert`. Breaking uses `!`.
- Worktree-only edits. The main agent and all subagents never edit files
  on `main` directly. Every change goes through
  `git worktree add .worktrees/issue-N-<slug>`.
- One issue → one PR targeting `main`. No stacked PRs.
- Quality gate before any push: `prek run --all-files` +
  `cargo test --all-targets` + `cargo run` (end-to-end self-check).
