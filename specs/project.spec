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

- The KV metastore holds only tiny mutable registry pointers and compact
  CAS-managed coordination records (operation identity/digest/state/version
  and engine manifest mappings). Data-plane rows, object bytes, credentials,
  signed URLs, and arbitrary request payloads are forbidden.
- Manifests are immutable: written once at
  `<table_root>/<table>/_manifests/v<N>.json`, never rewritten.
- Commit protocol: durably reserve the operation, stage reference lineage,
  write the immutable manifest, then CAS the version pointer and terminal
  operation state. Retries with the same authenticated identity and digest
  converge on the original version; conflicting or unrecoverable state fails
  closed.
- Backend types (RocksDB, DynamoDB) never leak outside the `lake-meta`
  crate; everything else programs against the `MetaStore` trait.
- SQL surface is DataFusion; wire-protocol direction is Arrow Flight SQL.

### Style and toolchain

- Errors: `snafu` exclusively in domain crates (per-crate `{CrateName}Error` +
  `Result<T>` alias). `anyhow` only at application boundaries (`lake-cli`).
  Never `thiserror` or hand-rolled `impl Error`.
- Construction: `#[derive(bon::Builder)]` for any struct with 3 or more
  fields crossing module boundaries. Struct literals within the defining
  module are fine.
- Async-first: public APIs in domain crates are async
  (`#[async_trait]` + `Send + Sync` on trait definitions). Sync bridges
  (e.g. `block_on` for sync framework traits) are boundary-only and carry
  a `ponytail:` note naming the upgrade path.
- No wildcard imports (`use foo::*`).
- `.expect("context")` over `unwrap()` in non-test code.
- Apache-2.0 license header on every source file.

### Code text

- All source comments and doc comments in English.
- New or modified `pub` items require `///` doc comments.
- Inline comments explain *why*, not *what*. Skip comments that restate code.

### Process

- Conventional Commits, enforced by CI and reviewer (jj fires no git hooks). Format:
  `<type>(<scope>): <description> (#N)` with `Closes #N` in body.
- Allowed types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`,
  `perf`, `style`, `build`, `revert`. Breaking uses `!`.
- Workspace-only edits. The main agent and all subagents never edit files
  on the main checkout. Every change goes through
  `jj workspace add .worktrees/issue-N-<slug>`.
- One issue → one PR targeting `main`. No stacked PRs.
- Every folder carries an `AGENT.md` (10–20 lines: purpose, invariants,
  layout). New crates/directories require one before merge; keep it a
  catalog card, not a manual.
- Quality gate before any push: `mise run gate` (prek hooks + workspace
  tests + e2e self-check); lane 1 adds `mise run spec-lifecycle <spec>`.
