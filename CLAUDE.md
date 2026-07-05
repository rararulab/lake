# CLAUDE.md — Lake Development Guide

**新会话第一步：`./init.sh`**（or `just doctor`）— 一键检查工具链、git hooks、`cargo check`，以及 open `agent:claude` issue 数量。

## Communication
- 用中文与用户交流

## North Star

`goal.md` at the repo root defines what lake is, what lake is NOT, and the
observable signals that mean lake is working. Read it before drafting any
spec or proposing any change. `spec-author` uses it as a gate; you should too.

## Project Philosophy

Lake is a lakehouse for embodied-AI data (robot episodes: images, video,
pointclouds, sensor streams), in the spirit of LanceDB. Read traffic is
DDoS-like: fleets of nodes hammer the same tables concurrently, so metadata
must scale reads without a hot central store.

Design ethos: **immutability over coordination**. The KV metastore holds
only tiny version pointers; everything readers touch is immutable and
cacheable. When in doubt, choose the design that keeps per-query KV load
at zero.

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

## Style Anchors

Rust style triangulated from three voices — each covers a different blind spot:

- **BurntSushi** (Andrew Gallant): error ergonomics via `snafu`, CLI patterns, exhaustive matching, documentation-first design
- **dtolnay** (David Tolnay): API minimalism, derive-macro philosophy (`serde`, `bon`), "if it compiles it works" surface area
- **Niko Matsakis**: ownership-first API design, type safety as a feature, making invalid states unrepresentable

When these anchors conflict, prefer: safety (Niko) > ergonomics (BurntSushi) > minimalism (dtolnay).

Details in `docs/guides/rust-style.md`.

## External Reality

These artifacts are authoritative — your work is accountable to them, not just to the user:

- `goal.md` — north star: read this **first** for any new request; spec-author uses it as a gate
- `specs/project.spec` — project-level technical/process constraints inherited by every task spec
- `specs/README.md` — lane 1 (spec-driven, BDD-bound test) vs lane 2 (lightweight chore) triage criteria; read this **before** opening an issue
- `.pre-commit-config.yaml` — code quality gate (check, fmt, clippy, doc warnings)
- `harness/roles/*.md` — engine-neutral role contracts (spec-author, implementer, reviewer, verifier); `.claude/agents/*.md` are thin wrappers over them
- `AGENT.md` — 行为契约：推理框架、执行边界、协作工作流

## Development Workflow

All changes — no matter how small — follow the issue → worktree → PR → merge
flow. No exceptions. See `docs/guides/workflow.md`.

- Branch work happens in `.worktrees/issue-N-<slug>` — never on the main
  checkout (`.claude/hooks/guard-main-branch.sh` enforces this).
- Lane triage per `specs/README.md`; lane-1 work gets a Task Contract in
  `specs/issue-N-<slug>.spec.md`.
- Quality gate before any push: `just gate` (= `prek run --all-files` +
  `cargo test --all-targets` + `cargo run` e2e self-check).
- Conventional Commits enforced by commit-msg hook.

## Commands

```bash
./init.sh             # session-start health check (= just doctor)
just gate             # full quality gate: hooks + test + e2e
just fmt              # cargo +nightly fmt --all
just clippy           # clippy -D warnings
cargo run             # end-to-end self-check: ingest -> commit -> SQL query
just agenda           # open agent:claude issues
```
