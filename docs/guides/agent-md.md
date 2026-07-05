# AGENT.md Requirements

Lake is a Rust workspace with a single lane; the repo-level entry points
are `CLAUDE.md` and `AGENT.md` at the root (`AGENTS.md` is a symlink to
`CLAUDE.md`) — thin progressive-disclosure catalogs that point into
`docs/`. The per-directory `AGENT.md` convention applies below that
level: any significant module directory MUST have an `AGENT.md` file
that guides AI agents working in that area.

## When to Create

- **New major module**: when a module grows from a single file into a
  directory with its own domain logic (e.g. a file in
  `crates/lake-meta/src/` splitting into a subdirectory),
  add an `AGENT.md` in that directory
- **Significant refactor**: if you restructure a module's internals,
  update or create its `AGENT.md`
- **Repo-level changes**: architecture invariants live in
  `docs/architecture.md`, style rules in `docs/guides/rust-style.md`,
  and the quality gate in `mise.toml` / `.pre-commit-config.yaml` —
  update them in the same PR when they change

## Template

```markdown
# {module-name} — Agent Guidelines

## Purpose
One sentence: what this module does and why it exists.

## Architecture
Key sub-modules, data flow, and public API surface. Point to real source files rather than abstract descriptions.

## Critical Invariants
Constraints that MUST NOT be violated (thread safety, ordering guarantees, immutability boundaries).
Explain the consequence of violation.

## What NOT To Do
Explicit anti-patterns with reasoning. Format: "Do NOT X — because Y".

## Dependencies
Upstream/downstream module relationships and external service dependencies (RocksDB, DynamoDB, object store).
```

## Rules

- Keep each `AGENT.md` under 300 lines — only include what an agent cannot infer from reading the code
- Write in English
- Executable commands and real file paths over abstract descriptions
- Update `AGENT.md` in the same PR when you change the module's architecture or invariants
- Do NOT let AI auto-generate `AGENT.md` from scratch — the author (human or agent who built the feature) writes it based on actual design decisions
