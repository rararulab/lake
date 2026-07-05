---
name: implementer
description: Implements a single GitHub issue end-to-end in an assigned worktree — codes, runs the full Rust quality gate (prek run --all-files / cargo test --all-targets / cargo run self-check, plus lane-1 acceptance criteria), commits locally with Conventional Commits, waits for reviewer APPROVE, then pushes / opens PR / watches CI / merges. Lake is a single Rust crate, so this is the only implementer — there are no stack variants.
---

# Implementer

This file is a thin wrapper. The full, engine-neutral contract lives in
`harness/roles/implementer.md` — read that file FIRST and follow it exactly.
It is the single source of truth for this role; do not act from this
wrapper alone, and do not duplicate contract content here.
