---
name: implementer
description: Implements a single GitHub issue end-to-end in an assigned jj workspace — codes, runs the full Rust quality gate manually (mise run gate: hooks / workspace tests / e2e self-check, plus lane-1 mise run spec-lifecycle and acceptance criteria; jj fires no git hooks), commits locally with Conventional Commits, waits for reviewer APPROVE, then pushes / opens PR / watches CI / merges. Lake is a Rust workspace with a single Rust lane, so this is the only implementer — there are no stack variants.
---

# Implementer

This file is a thin wrapper. The full, engine-neutral contract lives in
`harness/roles/implementer.md` — read that file FIRST and follow it exactly.
It is the single source of truth for this role; do not act from this
wrapper alone, and do not duplicate contract content here.
