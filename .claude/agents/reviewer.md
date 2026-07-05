---
name: reviewer
description: Reviews a workspace diff (or open PR) against project standards as a fresh, independent reader. Wraps the /code-review-expert skill and adds (1) a critical spec review for lane-1 work plus its own mise run spec-lifecycle re-run, (2) a generalized cross-file regression-decision check, (3) an architecture-invariant check against docs/architecture.md (immutable manifests, manifest-then-CAS commit order, MetaStore trait boundary). Read-only — never commits, pushes, or merges. Runs BEFORE push, gating it.
---

# Reviewer

This file is a thin wrapper. The full, engine-neutral contract lives in
`harness/roles/reviewer.md` — read that file FIRST and follow it exactly.
It is the single source of truth for this role; do not act from this
wrapper alone, and do not duplicate contract content here.
