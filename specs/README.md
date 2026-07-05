# specs/ — Task Contracts

This directory holds [`agent-spec`](https://github.com/ZhangHanDong/agent-spec)
Task Contracts for lake work. Read `goal.md` first, then this file.

## Two lanes

Not every change needs a `.spec.md`. Pick the right lane.

### Lane 1 — Spec-driven (feature, bugfix, anything with testable behavior)

Use lane 1 when **at least one acceptance criterion can be bound to a real
test function** that fails before the change and passes after. Examples:

- New commit-protocol behavior (e.g. retry-on-conflict semantics)
- Bug fix where you can write a failing reproducer
- New catalog resolution behavior with observable SQL semantics
- Refactor that must preserve a documented contract (with parity tests)

Lane 1 flow:

1. `spec-author` writes `specs/issue-N-<slug>.spec.md` inheriting `project`.
2. `spec-author` creates the GitHub issue, referencing the spec file.
3. `implementer` reads the spec, implements, runs the quality gate
   locally, commits inside the worktree, **does not push**.
4. `reviewer` reads the worktree diff plus the spec; verifies the BDD
   scenarios pass; produces a verdict.
5. On APPROVE: implementer pushes, opens the PR, watches CI, merges.

### Lane 2 — Lightweight chore (structural, cleanup, CI, rename, config)

Use lane 2 when there is **no test function that meaningfully verifies
"done"**. Examples:

- Deleting a workflow file
- Renaming a directory
- Updating dependencies
- Editing documentation
- Restructuring a module without behavior change

Lane 2 flow:

1. `spec-author` writes the GitHub issue body directly with Intent + prior
   art + decisions + boundaries — same content shape as a Task Contract,
   minus BDD scenarios. No `specs/*.spec.md` file is created.
2. `implementer` reads the issue, implements, runs `cargo check` /
   `prek run --all-files`, commits, **does not push**.
3. `reviewer` reads the worktree diff plus the issue body.
4. On APPROVE: implementer pushes, opens the PR, watches CI, merges.

## How spec-author chooses the lane

A single question: **"Can I write at least one `Test:` selector that binds
to a real test function that meaningfully verifies the outcome?"**

- Yes → lane 1.
- No → lane 2.

If unsure, lane 2 — err on the side of less overhead. Lane 1's value is the
BDD binding; a spec without a real test binding is ceremony.
