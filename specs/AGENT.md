# specs/

agent-spec Task Contracts. Read `README.md` here for the lane-1 vs lane-2
triage rules and the tooling (`mise run spec-init / spec-lint /
spec-lifecycle / spec-selftest`).

- `project.spec` — constraints inherited by every task spec; not product
  direction (that's `goal.md`)
- `issue-N-<slug>.spec.md` — lane-1 Task Contracts (BDD scenarios bound
  to real test functions)
- `fixtures/zero-match.spec.md` — test fixture for the lifecycle guard;
  intentionally broken, do NOT "fix" it
