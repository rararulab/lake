# Issue #57 verification: bounded Metasrv shutdown

## Candidate

- Base: `48b7cc9c`
- Implementation head: `285a2f74`
- Scope: Metasrv Flight/background lifecycle and maintenance cancellation boundaries

## Contract evidence

- `mise run spec-lifecycle specs/issue-57-bounded-shutdown.spec.md`: 3/3 scenarios passed.
- All three selectors were absent on the base, present exactly once on the
  candidate, and passed as focused tests.
- Two stuck owned tasks each retain an `Arc`; the shared deadline returns typed
  `BackgroundDrainTimeout`, aborts and joins both tasks, and restores both
  strong counts to one.
- With two tables, cancelling while the first `maintain` call is paused and
  then releasing it produces exactly one engine call.
- With two drop tombstones, cancelling while the first object removal is
  paused produces `scanned=2`, `completed=1`, one engine call, and one durable
  tombstone left for the next process.
- Normal server shutdown still returns within grace, resigns leadership,
  releases owned references, and permits immediate listener rebinding.

## Quality gates

- `cargo test -p lake-metasrv`: 56 passed, 1 environment-dependent LocalStack
  test ignored.
- Two-node integration: 5/5 passed.
- Strict clippy with all targets/features and `-D warnings`: passed.
- Local and independent `mise run gate`: passed.
- Workspace, diff, and allowed-boundary checks: clean.

## Review

- Initial correctness review found that drop-tombstone and append-operation GC
  could begin another durable cleanup after shutdown. The token was threaded
  through both page loops with biased cancellation-versus-lock boundaries and
  a new paused drop-GC regression test.
- Correctness re-review of `285a2f74`: APPROVE, original P1 closed, no blocker
  or high-severity finding.
- Independent release verifier of `285a2f74`: PASS.
- One deadline begins at explicit shutdown and is shared by Flight drain plus
  concurrent maintenance/campaign join. Accepted durable work may finish;
  unfinished owned tasks are aborted and reaped at the deadline. Campaign
  cancellation remains after server drop, preserving authority ordering.
- No durable metadata or Flight wire format changed.
