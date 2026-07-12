# Issue #86 verification

Candidate base: `3a3788e83e513b6ea5b91bb06701494f180e3875`

## Root cause

The operation-derived staging journal is deliberately shared by exact replays.
Immediate terminal cleanup created two observation-to-read races:

1. A contender received `AlreadyExists` from staged `PutMode::Create`; the
   winner committed, finalized, and deleted the journal before the contender's
   comparison `get`.
2. Reconciler A observed incomplete final sidecars; reconciler B finalized and
   deleted the journal before A read staged chunk zero.

Both paths returned backend NotFound even though the exact operation was
durably terminal.

## TDD evidence

- `same_operation_append_survives_terminal_stage_cleanup` uses `Notify`
  barriers to force race 1. Before the fix it failed deterministically with
  staging `NotFound`; after exact transaction reconciliation it passes and both
  calls return version 2.
- `concurrent_recovery_survives_terminal_stage_cleanup` forces race 2. Before
  the fix reconciler A failed deterministically with staging `NotFound`; after
  rechecking complete final sidecars it returns the same committed version.
- `different_payload_replay_remains_idempotency_conflict` confirms transaction
  reconciliation cannot turn a digest mismatch into success.
- `missing_stage_without_final_lineage_fails_closed` confirms missing staging
  alone remains an error when complete final sidecars do not exist.

The production fix adds no lock, sleep, background task, or unbounded retry.
Terminal cleanup remains immediate and idempotent.

## Focused verification

- `mise run spec-lint specs/issue-86-idempotent-stage-cleanup-race.spec.md` —
  PASS, quality 100%.
- `mise run spec-lifecycle specs/issue-86-idempotent-stage-cleanup-race.spec.md`
  — PASS, all four selectors executed at least one passing test.
- `cargo test -p lake-engine-lance --all-targets` — PASS twice after the fix:
  36/36 unit tests and 1/1 LocalStack wiring test; two explicit LocalStack
  protocol tests remain ignored outside the integration runner.
- `cargo clippy -p lake-engine-lance --all-targets -- -D warnings` — PASS.
