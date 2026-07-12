# Verification: append-operation GC throughput

## Required evidence

- A tick drains several physical operation pages without exceeding its budget.
- Budget exhaustion preserves and resumes the opaque cursor on the next tick.
- Shutdown prevents any scan or reconciliation after the current page boundary.
- Existing exact-stage cleanup, failure retention, fencing, and expired replay
  behavior remain green.
- Configuration and metrics are finite, startup-validated, and identity-free.

## GREEN evidence

- RED compile failed because the multi-page sweep API and stats did not exist.
- Six focused operation-GC tests passed, including the three new lifecycle
  cases and all prior cleanup/recovery cases.
- CLI startup-limit validation passed for default, valid, zero, malformed, and
  oversized operation page budgets.
- Lane-1 lifecycle passed all five scenarios with non-zero selector matches.
- Strict Clippy passed for `lake-metasrv` and `lake-cli`, all targets, warnings
  denied.
- Full gate passed all workspace tests, e2e, hooks, and site checks.
- Workspace rustdoc passed with warnings denied.
- Initial performance review found that a page-only budget could still delay
  table maintenance for minutes. The corrected stage has a startup-validated
  10-second default wall-clock deadline, cancels blocked scan/reconciliation
  futures safely against durable state, and emits `time_exhausted`.
- Initial correctness review required production-backend cursor evidence. The
  LocalStack lifecycle now deletes every limit-one page before resuming its
  opaque cursor in both v1 Scan and v2 Query modes; the integration passed.
- Cursor publication now occurs only after a whole page is processed, so
  deadline or shutdown cancellation retries a partial page instead of skipping
  its unprocessed entries.
- Corrected lane-1 lifecycle passed 7/7, strict Clippy passed across the three
  affected crates, and the corrected full gate and rustdoc passed.
- Correctness re-review found one remaining shutdown branch that broke out of
  the entry loop and then published a partial-page cursor. It now returns
  immediately without publishing; a regression test resumes with a fresh token
  and proves all three entries remain reachable.
- Performance re-review approved the page plus wall-clock fairness bounds, and
  release/operations re-review passed the corrected configuration, metrics,
  docs, and LocalStack evidence.
- Correctness re-review approved after the partial-page shutdown regression
  was fixed; no P0/P1 findings remain.
- The final exact code head passed the full gate again after that fix.
