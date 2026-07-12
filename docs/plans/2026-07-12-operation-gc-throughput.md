# Append-operation GC throughput implementation plan

## Goal

Raise the default append-operation cleanup ceiling above sustained production
write rates without making one maintenance tick unbounded or weakening exact
stage cleanup and replay safety.

## Architecture

`MaintenanceLimits` owns a finite operation-page budget because this is a
per-tick fairness bound, while the existing operation policy continues to own
retention and page size. The leader follows the opaque process-local cursor
for at most that many physical pages, stops at end-of-scan without wrapping in
the same tick, and reports page/item/budget progress using bounded labels.

## Tasks

1. Add RED tests for multi-page drain, budget/resume, shutdown at a page
   boundary, configuration validation, and metric output.
2. Extend immutable maintenance configuration and CLI environment parsing with
   a bounded maximum operation-page count per tick.
3. Refactor operation GC so one stage drains consecutive pages under that
   budget while preserving serial reconciliation and cancellation boundaries.
4. Count physical pages and budget exhaustion in existing low-cardinality
   metrics, then document capacity sizing and rollout.
5. Run focused tests, strict clippy, lane-1 lifecycle, full gate, rustdoc, and
   independent correctness/performance/release review before push.
