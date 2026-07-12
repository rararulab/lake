# Append-operation GC throughput implementation plan

## Goal

Raise the default append-operation cleanup ceiling above sustained production
write rates without making one maintenance tick unbounded or weakening exact
stage cleanup and replay safety.

## Architecture

`MaintenanceLimits` owns finite operation page and wall-clock budgets because
these are per-tick fairness bounds, while the existing operation policy
continues to own retention and page size. The leader follows the opaque
process-local cursor within both ceilings, advances it only after a whole page
is processed, stops at end-of-scan without wrapping in the same tick, and
reports page/item/budget progress using bounded labels.

## Tasks

1. Add RED tests for multi-page drain, budget/resume, shutdown at a page
   boundary, wall-clock cancellation, Dynamo delete-while-paging,
   configuration validation, and metric output.
2. Extend immutable maintenance configuration and CLI environment parsing with
   a bounded maximum operation-page count per tick.
3. Refactor operation GC so one stage drains consecutive pages under that
   budget while preserving serial reconciliation and cancellation boundaries.
4. Count physical pages and budget exhaustion in existing low-cardinality
   metrics, then document capacity sizing and rollout.
5. Run focused tests, strict clippy, lane-1 lifecycle, full gate, rustdoc, and
   independent correctness/performance/release review before push.
