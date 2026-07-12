spec: task
name: "operation-gc-throughput"
inherits: project
tags: [metasrv, maintenance, gc, performance, observability]
---

## Intent

The leader currently scans exactly one append-operation page per maintenance
tick. At the production defaults this caps cleanup at 128 records per minute,
so a sustained append rate above roughly 2.13 operations per second makes
durable replay state grow without bound. One tick must drain multiple pages
while retaining an explicit work ceiling and yielding to drop/table
maintenance and shutdown.

This advances `goal.md`'s bounded stateful metadata authority at the target
write scale. It does not put reader traffic on metadata, add cross-table
transactions, or introduce a storage-node tier.

## Decisions

- Add a validated maximum append-operation page count to the immutable
  maintenance limits. The default is finite and materially raises throughput;
  zero and oversized values fail before serving.
- Within a tick, scan consecutive operation pages until the cursor reaches the
  end, the page budget is exhausted, an error occurs, or shutdown is
  requested. Do not wrap and rescan from the beginning in the same tick.
- Preserve the existing per-record reconciliation, per-table lock, durable
  fencing, exact-stage cleanup, and record-delete ordering.
- Report physical pages, scanned/deleted items, and budget exhaustion through
  the existing finite-label maintenance metrics. Never label by tenant,
  table, operation, URI, key, or cursor.
- Keep the cursor process-local. A new leader safely starts from the beginning;
  no durable index or key-layout migration is introduced in this issue.

## Boundaries

### Allowed Changes
crates/lake-metasrv/**
crates/lake-cli/**
docs/architecture.md
docs/guides/cli.md
docs/guides/kubernetes.md
docs/plans/2026-07-12-operation-gc-throughput.md
specs/issue-98-operation-gc-throughput.spec.md
verification/issue-98-operation-gc-throughput.md

### Forbidden
crates/lake-meta/**
crates/lake-engine*/**
crates/lake-query/**
crates/lake-sdk/**
durable metadata formats
DynamoDB table schema or indexes
parallel per-record reconciliation
reader-to-metadata traffic

## Completion Criteria

Scenario: One maintenance tick drains multiple operation pages
  Test:
    Package: lake-metasrv
    Filter: operation_gc_drains_multiple_pages_within_budget
  Given expired append-operation records spanning several one-record pages
  When one operation-GC stage runs with a page budget covering those pages
  Then every page is scanned once and every safely reconciled record is deleted

Scenario: Page budget stops work and the cursor resumes next tick
  Test:
    Package: lake-metasrv
    Filter: operation_gc_stops_at_page_budget_and_resumes
  Given more expired operation pages than one tick may consume
  When the first tick exhausts its page budget and a second tick resumes
  Then the first tick stays within budget and the second continues from its cursor

Scenario: Operation GC yields promptly to shutdown
  Test:
    Package: lake-metasrv
    Filter: operation_gc_shutdown_stops_between_pages
  Given a multi-page operation sweep whose cancellation fires after page one
  When the GC stage reaches the next page boundary
  Then it performs no further page scan or record reconciliation

Scenario: Invalid operation page budgets fail before serving
  Test:
    Package: lake-cli
    Filter: maintenance_limit_values_are_validated_before_serving
  Given missing, valid, zero, malformed, or oversized page-budget configuration
  When Metasrv startup limits are parsed
  Then defaults and valid values are exact and invalid values fail before binding

Scenario: Operation GC metrics remain bounded and identity-free
  Test:
    Package: lake-metasrv
    Filter: metasrv_metrics_cover_append_leadership_and_maintenance
  Given a maintenance sweep with append-operation work
  When Prometheus metrics are rendered
  Then physical page and budget signals use finite stage/outcome labels only

## Out of Scope

- Adding a durable expiration-time index or changing operation keys.
- Running record reconciliation concurrently.
- Changing replay retention or append idempotency semantics.
- Solving the independent Query catalog full-refresh scaling problem.
