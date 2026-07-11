spec: task
name: "paged-table-maintenance"
inherits: project
tags: [meta, metasrv, maintenance, pagination, dynamodb, performance]
---

## Intent

One leader maintenance tick must have a finite registry-I/O and table-work
ceiling. The current sweep lists all namespaces, lists tables separately for
each namespace, point-reads every registration, and attempts every table. At
the target of roughly ten thousand tables this creates repeated DynamoDB scans
and an unbounded burst every minute.

## Decisions

- Add a typed table-registration page API over `MetaStore::scan_prefix_page`.
  Its continuation remains backend-opaque and page results never exceed the
  requested positive limit.
- Each maintenance tick consumes at most one registry page. A process-local
  cursor resumes the next tick and wraps to the beginning after the final page;
  a new leader safely starts from the beginning.
- Keep the scanned registration only as a work candidate. After acquiring the
  table lock, point-read the current registration and operate on that current
  generation, never a stale location/version from before the lock.
- Add validated `MaintenanceLimits` with default interval 60 seconds and table
  page size 128. Expose positive deployment overrides
  `LAKE_MAINTENANCE_INTERVAL_SECS` and
  `LAKE_MAINTENANCE_TABLE_PAGE_SIZE`; maximum page size is 10000.
- Preserve drop-tombstone and append-operation GC paging, cancellation
  boundaries, per-table error isolation, and registry CAS publication.
- Trace page-level scanned, maintained, skipped, and failed counts.

## Boundaries

### Allowed Changes
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-cli/**
docs/architecture.md
docs/guides/cli.md
docs/plans/2026-07-12-paged-table-maintenance.md
specs/issue-59-paged-maintenance.spec.md
verification/issue-59-paged-maintenance.md

### Forbidden
crates/lake-query/**
crates/lake-engine*/**
crates/lake-sdk/**
durable metadata formats
DynamoDB table schema or indexes
Flight wire protocols
parallel table maintenance

## Completion Criteria

Scenario: Registry table pages are bounded and resumable
  Test:
    Package: lake-meta
    Filter: scan_table_pages_are_bounded_and_resumable
  Given five table registrations and a page limit of two
  When pages are read by passing each opaque continuation to the next request
  Then every page has at most two entries and all five tables appear exactly once

Scenario: Maintenance advances one bounded table page per tick
  Test:
    Package: lake-metasrv
    Filter: table_maintenance_pages_resume_without_full_registry_sweep
  Given three registered tables and a maintenance page size of two
  When two table-maintenance ticks run
  Then the first attempts two tables, the second attempts one, and no namespace listing is used

Scenario: Maintenance re-resolves a scanned table after locking
  Test:
    Package: lake-metasrv
    Filter: table_maintenance_reresolves_after_scanned_generation_changes
  Given a scanned old registration whose table lock is held while the registry is replaced
  When the lock is released and maintenance continues
  Then the engine sees only the replacement location and never the stale scanned location

Scenario: Invalid maintenance limits fail before serving
  Test:
    Package: lake-cli
    Filter: maintenance_limit_values_are_validated_before_serving
  Given zero, malformed, or oversized interval/page values
  When Metasrv startup configuration is built
  Then configuration fails before binding and valid values preserve their exact settings

## Out of Scope

- Optimizing the offline managed-object GC registry snapshot.
- Adding a DynamoDB GSI or changing durable key layout.
- Durable maintenance cursors, table priority, or parallel compaction.
- Changing engine compaction or retained-version policy.
