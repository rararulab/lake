spec: task
name: "guarded-operation-gc-test"
inherits: project
tags: [test, metastore, maintenance, reliability]
---

## Intent

Keep the production metadata-fencing regression test deterministic. It must
prove that explicit operation GC deletes through the lease-guarded metastore,
without depending on whether a preceding real-time maintenance pass crosses a
wall-clock second.

## Decisions

- The test's ordinary maintenance pass uses a positive replay-retention horizon
  so it exercises table maintenance but cannot consume newly appended operation
  records on a second boundary.
- The existing explicit synthetic-time operation-GC sweep remains responsible
  for the guarded-delete assertion.
- This is test-only: operation-GC production behavior, defaults, and timing
  remain unchanged.

## Boundaries

### Allowed Changes
crates/lake-metasrv/src/lib.rs
specs/issue-185-guarded-operation-gc-test.spec.md

### Forbidden
production operation-GC semantics
retention defaults
storage or metadata protocol changes
timing sleeps or retry loops in tests

## Completion Criteria

Scenario: Guarded operation-GC coverage is independent of real-time sweep timing
  Test:
    Package: lake-metasrv
    Filter: production_metadata_mutations_use_guarded_store
  Given a fenced authority that appends operation records
  When its full maintenance sweep runs before an explicit synthetic-time GC
  sweep
  Then the explicit sweep performs the guarded deletion assertion regardless
  of wall-clock second boundaries

## Out of Scope

- Changing operation replay retention or GC scheduling in production.
- Adding retries to hide timing-sensitive tests.
