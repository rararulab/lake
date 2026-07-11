spec: task
name: "gc-registry-scan"
inherits: project
tags: [cli, gc, meta, registry, performance]
---

## Intent

Managed-object GC must validate the complete registry root without issuing
metadata requests proportional to both namespaces and tables. Today every
planning snapshot and every apply-page validation lists all namespaces, lists
each namespace, and point-reads every table. At roughly ten thousand tables,
the fail-closed safety check becomes an N+1 request amplifier against the
bounded metadata authority.

This advances `goal.md`'s bounded metadata-authority design while preserving
the existing complete-root safety model for destructive object collection.

## Decisions

- Build the GC root snapshot with `lake_meta::registry::scan_tables`, which
  returns decoded registrations from one backend prefix scan.
- Preserve `BTreeMap<TableRef, TableRegistration>` as the canonical snapshot
  representation so root equality and SHA-256 fingerprint bytes remain
  deterministic and unchanged for the same registrations.
- Keep planning's post-mark equality check and apply's pre-page fingerprint
  checks exactly fail-closed; only the way each complete snapshot is read
  changes.
- Add an instrumented metastore regression test with registrations in multiple
  namespaces. It must observe exactly one table scan and no namespace-list,
  table-list, or point-get calls.

## Boundaries

### Allowed Changes
crates/lake-cli/**
docs/architecture.md
docs/design/managed-objects.md
docs/plans/2026-07-12-gc-registry-scan.md
specs/issue-61-gc-registry-scan.spec.md
verification/issue-61-gc-registry-scan.md

### Forbidden
crates/lake-meta/**
crates/lake-objects/**
crates/lake-engine*/**
durable GC plan or checkpoint formats
registry key layout or DynamoDB schema
weakening registry equality or fingerprint validation
parallel or incremental GC apply

## Completion Criteria

Scenario: GC snapshots the complete registry with one backend scan
  Test:
    Package: lake-cli
    Filter: gc_registry_snapshot_uses_single_scan
  Given registrations in multiple namespaces behind an instrumented metastore
  When the GC registry-root snapshot is built
  Then every registration is returned in deterministic table order using one scan and zero list or point-get calls

Scenario: Local planning and apply remain fail-closed and serverless
  Test:
    Package: lake-cli
    Filter: local_gc_dry_run_then_apply_uses_no_server
  Given an old unreferenced managed object and a local registry
  When a dry-run plan is created and explicitly applied
  Then dry-run does not mutate storage and apply consumes the fingerprint-bound plan

## Out of Scope

- Making the complete GC root snapshot itself paged or incremental.
- Reducing how often apply revalidates the registry fingerprint.
- Changing retained-reference enumeration or object inventory paging.
- Adding online/background managed-object GC to Metasrv.
