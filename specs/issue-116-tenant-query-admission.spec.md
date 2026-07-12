spec: task
name: "tenant-query-admission"
inherits: project
tags: [query, tenant, admission, fairness, concurrency, flight]
---

## Intent

Add bounded per-tenant admission ahead of the existing per-replica Query
ceiling so one authenticated tenant cannot starve unrelated tenants.

## Decisions

- Tenant permits are acquired before global permits under one deadline.
- Permit ownership is RAII and follows the existing Flight stream lifecycle.
- Tenant gates are held by weak references in a finite registry and pruned
  synchronously; no persistent state or cleanup task is introduced.
- This is explicitly per-replica isolation, not a distributed quota.

## Boundaries

### Allowed Changes
crates/lake-query/**
crates/lake-cli/**
deploy/kubernetes/lake.yaml
README.md
docs/architecture.md
docs/design/sql-api-over-s3.md
docs/guides/cli.md
docs/guides/kubernetes.md
docs/plans/2026-07-13-tenant-query-admission.md
specs/issue-116-tenant-query-admission.spec.md
verification/issue-116-tenant-query-admission.md

### Forbidden
tenant identity in metric labels logs or public errors
unbounded tenant maps queues waits or background cleanup
holding a global permit while waiting for a tenant permit
metadata catalog or object-store I/O during admission
claiming cluster-global quotas or durable async scheduler fairness
changing authentication or authorization semantics

## Completion Criteria

Scenario: Saturated tenant cannot starve another tenant
  Test:
    Package: lake-query
    Filter: tenant_query_admission_isolates_noisy_neighbor
  Given tenant A holds its local concurrency share
  When another A request queues and tenant B requests a free global slot
  Then B is admitted while the queued A request times out without holding global capacity

Scenario: Aggregate replica ceiling still applies across tenants
  Test:
    Package: lake-query
    Filter: tenant_query_admission_preserves_global_limit
  Given different tenants each hold a tenant permit
  When their aggregate reaches the global Query limit
  Then another otherwise eligible tenant receives ResourceExhausted

Scenario: Tenant tracker lifecycle is bounded and reclaimable
  Test:
    Package: lake-query
    Filter: tenant_query_admission_reclaims_inactive_trackers
  Given the bounded tracker is full of active tenant gates
  When a new tenant arrives before and after an old permit is dropped
  Then it is first rejected and later admitted after inactive gates are pruned

Scenario: Flight streams own both admission levels
  Test:
    Package: lake-query
    Filter: flight_discovery_error_releases_tenant_admission_permit
  Given a tenant discovery stream owns global and tenant capacity
  When the stream terminates with an error
  Then a new stream for the same tenant is admitted immediately

Scenario: Tenant limit configuration fails before serving
  Test:
    Package: lake-cli
    Filter: query_tenant_limit_values_are_validated_before_serving
  Given zero excessive contradictory or malformed tenant limits
  When lake query parses its immutable environment policy
  Then startup fails before binding the Query listener or starting background work

Scenario: Admission telemetry remains identity-free
  Test:
    Package: lake-query
    Filter: query_metrics_cover_admission_and_catalog_refresh
  Given admitted globally saturated tenant-saturated and tracker-saturated requests
  When Query exports admission counters
  Then only finite outcome labels exist and no tenant identity is exported

## Out of Scope

- Cluster-global admission across Query replicas.
- Durable async worker scheduling fairness.
- Per-tenant scanned bytes, result bytes, memory, spill, or egress accounting.
