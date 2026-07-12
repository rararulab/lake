spec: task
name: "async-tenant-fairness"
inherits: project
tags: [query, async, tenant, fairness, admission, deadline]
---

## Intent

Close the resource-isolation gap between foreground Flight requests and
durable `PollFlightInfo` execution. Today a tenant can submit enough long
running asynchronous scans to occupy every background worker indefinitely:
the scan loop waits on jobs in key order, has only a replica-wide concurrency
limit, and renews leases without an execution deadline. Unrelated tenants can
therefore starve even though foreground Query admission is tenant-aware.

## Decisions

- Add a bounded process-local asynchronous scheduler with an explicit
  per-tenant running ceiling below the replica worker ceiling.
- A tenant at its running ceiling never consumes another scheduler slot while
  waiting; eligible tenants in the scanned page can run immediately.
- Decode bounded candidate records from the existing state scan instead of
  adding one point read per candidate before scheduling.
- Every worker run owns one absolute execution deadline. Timeout cancels query
  execution and lease renewal, then publishes a stable identity-free terminal
  failure through the existing CAS-fenced state machine.
- Limits are immutable startup configuration and are described as per-replica,
  not cluster-global quotas.

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
docs/plans/2026-07-13-async-tenant-fairness.md
specs/issue-118-async-tenant-fairness.spec.md
verification/issue-118-async-tenant-fairness.md

### Forbidden
tenant or principal identity in metric labels logs or public failure codes
unbounded queues maps worker tasks scan pages or waits
holding a worker slot while waiting for a saturated tenant
changing async state keys record schema ticket format or object layout
claiming cluster-global fairness or quotas across Query replicas
per-tenant scan result memory spill egress or byte accounting
rewriting async result encoding or download buffering

## Completion Criteria

Scenario: Saturated async tenant does not consume another worker slot
  Test:
    Package: lake-query
    Filter: async_scheduler_skips_saturated_tenant_for_eligible_neighbor
  Given tenant A already owns its configured async running share
  When a scan page contains more A jobs and one eligible tenant B job
  Then B is scheduled without an A waiter occupying the remaining worker slot

Scenario: Scheduler candidate scans avoid per-job metadata reads
  Test:
    Package: lake-query
    Filter: async_scheduler_uses_bounded_scan_records_without_point_reads
  Given one bounded async state page containing pending and terminal records
  When the scheduler selects runnable candidates
  Then it decodes the page values directly and performs no candidate point reads

Scenario: Async execution deadline is terminal and releases capacity
  Test:
    Package: lake-query
    Filter: async_worker_deadline_fails_job_and_releases_tenant_capacity
  Given a worker owns a lease for a query stream that does not finish
  When its absolute execution deadline expires
  Then execution and renewal stop the job becomes a stable timeout failure and another tenant can run

Scenario: Stale workers cannot overwrite a timeout terminal state
  Test:
    Package: lake-query
    Filter: async_timeout_state_fences_stale_worker_completion
  Given a timed out worker has published its terminal failure with its lease
  When stale completion or renewal arrives for the same job
  Then the CAS-fenced state machine rejects it and preserves the timeout failure

Scenario: Async tenant and deadline limits fail before serving
  Test:
    Package: lake-cli
    Filter: async_scheduler_limit_values_are_validated_before_serving
  Given zero excessive contradictory or malformed async scheduling limits
  When lake query parses its immutable environment policy
  Then startup fails before binding Query or starting background work

Scenario: Async scheduling telemetry remains identity-free
  Test:
    Package: lake-query
    Filter: async_scheduler_metrics_are_bounded_and_identity_free
  Given admitted tenant-saturated and deadline-expired async jobs
  When Query exports scheduler counters and gauges
  Then labels come from a finite outcome vocabulary and contain no tenant or query identity

## Out of Scope

- Cluster-global tenant quotas or strict fairness across Query replicas.
- A durable per-tenant queue index or admission limit on queued job count.
- Per-tenant scanned bytes, result bytes, DataFusion memory, spill, or egress.
- Streaming async result encoding/download; tracked separately as the next P0.
