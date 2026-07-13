spec: task
name: "async-tenant-resource-quotas"
inherits: project
tags: [query, async, tenant, quota, resource, durability, object-storage]
---

## Intent

Bound durable asynchronous-query resource consumption per tenant. Worker
concurrency is finite, but one job can currently publish up to 4,096 64-MiB
parts and a tenant can retain unbounded queued or terminal jobs until expiry.
Add a durable point-addressed tenant index and an immutable per-job byte limit
without placing metadata I/O on foreground SQL execution or exposing identity.

## Decisions

- Add a versioned tenant resource index under a SHA-256 domain-separated tenant
  digest. Raw tenant IDs and their digest never appear in logs, metrics, errors,
  Flight payloads, or object paths beyond the existing authenticated scope.
- Reserve one outstanding-job entry before job-spec object upload. CAS retries
  are bounded. The index holds at most 128 entries and is never constructed by
  scanning async query records.
- A new reservation initially has a five-minute grace. When pruning an expired
  reservation, point-read its query record: retain live records through their
  durable expiry and remove only missing/expired owners. Crashes may over-count
  temporarily but never under-count a live record.
- Count queued, running, completed, failed, cancelled, expired, and cleaning
  records until scoped objects and the state record are deleted. Cleanup then
  releases only the exact `(query ID, opaque reservation token)` pair from the
  tenant index.
- New schema-v2 async records persist a result byte limit and tenant reservation
  token. Schema-v1 records remain decodable, runnable, pollable, and cleanable
  with the legacy hard protocol ceiling and no fabricated reservation token.
- If a schema-v1 replica wins a deterministic state create after a schema-v2
  reservation, the v2 coordinator releases only its own exact token before
  resuming the v1 record. A transient release failure self-heals after the
  grace period by discarding the unmatched v1 reservation; it never discards a
  token owned by a schema-v2 record.
- Default limits are eight outstanding jobs per tenant and 16 GiB result bytes
  per job. Allowed ranges are 1..=128 and 64 MiB..=256 GiB.
- A worker derives its encoded-part budget from the record, not current process
  configuration. It rejects before starting a part whose bounded encoder cannot
  fit and never publishes a manifest above the record limit.
- Quota exhaustion maps to identity-free Flight ResourceExhausted and the fixed
  `lake_query_async_quota_rejections_total{reason="outstanding_jobs"}` metric.
  No tenant IDs, hashes, query IDs, or configured sizes become metric labels.

## Boundaries

### Allowed Changes
Cargo.lock
README.md
crates/lake-cli/**
crates/lake-query/**
deploy/kubernetes/lake.yaml
docs/architecture.md
docs/guides/kubernetes.md
docs/plans/2026-07-13-async-tenant-resource-quotas.md
specs/issue-126-async-tenant-resource-quotas.spec.md
verification/issue-126-async-tenant-resource-quotas.md

### Forbidden
unbounded tenant index records CAS retries scans or cleanup work
raw tenant principal query or digest values in keys visible output logs or metrics
process-local counters as durable quota authority
under-counting a live durable async record after crash retry or config change
loosening a persisted job limit after worker restart
deleting another submission tenant reservation or object scope
per-row per-part or foreground synchronous catalog metadata RPC traffic
changing async ticket job object part manifest or DataLocation wire formats
routing result object bytes through Metasrv or Query Flight
claiming cluster-global CPU memory or execution fairness in this issue

## Completion Criteria

Scenario: Durable tenant quota isolates concurrent coordinators
  Test:
    Package: lake-query
    Filter: async_tenant_quota_is_durable_and_isolated
  Given two coordinators sharing state and a finite outstanding limit
  When they concurrently reserve more jobs for one tenant while another submits
  Then the first tenant never owns more than its limit and the other tenant remains independently admissible

Scenario: Stale reservations recover without under-counting live records
  Test:
    Package: lake-query
    Filter: async_tenant_quota_reclaims_stale_reservations
  Given expired pre-record reservations plus an expired reservation whose live record remains
  When a new submission prunes the bounded tenant index
  Then missing owners are reclaimed while the live record is retained through its durable expiry

Scenario: Cleanup releases only the exact tenant reservation
  Test:
    Package: lake-query
    Filter: async_cleanup_releases_exact_tenant_reservation
  Given two retained jobs for one tenant and one reaches fenced cleanup
  When scoped objects and its state record are deleted
  Then only that query reservation is released and retry or crash cannot release its neighbor

Scenario: Persisted result bytes fence worker restart
  Test:
    Package: lake-query
    Filter: async_result_limit_is_immutable_across_worker_restart
  Given a schema-v2 job created under a small result limit and a restarted worker with looser configuration
  When encoded output reaches the persisted ceiling
  Then the worker fails boundedly without publishing an oversized part or completion manifest

Scenario: Legacy async records remain compatible
  Test:
    Package: lake-query
    Filter: async_resource_v1_records_remain_compatible
  Given a valid schema-v1 queued completed or cleaning record
  When a current coordinator loads runs polls or cleans it
  Then legacy behavior remains valid without inventing a tenant reservation or weakening its old hard result bound

Scenario: Legacy create race cannot poison v2 tenant capacity
  Test:
    Package: lake-query
    Filter: v1_create_race_releases_new_reservation_before_idempotent_resume
  Given a schema-v2 coordinator reserves a deterministic submission and a
  schema-v1 replica wins the state-record create race
  When the v2 coordinator resumes the valid v1 submission
  Then its exact token is released and a later tenant reservation remains
  admissible

Scenario: CLI rejects unsafe async resource limits before serving
  Test:
    Package: lake-cli
    Filter: async_resource_limit_values_are_validated_before_serving
  Given zero excessive malformed boundary and default environment values
  When Query async configuration is constructed
  Then only 1..=128 outstanding and 64 MiB..=256 GiB result limits are accepted before listener bind

Scenario: Kubernetes reference declares finite async resources
  Test:
    Package: lake-cli
    Filter: kubernetes_reference_is_secure_and_matches_runtime_contract
  Given the production reference deployment
  When its Query resource contract is validated
  Then explicit outstanding-per-tenant and per-job result-byte limits are present and documented as durable storage bounds

## Out of Scope

- Cluster-global CPU, DataFusion memory, or worker concurrency quotas.
- Tenant billing, usage export, chargeback, or user-visible quota introspection.
- Immediate cleanup of cancelled/completed results before their configured
  lifetime.
- Changing the 64-MiB per-part streaming bound.
