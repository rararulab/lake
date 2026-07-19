spec: task
name: "async-global-execution"
inherits: project
tags: [query, async, cluster, quota, lease, kubernetes]
---

## Intent

Query replicas currently enforce async worker capacity only in process. With
multiple replicas, a deployment configured for four workers can execute four
jobs on every replica, violating the operator's intended cluster ceiling.
Reproducer: run two Query replicas against the same dedicated async state
store, configure each for four local workers, and submit more than four
long-running jobs. Both schedulers claim jobs and execute up to eight scans;
unrelated tenants can therefore receive more cluster capacity than the
deployment policy declares.

This advances the North Star's stateless, horizontally scalable Query layer:
replicas coordinate only through compact CAS state in the injected async store,
while data still flows directly between Query and object storage. It does not
make Meta a data-plane hop or introduce a scheduling control plane.

## Decisions

- An opt-in, compact shared execution-lease index provides finite global and
  per-tenant running capacity across Query replicas. It is an opaque-token,
  CAS-managed coordination record in the dedicated async state store, never an
  in-memory authority or a scan of all jobs.
- A worker reserves cluster capacity before it claims a job lease. Saturation is
  retryable: the job remains pending and is not terminally failed. If claim
  cannot proceed, the worker releases only its exact execution-lease token.
- The worker renews the execution lease with its job lease and releases the
  exact token on every completion path. Expired leases are reclaimed by bounded
  CAS so a crashed owner may over-count briefly but never retains authority
  after its lease expires; stale tokens cannot renew or release a replacement.
- The existing per-replica scheduler remains responsible for local
  work-conservation and page selection. This task adds capacity enforcement,
  not a global queue, leader, strict dispatch order, or a foreground metadata
  lookup.
- Global mode is enabled only when both immutable environment values are set:
  `LAKE_ASYNC_GLOBAL_WORKER_CONCURRENCY` and
  `LAKE_ASYNC_GLOBAL_WORKER_CONCURRENCY_PER_TENANT`. They use the same finite
  worker range as local scheduling and the tenant value cannot exceed the
  total. A partial pair fails before Query binds.
- The Kubernetes reference opts in to the shared limits. Operators drain and
  recreate all Query replicas for enable, disable, or value changes; mixed
  versions are not a valid rollout because old workers do not own shared
  leases.
- Telemetry uses only a finite outcome vocabulary. Tenant IDs, query IDs,
  worker IDs, opaque lease tokens, and configured capacities never become
  labels, logs, errors, or public payloads.

## Boundaries

### Allowed Changes
README.md
crates/lake-cli/**
crates/lake-query/**
deploy/kubernetes/lake.yaml
docs/architecture.md
docs/design/sql-api-over-s3.md
docs/guides/cli.md
docs/guides/kubernetes.md
specs/issue-273-async-global-execution.spec.md
verification/issue-273-async-global-execution.md

### Forbidden
crates/lake-meta/**
async job ticket DataLocation result manifest or object-part wire formats
object bytes credentials signed URLs or arbitrary request payloads in Meta
unbounded lease records CAS retries scans queues maps or background work
global scheduler queues leader election foreground metadata lookups or
process-local state as cluster quota authority
raw tenant principal query worker or opaque-token identity in keys visible
output logs metrics errors or public Flight payloads
changing issue-126 retained-job/result-byte quota semantics
cluster-global CPU memory billing egress scanned-byte or result-byte accounting

## Completion Criteria

Scenario: Shared execution leases cap concurrent replicas without cross-tenant starvation
  Test:
    Package: lake-query
    Filter: cluster_execution_leases_are_bounded_and_durable
  Given independent async stores share one durable state backend with finite global and tenant limits
  When concurrent workers reserve executions for a saturated tenant and another tenant
  Then no tenant or cluster exceeds the configured running capacity and the other tenant remains admissible

Scenario: Expiry and opaque tokens fence stale execution owners
  Test:
    Package: lake-query
    Filter: cluster_execution_leases_reclaim_expiry_and_fence_tokens
  Given an execution owner crashes and a successor reclaims its expired capacity
  When the stale token attempts renewal or release after the successor reservation
  Then only the successor remains in the bounded index and the stale token cannot mutate it

Scenario: Cluster capacity saturation keeps durable jobs pending
  Test:
    Package: lake-query
    Filter: cluster_execution_capacity_saturation_keeps_job_pending
  Given a cluster execution lease already consumes the finite capacity
  When another worker considers a pending durable job
  Then it returns retryable saturation without claiming or terminally failing the job

Scenario: Global execution environment is complete and safe before serving
  Test:
    Package: lake-cli
    Filter: async_global_execution_limit_values_are_validated_before_serving
  Given absent partial zero excessive contradictory malformed and valid global worker settings
  When lake constructs immutable Query async configuration
  Then only an absent pair or a complete finite compatible pair reaches listener setup

Scenario: Cluster execution telemetry remains finite and identity-free
  Test:
    Package: lake-query
    Filter: async_global_execution_metrics_are_bounded_and_identity_free
  Given admitted saturated expired and stale-token cluster lease outcomes
  When Query exports execution-capacity metrics
  Then labels are drawn from a fixed outcome vocabulary and contain no tenant query worker or token identity

Scenario: Kubernetes declares the coordinated async contract
  Test:
    Package: lake-cli
    Filter: kubernetes_reference_declares_cluster_async_execution_limits
  Given the production reference deployment
  When its Query environment is validated
  Then it provides both finite global execution limits and documents the drain-and-recreate rollout constraint

## Out of Scope

- Global queueing, leader election, strict cluster-wide scheduling fairness, or
  foreground-query coordination.
- Cluster-global CPU, DataFusion memory, spill, scanned-byte, result-byte,
  egress, billing, or user-visible quota accounting.
- Changing durable async tickets, result manifests, object layout, or the
  issue-126 resource-reservation contract.
