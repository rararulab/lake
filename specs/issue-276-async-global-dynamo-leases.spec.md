spec: task
name: "async-global-dynamo-leases"
inherits: project
tags: [query, async, cluster, dynamodb, localstack, lease]
---

## Intent

Global async execution capacity is coordinated through the production
`DynamoMeta` backend, but its lease contract is currently proved only by
RocksMeta unit tests. Reproducer: run two Query replicas against one DynamoDB
table with a global limit of one; after the first replica reserves capacity,
the second must remain queued. Once the first owner expires, the second may
take capacity, while the old token must not renew or release the successor. A
backend-specific conditional-write or empty-value mismatch would otherwise let
production replicas exceed the cluster limit, deadlock it, or let a stale
worker remove its successor while development tests remain green.

This advances the North Star's stateless Query tier: replicas coordinate only
through compact CAS state in the durable metadata backend. It does not make the
metadata tier a data-plane hop or store object bytes, credentials, or request
payloads in it.

## Decisions

- Exercise two independent `DynamoMeta` handles against one unique LocalStack
  table, so the test observes the real DynamoDB CAS behavior used by separate
  Query replicas rather than sharing a RocksDB process handle.
- Cover global saturation, durable pending state, expiry reclamation, and
  exact-token fencing in one deterministic ignored integration test. Logical
  timestamps avoid sleeps and wall-clock races.
- Add `lake-query` to the existing ignored-only LocalStack integration runner;
  local `mise run test-integration` and the external CI mode then execute the
  same production-backend contract.

## Boundaries

### Allowed Changes
crates/lake-query/src/async_query.rs
scripts/test-integration.ts
specs/issue-276-async-global-dynamo-leases.spec.md
verification/issue-276-async-global-dynamo-leases.md

### Forbidden
crates/lake-meta/**
async worker scheduling or execution semantics
async job ticket DataLocation result manifest or object-part wire formats
object bytes credentials signed URLs or arbitrary request payloads in Meta
production DynamoDB schema API configuration or IAM policy
unbounded lease records CAS retries scans queues maps or background work
raw tenant principal query worker or opaque-token identity in visible output logs metrics errors or public Flight payloads

## Acceptance Criteria

Scenario: DynamoDB global execution leases retain the cluster contract across replicas
  Test:
    Package: lake-query
    Filter: dynamo_execution_leases_localstack_is_wired
  External verification: `mise run test-integration` runs
  `dynamo_execution_leases_preserve_cluster_capacity_and_fencing` against
  LocalStack because the spec runner does not execute ignored infrastructure
  tests.
  Given two Query async stores backed by separate DynamoMeta handles for one LocalStack table
  When one replica saturates capacity, expires, and a second replica takes the successor lease
  Then the queued job remains pending while saturated, expiry reclaims capacity, and the stale token cannot renew or release the successor

## Out of Scope

- New global scheduling behavior, leader election, strict global fairness, or
  foreground metadata lookups.
- Changes to DynamoMeta implementation, query public APIs, production tables,
  deployments, or IAM.
