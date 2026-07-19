spec: task
name: "iceberg-snapshot-admission"
inherits: project
tags: [iceberg, rest, cache, admission, concurrency]
---

## Intent

Keep Lake's stateless Query tier from turning a request flood of distinct
Iceberg table names into unbounded process-local state and external REST work.
The existing snapshot cache has a 10,000-entry bound and coalesces one table
key, but its map of distinct pending single-flight loads has no independent
limit.

Reproducer: configure the `analytics` namespace, make external table loads
block, then concurrently resolve more distinct syntactically valid table names
than the Query replica's normal admission budget. Before this change every name
allocates a pending watch channel and begins an external table load; a client
can therefore retain arbitrary pending state until the REST deadline expires.

This advances `goal.md`'s requirement that the stateless Query layer absorbs
fleet-read fan-out while shielding a bounded metadata authority. It does not
cross the prohibition on direct reader load against a metadata authority:
Iceberg remains an external metadata authority and Lake remains read-only.

## Decisions

- A Query replica admits at most 64 distinct pending Iceberg snapshot loads.
  The bound is independent of the 10,000 completed-snapshot cache and matches
  the default Query concurrency budget.
- A follower for a key already pending remains admissible when the distinct-key
  limit is full; it waits for that key's one leader and does not consume a new
  slot.
- A new distinct key at capacity fails closed before an external table load or
  cache entry. Its error and telemetry remain identity-free.
- Completing or cancelling a load releases exactly its slot. No queue,
  background refresh, retry policy, or deployment knob is introduced.

## Boundaries

### Allowed Changes
crates/lake-iceberg/src/lib.rs
crates/lake-iceberg/tests/catalog.rs
docs/design/iceberg-federation.md
specs/issue-259-iceberg-snapshot-admission.spec.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-catalog/**
crates/lake-query/**
crates/lake-flight/**
Lake registry or ticket-schema changes
Iceberg write, DDL, DML, commit, catalog mutation, or catalog enumeration
background refresh loops, retry/circuit-breaker policies, or negative caching
new REST auth, TLS, CA, proxy, DNS, or credential behavior
credentials in metadata, SQL, tickets, logs, metrics, or URLs

## Completion Criteria

Rule: iceberg-snapshot-admission — distinct external catalog loads are bounded

Scenario: A full distinct-key admission limit rejects another table before I/O
  Test:
    Package: lake-iceberg
    Filter: distinct_snapshot_loads_are_bounded_and_release_after_cancellation
  Given 64 different configured Iceberg table keys whose external loads are
    blocked
  When a 65th distinct key is resolved
  Then it receives an identity-free overload error without a 65th external
    table load, and the capacity is usable again after the blocked leaders are
    cancelled

Scenario: A matching key still shares an admitted pending load
  Test:
    Package: lake-iceberg
    Filter: concurrent_snapshot_refreshes_share_one_external_load
  Given an admitted pending load for one exact configured namespace/table key
  When additional planners resolve that same key
  Then they share its one external load rather than taking distinct-key slots

## Out of Scope

- Cross-replica or shared catalog admission.
- Configuring the hard limit per deployment.
- Changes to Iceberg snapshot retention, Flight ticket contents, or external
  catalog availability behavior beyond bounded pending-load admission.
