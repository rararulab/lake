spec: task
name: "dynamo-prefix-isolation"
inherits: project
tags: [meta, dynamodb, performance, migration, consistency]
---

## Intent

Make Dynamo metadata prefix work proportional to the requested key family and
page size. Catalog refresh for `tbl/` must not evaluate retained append
operations or manifest records, while migration preserves the exact CAS,
guarded-mutation, and incarnation semantics of the current authority.

## Decisions

- Use a strongly consistent companion v2 table, not an eventually consistent
  GSI over the v1 table.
- Partition each logical family across 64 stable shards and keep the complete
  logical key as the range key.
- Point reads address one shard. Prefix pages advance through shards with an
  opaque versioned cursor and one bounded Dynamo Query per page.
- Upgrade through v1 authority, atomic dual writes, bounded conditional
  backfill, exact verification, and a monotonic completion marker.
- Finalization requires every commit-capable metadata node to be dual-capable;
  mixed v1-only writers after the marker are an explicit deployment violation.

## Boundaries

### Allowed Changes
crates/lake-meta/**
crates/lake-cli/**
Cargo.lock
README.md
deploy/**
docs/architecture.md
docs/design/dynamo-prefix-layout.md
docs/guides/kubernetes.md
docs/guides/local-deploy.md
docs/plans/2026-07-12-dynamo-prefix-isolation.md
specs/issue-94-dynamo-prefix-isolation.spec.md
verification/issue-94-dynamo-prefix-isolation.md

### Forbidden
crates/lake-engine*/**
crates/lake-metasrv/**
crates/lake-query/**
changing logical MetaStore key formats
eventually consistent authority reads
unbounded backfill or verification scans
overwriting a conflicting v2 value during migration
publishing completion without exact verification
increasing retention or scan limits as the fix

## Completion Criteria

Scenario: Mixed families map to isolated stable shards
  Test:
    Package: lake-meta
    Filter: dynamo_v2_layout_is_stable_and_family_isolated
  Given registry, operation, manifest, and root logical keys
  When their v2 physical keys are derived
  Then family buckets are distinct, complete keys are preserved, and shard assignment is deterministic within 00..3f

Scenario: Prefix pages never evaluate unrelated families
  Test:
    Package: lake-meta
    Filter: dynamo_v2_prefix_pages_are_query_bounded
  Given more unrelated operation records than the requested registry page size
  When a registry prefix page is read from v2
  Then one strongly consistent Query evaluates at most the page limit and returns only registry keys

Scenario: Sharded continuation returns every matching key exactly once
  Test:
    Package: lake-meta
    Filter: dynamo_v2_cursor_resumes_across_shards
  Given matching keys spread across multiple shards
  When pages of size one are consumed through opaque continuations
  Then every matching key appears exactly once and the final continuation is absent

Scenario: Dual CAS cannot diverge physical copies
  Test:
    Package: lake-meta
    Filter: dynamo_dual_cas_is_atomic_and_fail_closed
  Given matching v1 and v2 values plus competing expected bytes
  When create, update, and delete CAS operations race
  Then both copies move together or neither moves and no stale value wins

Scenario: Guarded mutations preserve exact fencing in dual mode
  Test:
    Package: lake-meta
    Filter: dynamo_dual_guarded_mutation_preserves_fence
  Given an exact lease guard and dual target copies
  When the guard changes before a stale target mutation
  Then neither physical target changes

Scenario: Backfill converges with a concurrent dual writer
  Test:
    Package: lake-meta
    Filter: dynamo_v2_backfill_never_overwrites_newer_value
  Given a v1 record scanned before a concurrent dual update
  When backfill encounters the newer v2 value
  Then it reloads authority and converges without restoring the scanned stale bytes

Scenario: Finalization rejects incomplete or unverified migration
  Test:
    Package: lake-meta
    Filter: dynamo_v2_finalize_requires_exact_verified_backfill
  Given missing, extra, or value-divergent v2 records
  When finalization is requested
  Then the completion marker remains absent and the mismatch is reported

Scenario: Legacy deployments remain readable before finalization
  Test:
    Package: lake-meta
    Filter: dynamo_v1_dual_v2_migration_roundtrip
  Given an existing v1 LocalStack table and a rolling dual-mode deployment
  When bounded backfill completes and the marker is finalized
  Then pre-marker reads use v1, post-marker reads use v2, and exact values survive the transition

## Out of Scope

- Deleting the legacy v1 table in the same release.
- Changing RocksDB physical layout.
- Cross-region Dynamo replication or global-table conflict resolution.
- Removing the engine-neutral `MetaStore` abstraction.
