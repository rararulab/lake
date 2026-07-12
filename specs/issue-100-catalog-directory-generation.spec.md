spec: task
name: "catalog-directory-generation"
inherits: project
tags: [catalog, metadata, dynamodb, cache, performance, migration]
---

## Intent

Every Query replica currently scans and deserializes the complete `tbl/`
registry every five seconds. At the target of roughly ten thousand tables that
is about two thousand registration reads per second per replica, even when the
directory has not changed. Query fan-out therefore multiplies steady metadata
traffic instead of shielding the bounded authority.

Reproducer: create 10,000 stable tables, warm one Query catalog, and let its
background refresh run without DDL. Each tick still scans all registrations;
adding Query replicas multiplies the same scan. Ordinary append commits also
rewrite `current_version` inside those registrations even though listing and
schema state are unchanged.

This work advances `goal.md`'s requirement that reader fan-out produce roughly
O(cache-miss), not O(readers), metadata traffic. It does not add cross-table
transactions, route readers through Metasrv, or introduce a storage-node tier.

## Decisions

- Store one opaque directory generation and one monotonic authority marker
  under reserved internal metastore keys.
- Registry create and exact delete atomically apply their conditional table
  mutation and replace the directory generation in the same backend
  transaction. RocksDB uses one write batch; DynamoDB uses one
  `TransactWriteItems`, including dual-layout and leader-guard conditions.
- Version-only `registry::set_version` and legacy incarnation migration do not
  change the directory generation because listings and schemas are unchanged.
- Before the durable authority marker exists, every refresh keeps the legacy
  full-scan behavior. This is the mixed-writer compatibility mode.
- Publish authority only through an explicit, idempotent CLI finalizer that
  requires acknowledgements that every registry writer is generation-capable
  and write admission is quiescent. Authority is monotonic and has no routine
  rollback path.
- Once authoritative, Query point-reads the generation. An unchanged value
  updates refresh health without scanning. A changed value performs a full
  scan and re-reads generation before publishing; a concurrent change rejects
  that candidate and retries within a finite bound while serving last-good.
- Old Query binaries remain safe because they ignore the marker and continue
  scanning. Running an old writer after acknowledged finalization is forbidden
  by the rollout contract and documented as a fail-closed deployment boundary.
- Generation and authority values are operational control data only. They
  never enter metric labels, logs, SQL results, or public Flight schemas.

## Boundaries

### Allowed Changes
crates/lake-meta/**
crates/lake-catalog/**
crates/lake-metasrv/**
crates/lake-query/**
crates/lake-cli/**
docs/architecture.md
docs/design/catalog-directory-generation.md
docs/guides/cli.md
docs/guides/kubernetes.md
docs/plans/2026-07-12-catalog-directory-generation.md
specs/issue-100-catalog-directory-generation.spec.md
verification/issue-100-catalog-directory-generation.md

### Forbidden
crates/lake-engine*/**
crates/lake-sdk/**
table row or object data in metadata
public Flight wire schemas
cross-table SQL transactions
DynamoDB GSIs or new tables
reader connections to Metasrv
directory generation in metric labels

## Completion Criteria

Scenario: Registry directory mutations publish one atomic generation
  Test:
    Package: lake-meta
    Filter: signaled_registry_mutations_are_atomic
  Given an authoritative directory and one table registration
  When register succeeds, version advances, a conflicting mutation fails, and exact delete succeeds
  Then only successful register and delete change generation and no observer sees one without the other

Scenario: Unchanged authoritative generation skips the registry scan
  Test:
    Package: lake-catalog
    Filter: catalog_generation_skips_unchanged_registry_scan
  Given a warmed authoritative catalog and no directory DDL
  When repeated refreshes run across the normal staleness interval
  Then each refresh performs bounded point reads and the complete registry scan count stays at one

Scenario: Append version churn does not invalidate directory listings
  Test:
    Package: lake-catalog
    Filter: append_version_churn_does_not_invalidate_directory_generation
  Given an authoritative catalog containing one table
  When its registry current version advances several times
  Then generation remains unchanged and catalog refresh performs no new directory scan

Scenario: Compatibility mode remains safe for legacy writers
  Test:
    Package: lake-catalog
    Filter: legacy_writer_mode_keeps_full_catalog_revalidation
  Given no durable directory-authority marker
  When a legacy-style registration appears without a generation update
  Then the next refresh scans and publishes it instead of trusting the unchanged generation

Scenario: Concurrent directory changes cannot publish a mixed snapshot
  Test:
    Package: lake-catalog
    Filter: catalog_generation_change_during_scan_preserves_last_good
  Given a warmed authoritative catalog whose generation changes during a scan
  When refresh validates the candidate generation
  Then it does not publish that candidate and retains the prior immutable listing/schema snapshot

Scenario: Stale metadata leaders cannot signal directory changes
  Test:
    Package: lake-metasrv
    Filter: stale_leader_cannot_publish_directory_generation
  Given a fenced leader whose exact lease has been replaced
  When it attempts a signaled registry mutation
  Then neither the table registration nor directory generation changes

Scenario: Authority finalization requires explicit rollout acknowledgements
  Test:
    Package: lake-cli
    Filter: catalog_generation_finalize_requires_rollout_acknowledgements
  Given missing, partial, or complete rollout and write-quiescence acknowledgements
  When the finalization command is parsed
  Then only the fully acknowledged command is accepted and repeat finalization is idempotent

Scenario: Dynamo dual-layout signaling is integration-wired
  Test:
    Package: lake-meta
    Filter: dynamo_catalog_generation_atomicity_localstack_is_wired
  Given the shared LocalStack integration runner
  When v1 dual-write and v2-authoritative registry mutations execute
  Then target and generation move atomically in both layouts and failed conditions move neither

## Out of Scope

- Incremental change-log replay; a changed generation still triggers one full
  bounded-family registry scan.
- Splitting version pointers out of table registrations.
- Automatically inferring that all external writers have been upgraded.
- Changing the five-second refresh cadence or public discovery semantics.
