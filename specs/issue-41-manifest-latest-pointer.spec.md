spec: task
name: "manifest-latest-pointer"
inherits: project
tags: [manifest, dynamodb, performance, migration]
---

## Intent

Lake's bounded metadata authority must not pay for all historical commits when
opening a table. Today `MetaManifestStore::get_latest_version` lists every
per-version key and then reads the maximum. On Dynamo that is a strongly
consistent table Scan with a filter. Reproducer: append thousands of versions,
open the dataset, and observe one metadata scan whose evaluated items grow
with both this dataset's history and unrelated keys.

Lance's own Dynamo store uses a partition/sort-key query for latest, but Lake's
generic single-key `MetaStore` has no backend-specific query API. This contract
therefore makes one fixed latest record authoritative while retaining immutable
per-version records for historical reads. The first read of a legacy dataset
may scan once to install the pointer; steady-state reads may not scan.

## Decisions

- Store `lance-manifest-latest/<base_uri>` as `{version, path}` and use one
  strongly consistent `MetaStore::get` for steady-state latest resolution.
- The fixed latest record is also the atomic claim for the current version.
  Before advancing it, archive the prior exact pointer into its immutable
  per-version history key, then CAS latest from exact old bytes to new bytes.
- Publishing latest in `put_if_not_exists` is safe because Lance has already
  durably written the staging manifest; readers may finalize that staging path
  using Lance's existing commit handler.
- Historical backfill writes a version key without regressing latest.
- Drop transitions fixed latest through durable `deleting` and `deleted`
  markers. Recreate replaces `deleted`; the key never becomes absent, so stale
  migration cannot revive a dropped pointer through an ABA CAS.
- Archive and historical-backfill creation atomically guard on the exact fixed
  latest bytes observed by the writer, so a pre-fence writer cannot publish
  history after drop.
- If latest is absent but legacy history exists, scan once, CAS-install the
  maximum record, and let racing migrators converge on the installed value.
- This commit-protocol boundary is not mixed-writer compatible: pre-#41
  binaries do not update the fixed pointer. Drain commit-capable nodes before
  upgrading the metadata group, then resume writes after every possible leader
  runs #41. Stored datasets need no offline rewrite and migrate on first use.
- Keep retention cleanup out of this issue; #42 couples it to authoritative
  Lance cleanup outcomes and a durable bounded cursor.

## Boundaries

### Allowed Changes
crates/lake-engine-lance/**
docs/architecture.md
docs/plans/2026-07-11-manifest-latest-pointer.md
specs/issue-41-manifest-latest-pointer.spec.md
verification/issue-41-manifest-latest-pointer.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-query/**
crates/lake-sdk/**

## Completion Criteria

Scenario: Published datasets resolve latest with one point read
  Test:
    Package: lake-engine-lance
    Filter: latest_pointer_avoids_history_scan_after_publication
  Given a dataset whose fixed latest pointer exists
  When latest is resolved repeatedly
  Then each resolution uses the fixed key and never lists version history

Scenario: Concurrent claims cannot regress latest
  Test:
    Package: lake-engine-lance
    Filter: concurrent_manifest_claims_never_regress_latest
  Given two writers racing to claim the same next version
  When both archive the same prior pointer and CAS latest
  Then exactly one claim wins and latest remains at the winning next version

Scenario: Legacy history migrates once
  Test:
    Package: lake-engine-lance
    Filter: legacy_history_installs_latest_pointer_once
  Given only legacy per-version records
  When latest is first resolved and then resolved again
  Then the first call installs the maximum as fixed latest and the second call performs no history scan

Scenario: Migrated staging history finalizes without blocking advancement
  Test:
    Package: lake-engine-lance
    Filter: legacy_staging_finalize_can_advance
  Given a legacy maximum version still points at a staging manifest
  When the fixed pointer is installed, finalized, and the next version is claimed
  Then both the duplicate legacy archive and latest converge to final before advancement

Scenario: Current and historical reads preserve Lance semantics
  Test:
    Package: lake-engine-lance
    Filter: current_and_historical_version_reads_survive_pointer_layout
  Given latest has advanced and its prior version was archived
  When Lance resolves both versions explicitly
  Then both exact manifest paths are returned and historical backfill cannot regress latest

Scenario: Dataset deletion clears both layouts
  Test:
    Package: lake-engine-lance
    Filter: delete_clears_latest_and_history
  Given fixed latest plus archived historical records
  When the external manifest dataset is deleted
  Then no live latest or historical version remains and recreate replaces the durable deleted marker

Scenario: Delete fences migration and recreate races
  Test:
    Package: lake-engine-lance
    Filter: delete_fence_blocks_stale_migration_and_recreate
  Given migration has read legacy history or recreate races an active drop
  When deletion owns the fixed-key fence
  Then neither operation can publish until durable deletion finishes

Scenario: Concurrent finalizers converge
  Test:
    Package: lake-engine-lance
    Filter: concurrent_finalize_converges_on_same_path
  Given current, historical, or migrated staging pointers
  When two finalizers install the same final path concurrently
  Then both calls succeed and observe the same installed path

Scenario: History creation cannot cross the delete fence
  Test:
    Package: lake-engine-lance
    Filter: history_create_is_guarded_by_exact_latest
  Given archive or historical backfill has read the old fixed pointer
  When drop installs its deletion fence before history creation
  Then the guarded history transaction fails and deletion leaves no old history

## Out of Scope

- Physical DynamoDB partition/sort-key migration.
- Deleting retained or obsolete external manifest history; tracked by #42.
- Changing Lance's manifest file format, naming scheme, or commit handler.
- Query-layer provider caching or catalog refresh behavior.
- Simultaneous commits from binaries on both sides of the #41 protocol boundary.
