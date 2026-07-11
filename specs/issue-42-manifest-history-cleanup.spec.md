spec: task
name: "manifest-history-cleanup"
inherits: project
tags: [manifest, cleanup, retention, dynamodb]
---

## Intent

Lance cleanup removes obsolete manifest objects after evaluating retention,
tags, and referenced branches, but Lake's external per-version records remain
forever. Dynamo scan cost therefore grows with commit history even after the
corresponding snapshot is physically gone. Lake must reclaim only records
whose manifest object is authoritatively absent after successful Lance cleanup,
and must bound metadata work per maintenance call.

## Decisions

- Lance remains the retention authority. Do not infer deletion from a numeric
  cutoff or duplicate Lance's tag/branch reachability logic.
- Run Lance cleanup first. Only after it succeeds, inspect a bounded page of
  external history and delete a record when the exact stored manifest path no
  longer exists in Lance's object store.
- Guard every history delete and cursor mutation with the exact incarnation-
  bound latest pointer. Drop/recreate or a concurrent commit makes stale
  maintenance fail closed.
- Persist a per-dataset, incarnation-bound continuation under
  `lance-manifest-cleanup/<base_uri>`. Replay after a crash is idempotent;
  reaching the end resets the next sweep to the prefix start.
- Keep the fixed latest pointer and durable delete markers untouched.
- The page limit bounds metastore records and object HEAD requests. Physical
  Lance cleanup retains its existing policy and object-store rate behavior.

## Boundaries

### Allowed Changes
crates/lake-engine-lance/**
docs/architecture.md
docs/plans/2026-07-12-manifest-history-cleanup.md
specs/issue-42-manifest-history-cleanup.spec.md
verification/issue-42-manifest-history-cleanup.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-query/**
crates/lake-sdk/**

## Completion Criteria

Scenario: Removed manifest history is reclaimed in bounded pages
  Test:
    Package: lake-engine-lance
    Filter: removed_manifest_history_is_reclaimed_boundedly
  Given more obsolete external records than one maintenance page
  When Lance cleanup has removed their exact manifest objects
  Then each call performs at most the configured HEAD/delete budget and repeated calls reclaim all obsolete records

Scenario: Retained snapshots keep their external records
  Test:
    Package: lake-engine-lance
    Filter: retained_manifest_history_survives_cleanup
  Given retained or tagged manifest objects still exist after Lance cleanup
  When external history reconciliation visits their records
  Then those exact records remain readable

Scenario: Cleanup cursor resumes without touching latest
  Test:
    Package: lake-engine-lance
    Filter: cleanup_cursor_resumes_without_touching_latest
  Given cleanup stops after a bounded page and later resumes
  When the durable cursor advances under the same incarnation
  Then it continues after the prior page and the fixed latest pointer is byte-for-byte unchanged

Scenario: S3 and Dynamo cleanup agree on physical outcomes
  Test:
    Package: lake-engine-lance
    Filter: external_manifest_cleanup_localstack
  Given a Lance dataset on LocalStack S3 with external history in DynamoDB
  When maintenance removes obsolete manifests and reconciles one or more pages
  Then only Dynamo records whose S3 manifest paths are absent are removed

## Out of Scope

- Changing Lance tag, branch, or time-retention semantics.
- A DynamoDB partition/sort-key migration for manifest history.
- Deleting the fixed latest pointer or durable deletion marker.
- Making Lance physical file cleanup itself page-sized.
