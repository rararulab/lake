spec: task
name: "incremental-object-reference-gc"
inherits: project
tags: [objects, engine, lance, s3, gc, lifecycle]
---

## Intent

Reclaim completed managed objects that never became reachable from a committed
table version, without scanning all episode rows, blocking Metasrv, or deleting
objects still needed by a retained snapshot.

## Decisions

- Every version-producing engine operation writes an immutable, versioned
  reference-delta sidecar before returning the version to the metadata CAS.
- Append deltas contain sorted/deduplicated `DataLocation` identities extracted
  while batches stream into the engine; they never retain consumed batches.
- A delta names its parent version plus added and removed identities. Append is
  additive today; the model supports future row deletion and compaction.
- The registry version is the reachability root. GC traverses sidecar lineage
  for the current and engine-retained snapshots. Missing, corrupt, cyclic, or
  mismatched lineage fails closed for that table and prevents deletion.
- Candidate enumeration is paginated and age-gated. Objects newer than the
  configured safety horizon are never planned or deleted.
- Planning and deletion run in a separate CLI worker, not Query or Metasrv.
  Dry-run is the default; mutation requires an explicit apply flag.
- Apply consumes a deterministic plan, persists page/checkpoint progress, and
  treats already-absent objects as idempotent success.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-common/**`
- `crates/lake-engine/**`
- `crates/lake-engine-lance/**`
- `crates/lake-objects/**`
- `crates/lake-metasrv/**`
- `crates/lake-cli/**`
- `README.md`
- `docs/architecture.md`
- `docs/design/**`
- `docs/plans/**`
- `specs/**`
- `verification/**`
- `**/.github/**`
- `**/AGENT.md`
- `**/CLAUDE.md`
- `**/docs/guides/mise-ci.md`
- `**/docs/guides/workflow.md`
- `**/mise.toml`

The final six patterns account for shared-checkout history from merged issue
12 that the repository-wide worktree verifier still reports. This workspace
does not edit those paths.

### Forbidden
- Scanning complete table contents to discover live objects
- Running object inventory or deletion in Query or Metasrv maintenance
- Deleting an object younger than the safety horizon
- Continuing deletion when any retained table lineage is unknown or corrupt
- Buffering an unbounded inventory, reference set, or deletion plan page
- Treating dry-run output as authorization to mutate storage

## Completion Criteria

Scenario: Reference deltas are canonical and fail closed on corruption
  Test:
    Package: lake-common
    Filter: object_reference_delta_roundtrips_canonically
  Given duplicate unordered DataLocations and a parent/new table version
  When a reference delta is built, encoded, and decoded
  Then identities are sorted and deduplicated and corrupt/version-mismatched
  input returns a typed error

Scenario: Lance append journals references without retaining streamed batches
  Test:
    Package: lake-engine-lance
    Filter: append_writes_object_reference_delta_without_retaining_batches
  Given streamed batches containing FILE values
  When Lance commits an append
  Then its sidecar links parent to new version, contains the FILE identities,
  and prior consumed batches are released before the stream ends

Scenario: Retained lineage enumeration survives compaction
  Test:
    Package: lake-engine-lance
    Filter: retained_object_references_follow_version_lineage
  Given multiple append deltas followed by a version-producing maintenance run
  When the engine enumerates references rooted at retained versions
  Then every reachable addition minus removals is returned without scanning
  table RecordBatches

Scenario: GC planning is age-gated bounded and fail-closed
  Test:
    Package: lake-objects
    Filter: gc_plan_marks_only_old_unreferenced_managed_objects
  Given paginated candidates, retained reference pages, a safety horizon, and
  one table with unknown lineage
  When a dry-run plan is constructed
  Then no deletion is authorized until all lineage is known; with complete
  lineage only old unreferenced managed objects appear in deterministic pages

Scenario: Applied S3 GC resumes idempotently
  Test:
    Package: lake-objects
    Filter: s3_gc_apply_resumes_from_checkpoint_localstack
  Given a persisted dry-run plan containing old orphan objects
  When apply is interrupted and restarted
  Then completed pages are not repeated, absent objects count as success, live
  objects remain, and the final checkpoint records completion

Scenario: CLI defaults to dry-run and requires explicit apply
  Test:
    Package: lake-cli
    Filter: gc_command_is_dry_run_unless_apply_is_explicit
  Given local or cloud storage configuration
  When `lake gc` is parsed and executed without or with `--apply`
  Then the default emits a deterministic plan without deletion and mutation is
  routed only through the checkpointed apply path

## Out of Scope

- Cross-region replication cleanup
- S3 Inventory report generation configuration
- Tenant-specific retention policy
- Row-level DELETE SQL
- Deduplicating objects shared across independent Lake deployments
