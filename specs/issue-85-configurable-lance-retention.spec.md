spec: task
name: "configurable-lance-retention"
inherits: project
tags: [engine, lance, maintenance, retention, cli, production]
---

## Intent

Replace Lance maintenance's hard-coded ten-version retention shortcut with a
validated immutable policy that operators can configure before either local or
cloud storage is opened. The default must preserve today's behavior, while
invalid or typo-scale values fail closed instead of silently disabling useful
cleanup or retaining an unbounded history.

## Decisions

- Add a public `LanceMaintenancePolicy` value object in `lake-engine-lance`.
- Retain a count-based policy in this batch because it is deterministic,
  chrono-free, and already preserves tagged Lance versions.
- Default to 10 retained versions and accept only `1..=10000`.
- Configure the count with `LAKE_LANCE_RETAIN_VERSIONS`; parse it once at the
  CLI boundary before opening RocksDB, DynamoDB, local paths, or S3.
- Apply the same policy to local, external-manifest, and object-store engines.
- Reclaim external manifest history only after Lance cleanup succeeds, exactly
  as today.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
README.md
crates/lake-engine-lance/**
crates/lake-cli/**
docs/architecture.md
docs/guides/cli.md
docs/plans/2026-07-12-configurable-lance-retention.md
specs/issue-85-configurable-lance-retention.spec.md
verification/issue-85-configurable-lance-retention.md

### Forbidden
crates/lake-engine/**
crates/lake-meta/**
crates/lake-metasrv/**
Lance types outside lake-engine-lance
time-based retention or a new chrono dependency
deleting tagged versions
running cleanup before compaction or manifest reclamation before cleanup
runtime mutation of the maintenance policy

## Completion Criteria

Scenario: Retention policy rejects unsafe bounds
  Test:
    Package: lake-engine-lance
    Filter: maintenance_policy_rejects_unbounded_retention
  Given the public Lance maintenance policy constructor
  When retained versions is zero, greater than 10000, or the default
  Then invalid values fail and the default remains exactly ten versions

Scenario: Maintenance applies the configured history window
  Test:
    Package: lake-engine-lance
    Filter: maintenance_uses_configured_version_retention
  Given a Lance dataset with more committed versions than its configured window
  When engine maintenance completes successfully
  Then only the configured recent untagged versions remain available

Scenario: CLI validates retention before storage construction
  Test:
    Package: lake-cli
    Filter: lance_retention_values_are_validated_before_storage_open
  Given an optional `LAKE_LANCE_RETAIN_VERSIONS` value
  When process configuration is assembled
  Then missing uses ten, valid values are preserved, and zero, overflow, or non-numeric values fail

## Out of Scope

- Time-based retention or dual time/count policies.
- Per-table retention overrides.
- Changing compaction, maintenance cadence, or manifest cleanup page size.
- A distributed policy control plane or live configuration reload.
