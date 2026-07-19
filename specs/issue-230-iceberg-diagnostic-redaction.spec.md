spec: task
name: "iceberg-diagnostic-redaction"
inherits: project
tags: [iceberg, security, diagnostics]
---

## Intent

Lake's external Iceberg configuration already redacts REST credentials from
diagnostics, but `IcebergCatalogConfig` formats its warehouse identifier
verbatim in `Debug`. A warehouse can contain tenant, account, bucket, or
vendor-specific routing information, and compatible URI forms can resemble
credential-bearing authority syntax. That violates the read federation
boundary: deployment-only external details must not escape into diagnostic
output.

Reproducer: construct `IcebergCatalogConfig` with a synthetic
credential-looking warehouse string, then log or format it with `{:?}`. Before
this task, the full string appears in the result even though REST auth is
redacted.

## Decisions

- Preserve the warehouse string for `warehouse()` and the actual REST catalog
  connection. Lake must not impose URI parsing or new validation because valid
  object-store identifiers differ by provider.
- Format the `Debug` warehouse field as an opaque configured marker, matching
  the existing REST-auth marker, rather than exposing the identifier.
- Keep the endpoint and configured namespace diagnostics unchanged: the
  endpoint already rejects userinfo and the namespace allowlist is not secret.
- Document the redaction boundary in the Iceberg federation operational
  contract.

## Boundaries

### Allowed Changes
crates/lake-iceberg/src/lib.rs
crates/lake-iceberg/tests/config.rs
docs/design/iceberg-federation.md
specs/issue-230-iceberg-diagnostic-redaction.spec.md
verification/issue-230-iceberg-diagnostic-redaction.md

### Forbidden
crates/lake-cli/**
crates/lake-query/**
crates/lake-meta/**
Cargo.toml
Cargo.lock
docs/guides/cli.md
Iceberg catalog connection behavior
warehouse URI validation
Iceberg writes, catalog mutation, or metadata mirroring

## Completion Criteria

Rule: iceberg-diagnostic-redaction — deployment-only warehouse identifiers do
  not appear in Iceberg configuration diagnostics

Scenario: Debug output redacts a credential-looking warehouse identifier
  Test:
    Package: lake-iceberg
    Filter: iceberg_catalog_config_debug_redacts_warehouse
  Given an Iceberg configuration whose warehouse identifier contains a unique
    credential-looking component
  When the configuration is formatted with `Debug`
  Then the raw identifier and its sensitive component are absent, the output
  records an opaque configured warehouse marker, and `warehouse()` still
  returns the original identifier

## Out of Scope

- Changing warehouse URI syntax, storage authentication, or REST catalog
  connection behavior.
- Persisting Iceberg configuration, adding catalog types, or changing
  Iceberg's read-only federation policy.
