spec: task
name: "iceberg-rest-fixture"
inherits: project
tags: [iceberg, rest, integration, s3]
---

## Intent

Lake must prove that a deployment-configured Iceberg REST catalog can serve a
real external table through Lake SQL. The current Axum fixture validates the
wire shape, but it cannot reveal incompatibilities with an independently
implemented Apache catalog, its metadata commits, or object-store properties.

## Decisions

- Use Apache's pinned `iceberg-rest-fixture:1.10.1` and MinIO as an opt-in,
  checkout-scoped integration environment with dynamic host ports.
- Model a deployment-only S3 endpoint override as a validated value object.
  It passes only endpoint, region, path-style, and anonymous-read properties
  to the in-memory REST client; it accepts no access key, secret, signed URL,
  or arbitrary catalog property.
- Keep object credentials in the Query workload identity. The test uses a
  public, disposable MinIO bucket so Lake's S3 configuration contains no
  credential material.
- The ignored test creates an isolated namespace and Parquet file through the
  Apache authority, then proves Lake's QueryEngine reads it without touching
  Lake metadata.

## Boundaries

### Allowed Changes
crates/lake-iceberg/src/lib.rs
crates/lake-iceberg/tests/config.rs
crates/lake-query/Cargo.toml
crates/lake-query/tests/iceberg_rest_fixture.rs
crates/lake-cli/src/commands/serve.rs
scripts/AGENT.md
scripts/test-iceberg-env.ts
scripts/test-iceberg-integration.ts
scripts/test-iceberg-integration-env.ts
scripts/test-iceberg-integration-env.test.ts
mise.toml
.github/workflows/ci.yml
docs/design/iceberg-federation.md
docs/guides/cli.md
README.md
specs/issue-278-iceberg-rest-fixture.spec.md

### Forbidden
Lake registry or Metasrv changes
Iceberg writes, DDL/DML, catalog enumeration, or metadata mirroring in Lake
static S3 credentials, signed URLs, object bytes, or arbitrary REST properties in Lake configuration
a second Iceberg catalog or a required image pull in the fast `mise run gate`

## Completion Criteria

Rule: external-iceberg-rest-interoperability — a real Apache REST catalog is
  reachable through Lake's bounded, read-only query path

Scenario: S3 deployment override accepts only safe object-store configuration
  Test:
    Package: lake-iceberg
    Filter: s3_storage_configuration_requires_a_credential_free_endpoint
  Given a Query deployment needs a non-default S3-compatible endpoint
  When it creates an Iceberg S3 configuration
  Then credential-bearing or plaintext remote endpoints are rejected while TLS
  and numeric-loopback development endpoints remain usable

Scenario: Lake reads an Apache REST catalog table from real object storage
  Test:
    Package: lake-query
    Filter: apache_rest_catalog_fixture_runner_selects_real_interoperability_test
  Given the explicit Apache REST fixture integration environment
  When an independent catalog writer creates and commits a populated table
  Then Lake Query returns the committed row through
  `iceberg.<namespace>.episodes` without accessing Lake metadata

## Out of Scope

- Persistent test catalog state, catalog-provider certification beyond the
  pinned Apache fixture, S3 credential management, and production Iceberg
  mutation support.
