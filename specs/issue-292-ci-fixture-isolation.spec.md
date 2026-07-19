spec: task
name: "ci-fixture-isolation"
inherits: project
tags: [ci, localstack, iceberg, integration]
---

## Intent

Lake's post-merge LocalStack CI job currently executes every ignored test in
`lake-query`. The Apache Iceberg REST test is one of them, but it requires the
separate Apache REST + MinIO fixture and its explicit environment. The
LocalStack job consequently fails before the independently configured Iceberg
job can provide its interoperability signal.

Reproducer: run `mise run test-integration-external` with only the LocalStack
environment (`LAKE_DYNAMODB_ENDPOINT` and `LAKE_S3_ENDPOINT`). The runner
selects `apache_rest_catalog_with_minio_is_queryable`, which fails because
`LAKE_ICEBERG_TEST_REST_ENDPOINT` is absent.

## Decisions

- Keep one package list and one ignored-only policy in
  `scripts/test-integration.ts` for both checkout-owned and CI-managed
  LocalStack runs, as established by issue #81.
- Exclude only `apache_rest_catalog_with_minio_is_queryable` with a nextest
  filter expression. That test stays owned by `scripts/test-iceberg-integration.ts`
  and its Apache REST + MinIO fixture.
- Add a regression test alongside the existing Iceberg runner wiring test so
  either a removed exclusion or a removed dedicated selection is caught before
  CI.

## Boundaries

### Allowed Changes
crates/lake-query/tests/iceberg_rest_fixture.rs
scripts/test-integration.ts
scripts/AGENT.md
docs/guides/mise-ci.md
specs/issue-292-ci-fixture-isolation.spec.md

### Forbidden
.github/workflows/ci.yml
mise.toml
crates/lake-iceberg/**
crates/lake-meta/**
crates/lake-metasrv/**
Lake registry or Metasrv changes
Iceberg protocol behavior, fixture images, or object-storage credentials
duplicated LocalStack package lists or an Iceberg environment fallback in the LocalStack runner

## Completion Criteria

Rule: fixture-specific-ignored-test-selection — every ignored integration test
  runs only with the fixture environment it requires

Scenario: LocalStack runner excludes the Apache REST fixture test
  Test:
    Package: lake-query
    Filter: localstack_runner_excludes_apache_rest_fixture_test
  Given the LocalStack runner includes `lake-query` ignored tests
  When it constructs the nextest invocation for local or external LocalStack
  Then it keeps the shared package list and ignored-only policy while excluding
  only the Apache REST fixture test

Scenario: Missing Apache REST fixture environment is not masked by LocalStack
  Test:
    Package: lake-query
    Filter: localstack_runner_excludes_apache_rest_fixture_test
  Given a LocalStack-only environment without
  `LAKE_ICEBERG_TEST_REST_ENDPOINT`
  When the LocalStack runner constructs its ignored-test invocation
  Then it excludes the Apache REST fixture test instead of relying on an
  environment fallback or failing for a missing fixture endpoint

Scenario: Apache REST runner retains its explicit test selection
  Test:
    Package: lake-query
    Filter: apache_rest_catalog_fixture_runner_selects_real_interoperability_test
  Given the dedicated Apache REST + MinIO runner
  When it constructs its nextest invocation
  Then it still selects `apache_rest_catalog_with_minio_is_queryable` and owns
  the fixture lifecycle

## Out of Scope

- Changing the CI workflow, broadening the LocalStack fixture, or teaching the
  LocalStack runner Iceberg environment variables.
- Iceberg catalog writes, authentication changes, or production deployment
  behavior.
