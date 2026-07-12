spec: task
name: "managed-s3-protocol-ci"
inherits: project
tags: [objects, sdk, s3, localstack, ci, presign]
---

## Intent

Continuously exercise Lake's real managed-S3 large-object path. Today the
ignored `lake-objects` and `lake-sdk` LocalStack tests run through the local
integration script, but GitHub CI duplicates a narrower package list and skips
both crates. A regression in multipart upload, resumability, direct Range GET,
stage discovery, or SDK FILE insertion can therefore merge while the
production-protocol tests remain green only on paper.

## Decisions

- Keep one integration package list in `scripts/test-integration.ts`.
- Add an external-environment mode that uses an already provisioned LocalStack
  service without starting or stopping Docker; GitHub CI uses this mode.
- Preserve the existing local mode that owns checkout-scoped LocalStack
  lifecycle and always tears it down.
- Run ignored tests from `lake-objects`, `lake-sdk`, `lake-meta`, and
  `lake-engine-lance` in both modes.
- Add a real LocalStack protocol test that uploads an object, mints a presigned
  GET, and performs a Range request with the returned URL and required headers.
- Keep protocol tests ignored outside the explicit integration runner.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
mise.toml
crates/lake-objects/**
scripts/test-integration*.ts
scripts/AGENT.md
.github/workflows/ci.yml
docs/guides/mise-ci.md
docs/plans/2026-07-12-s3-protocol-ci.md
specs/issue-81-s3-protocol-ci.spec.md
verification/issue-81-s3-protocol-ci.md

### Forbidden
crates/lake-common/**
crates/lake-meta/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-sdk/**
production S3 credentials in CI
non-ignored tests that require Docker or LocalStack
duplicated integration package lists between local and CI runners
object bytes through Query or Metasrv

## Completion Criteria

Scenario: Presigned capability performs a real ranged S3 read
  Test:
    Package: lake-objects
    Filter: s3_presigned_range_get_localstack_is_wired
  Given a managed S3 object and a LocalStack endpoint
  When the store mints a bounded read capability and an HTTP client adds Range
  Then the integration suite proves the response is partial content with the exact requested bytes

Scenario: CI and local development share the managed-object integration runner
  Test:
    Package: lake-objects
    Filter: managed_s3_integration_runner_is_shared_with_ci
  Given ignored production-protocol tests in lake-objects and lake-sdk
  When GitHub CI runs against its LocalStack service container
  Then a named mise task invokes the same package list and ignored-only policy as the checkout-scoped local runner

## Out of Scope

- Running LocalStack tests in the fast `mise run gate` loop.
- Real AWS credentials or deployment against an external bucket.
- Browser-specific CORS policy.
- Presigned uploads.
