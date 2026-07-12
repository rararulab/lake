spec: task
name: "sdk-presigned-managed-read"
inherits: project
tags: [sdk, s3, presign, security, file]
---

## Intent

Allow a credentialed Rust SDK process to delegate one managed S3 object read
to a browser, player, or isolated worker without exposing AWS credentials or
routing object bytes through Query or Metasrv.

## Decisions

- `LakeClient::presign_read` delegates to the configured managed object store.
- A presigned read is an opaque capability type. `Debug` always redacts the
  URL; callers must explicitly request or consume the URL string.
- Expiration is caller-selected but limited to 1 second through 1 hour and is
  validated before the signing operation.
- S3 signing reuses the SDK process client, endpoint, region, path-style mode,
  and credential provider. It performs no object GET.
- The stable `DataLocation` bucket and tenant child-prefix are validated before
  signing. Prefix escapes, query-bearing S3 identities, and foreign buckets
  fail closed.
- Local and embedding stores that do not implement signing return a typed
  unsupported error.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-objects/**
crates/lake-sdk/**
README.md
docs/architecture.md
docs/design/managed-objects.md
docs/plans/2026-07-12-sdk-presigned-read.md
specs/issue-79-sdk-presigned-read.spec.md
verification/issue-79-sdk-presigned-read.md

### Forbidden
crates/lake-common/**
crates/lake-meta/**
crates/lake-query/**
crates/lake-metasrv/**
persisting signed URLs in Arrow rows or metadata
logging signed URLs
presigned uploads
unbounded or caller-unvalidated expiration
object bytes through Flight services

## Completion Criteria

Scenario: Managed S3 location becomes a bounded read capability
  Test:
    Package: lake-objects
    Filter: s3_presigned_read_is_scoped_bounded_and_redacted
  Given an S3 store bound to one managed tenant prefix and test credentials
  When an in-prefix DataLocation is signed for a valid expiration
  Then the result is a GET capability for that exact key, carries the bounded expiration, performs no network request, and redacts its URL from Debug

Scenario: Signing fails closed outside the managed boundary
  Test:
    Package: lake-objects
    Filter: presigned_read_rejects_escape_and_invalid_expiration
  Given a foreign bucket, sibling prefix, query-bearing identity, or invalid TTL
  When presigning is requested
  Then a typed boundary or expiration error is returned before signing

Scenario: Rust SDK delegates without requiring metadata connectivity
  Test:
    Package: lake-sdk
    Filter: sdk_presigned_read_delegates_to_managed_store
  Given a LakeClient with a signing-capable injected store and an unreachable Query channel
  When presign_read is called
  Then the store receives the stable DataLocation and TTL while Query and Metasrv are not contacted

## Out of Scope

- Credentialless server-side signing.
- Presigned writes.
- CDN authorization and revocation.
- Persisting or automatically logging capability URLs.
