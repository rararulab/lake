spec: task
name: "credentialless-managed-read-capabilities"
inherits: project
tags: [sdk, query, s3, presign, security, file]
---

## Intent

Allow a Rust SDK process without AWS credentials to obtain a short-lived HTTPS
GET capability for an immutable managed S3 `DataLocation` from its authenticated
Query endpoint. Today `LakeClient::presign_read` can sign only with the SDK
process's own IAM credentials. A browser/player/isolated worker that is given a
valid `DataLocation` therefore cannot stream a multi-gigabyte video without
also receiving cloud credentials.

Reproducer: configure a production S3 Query service with its normal managed
stage credentials; connect an authenticated SDK from a process with no AWS
credentials; call the existing direct-read path for an in-tenant video. It
cannot construct a direct S3 reader or a presigned URL, even though Query can
authorize the tenant and the service has the required S3 signer identity.

## Decisions

- Reuse AWS SDK GET presigning; do not proxy object bytes through Query or
  Metasrv and do not issue a new storage protocol.
- Add one authenticated Query Flight action whose request contains exactly one
  immutable `DataLocation` plus a caller-selected 1s..=1h expiry. Its response
  is an opaque, redacted `PresignedRead`-equivalent capability containing the
  URL, required headers, and expiry.
- Query derives the caller's `tenants/<tenant-id>` managed prefix before it
  asks its injected capability issuer to sign. Foreign buckets, sibling or
  escaping keys, query-bearing S3 identities, malformed body, unsupported
  local stages, and invalid expiry fail before a signed URL is returned.
- The server issuer is optional and is constructed only for the S3 managed
  stage by `lake query`; local deployments return an explicit failed
  precondition. It uses the Query process's existing AWS identity and never
  exposes that identity to the SDK.
- `LakeClient` exposes the remote capability method separately from the
  existing local-IAM `presign_read` method so an existing credentialed caller
  preserves its no-RPC behavior.
- Capability URL/header values are never logged, placed in Arrow rows,
  persisted in a checkpoint, or exposed by `Debug`.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-cli/**
crates/lake-common/**
crates/lake-objects/**
crates/lake-query/**
crates/lake-sdk/**
docs/architecture.md
docs/plans/**
README.md
specs/issue-140-credentialless-read-capabilities.spec.md
verification/issue-140-credentialless-read-capabilities.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
persisting signed URLs, headers, or AWS credentials
logging signed URLs, headers, AWS credentials, SQL text, or object content
presigned uploads, object writes, or object bytes through Query or Metasrv
an unauthenticated or non-TLS production capability endpoint
changing existing local-IAM `LakeClient::presign_read` semantics

## Acceptance Criteria

Rule: managed-read-capability — Query-authorized and tenant-scoped

Scenario: Query issues one tenant-scoped, bounded remote read capability
  Test:
    Package: lake-query
    Filter: managed_read_capability_action_scopes_tenant_and_redacts_response
  Given an authenticated tenant principal and an injected signing issuer
  When it requests a capability for an in-prefix managed S3 `DataLocation`
  Then the issuer receives only that tenant-scoped location and valid expiry,
  and the returned URL is redacted from Debug

Scenario: Query denies unsafe or unavailable capability requests before signing
  Test:
    Package: lake-query
    Filter: managed_read_capability_action_fails_closed_before_signing
  Given an absent issuer, malformed action body, invalid expiry, or a location
  outside the tenant managed prefix
  When an authenticated principal invokes the read-capability action
  Then the action returns a typed Flight precondition/invalid-argument error
  and the issuer is not called

Rule: sdk-remote-capability — the SDK can receive a capability without cloud credentials

Scenario: SDK receives a server-issued capability without SDK cloud credentials
  Test:
    Package: lake-sdk
    Filter: sdk_remote_read_capability_uses_query_action_without_stage_store
  Given an authenticated SDK client with a Query action endpoint and no managed
  object store
  When it requests a remote read capability for a `DataLocation`
  Then it sends the bounded action, decodes the opaque capability, and does not
  perform stage discovery, construct an S3 client, or contact Metasrv

Rule: s3-only-issuer — signing authority exists only in an S3 Query deployment

Scenario: CLI enables signing only for the S3 managed stage
  Test:
    Package: lake-cli
    Filter: query_server_capability_issuer_is_s3_only
  Given local and S3 Query server contexts
  When each builds Query server configuration
  Then only the S3 context installs a managed-read capability issuer

## Out of Scope

- Browser upload capabilities, CDN authorization/revocation, and codec indexes.
- Arbitrary S3 signing outside the caller's managed tenant prefix.
- Changing table authorization, query ticket contents, or Metadata control
  plane APIs.
- Cross-process quotas, billing, or retention for capabilities.
