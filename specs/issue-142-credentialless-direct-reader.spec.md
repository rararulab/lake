spec: task
name: "credentialless-direct-reader"
inherits: project
tags: [sdk, query, s3, http, streaming, security, file]
---

## Intent

Let an authenticated Rust SDK process with no cloud credentials stream a
multi-gigabyte managed video/model directly from the immutable `DataLocation`
returned by SQL. The existing Query-only client obtains a server-issued GET
capability but leaves sensitive HTTP handling to every application, so it does
not yet provide the same direct-reader experience as a credentialed SDK.

## Decisions

- `LakeClient` adds Query-only full and half-open range readers. Each asks
  Query for one bounded capability, then streams the HTTP response directly
  between the SDK and object storage; Query and Metasrv never proxy bytes.
- A full reader retains the existing constant-memory declared-size and
  SHA-256-at-EOF verification. A range reader validates its requested interval
  and accepts only an exact `206 Content-Range` response; a partial interval
  intentionally makes no whole-object hash claim.
- The SDK HTTP client disables redirects, forwards the issuer-required headers
  and a caller range when applicable, uses Rustls, and maps URL/header-bearing
  transport failures to opaque typed errors.
- Query-only readers retain their isolation: no managed-stage discovery, no
  SDK S3 client, no cloud credentials, and no fallback to local-IAM `open` or
  `presign_read`.
- Dropping a returned reader drops its response body rather than draining it in
  the background. No request or response path collects object-sized bytes.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-objects/**
crates/lake-sdk/**
docs/architecture.md
docs/plans/**
README.md
specs/issue-142-credentialless-direct-reader.spec.md
verification/issue-142-credentialless-direct-reader.md

### Forbidden
crates/lake-cli/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-query/**
proxying object bytes through Query or Metasrv
cloud credential discovery or a managed-stage client in the Query-only reader
logging, persisting, or exposing signed URL/header values in public errors
buffering a complete video/model in SDK memory
weakening existing credentialed `open`, `open_range`, or `presign_read` behavior

## Acceptance Criteria

Rule: query-only-full-reader — direct full reads are streamed and verified

Scenario: Query-only SDK streams one full managed object without cloud credentials
  Test:
    Package: lake-sdk
    Filter: query_only_full_read_streams_and_verifies_without_stage_access
    Level: integration
    Test Double: in-process Query action and HTTP object endpoints
  Given a Query-only SDK client, a valid tenant `DataLocation`, and a server
  that issues one required-header HTTP capability
  When the SDK opens and drains the object through the Query-only reader
  Then Query observes exactly one capability action, object storage observes
  the signed headers, no stage/object client is constructed, and the streamed
  bytes match the immutable size and SHA-256 identity

Rule: query-only-range-reader — seeking is exact and bounded

Scenario: Query-only SDK reads exactly one validated byte interval
  Test:
    Package: lake-sdk
    Filter: query_only_range_reader_requires_exact_partial_response
    Level: integration
    Test Double: in-process Query action and HTTP object endpoints
  Given a Query-only SDK client and a valid non-empty half-open byte interval
  When it opens that interval through a server-issued capability
  Then the direct request carries the exact HTTP Range header and only an
  exact matching `206 Content-Range` response yields the requested bytes

Rule: query-only-reader-fail-closed — sensitive HTTP failures do not escape

Scenario: Query-only reader rejects unsafe capability responses without secret leakage
  Test:
    Package: lake-sdk
    Filter: query_only_reader_fails_closed_and_redacts_capability
    Level: integration
    Test Double: in-process Query action and HTTP object endpoints
  Given a capability endpoint that redirects, returns a non-success status, or
  streams bytes that violate the declared immutable identity
  When the Query-only SDK reads it
  Then it fails with a typed opaque error or integrity error, does not follow a
  redirect, and neither debug output nor error text contains the URL token or
  required header value

## Out of Scope

- Browser upload capabilities, CDN authorization/revocation, retries across an
  expired signed URL, and codec indexes.
- Signing arbitrary external URLs or changing Query authorization.
- Retrofitting generic HTTP reads into the credentialed managed-stage API.
