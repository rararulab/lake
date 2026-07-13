spec: task
name: "no-proxy-direct-reader"
inherits: project
tags: [sdk, managed-objects, direct-read, http, capability, security]
---

## Intent

Keep Query-issued managed-object read capabilities private to the SDK and the
object-storage endpoint. Credentialless direct readers intentionally connect to
the capability URL without routing object bytes through Query or Metasrv, but
the shared reqwest client currently accepts system proxy configuration. A
process proxy can therefore receive a signed URL or required signed headers.
The direct reader must bypass every proxy while preserving direct streaming,
drop cancellation, range semantics, and redacted failures.

## Decisions

- Build the shared credentialless direct-read HTTP client with reqwest proxy
  use disabled and redirects disabled. This applies equally to full and range
  capability requests; it does not change Flight RPC proxy policy or an
  exported `PresignedRead` capability.
- Keep the client process-scoped and lazily initialized, with no new public
  SDK configuration or object-sized buffering. Reader ownership remains with
  the caller, so dropping a reader still drops the HTTP body without a
  background drain.
- Prove proxy bypass with a caller-supplied reqwest builder that explicitly
  proxies all requests to a local recording proxy. The test must not mutate
  process-wide proxy environment variables. The object endpoint must receive
  the required header exactly once while the proxy receives no request.
- Preserve existing redirect denial, exact-range validation, capability
  redaction, Query-only authorization, and typed opaque direct-read errors.

## Boundaries

### Allowed Changes
crates/lake-sdk/src/lib.rs
specs/issue-149-no-proxy-direct-reader.spec.md
verification/issue-149-no-proxy-direct-reader.md

### Forbidden
Cargo.toml
Cargo.lock
crates/lake-objects/**
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-meta/**
crates/lake-common/**
docs/architecture.md
README.md
new public SDK configuration or proxy options
process-wide proxy environment mutation
proxying object bytes through Query or Metasrv
background body draining or object-sized buffering
changing Flight RPC proxy policy or `PresignedRead` wire format

## Acceptance Criteria

Rule: credentialless-direct-read-bypasses-proxies — a managed read capability
only reaches its object endpoint

Scenario: A configured proxy is bypassed for a direct capability request
  Test:
    Package: lake-sdk
    Filter: direct_read_client_bypasses_configured_proxy
    Level: unit
    Test Double: local object and recording proxy HTTP servers with an
      explicitly proxied reqwest builder
  Given a valid direct-read capability and a reqwest builder configured to
  proxy every request to a local recorder
  When the SDK constructs and uses its direct-read client for that capability
  Then the object endpoint receives one request with the required signed
  header, the proxy receives no request, and the response streams normally

Scenario: Query-only full reads remain direct and integrity-verifying
  Test:
    Package: lake-sdk
    Filter: query_only_full_read_streams_and_verifies_without_stage_access
    Level: integration
    Test Double: real loopback Query Flight capability service and streaming
      object HTTP response
  Given a Query-only SDK client and a signed object capability
  When the caller drains the full direct reader
  Then only one capability action reaches Query and the stream verifies its
  immutable identity at EOF

Scenario: Query-only range reads keep exact direct semantics
  Test:
    Package: lake-sdk
    Filter: query_only_range_reader_requires_exact_partial_response
    Level: integration
    Test Double: real loopback Query Flight capability service and range HTTP
      response
  Given a Query-only SDK client and a valid half-open range
  When the SDK opens the range through a capability
  Then the object endpoint observes one exact Range GET and Query receives no
  object bytes

Scenario: Capability failures remain fail-closed and redacted
  Test:
    Package: lake-sdk
    Filter: query_only_reader_fails_closed_and_redacts_capability
    Level: integration
    Test Double: real loopback Query Flight capability service with redirect
      or corrupt object HTTP responses
  Given a redirecting or corrupt capability endpoint with signed URL and
  header values
  When the SDK opens the reader
  Then it rejects the redirect or EOF integrity failure without exposing either
  secret or retrying the request through Query

## Out of Scope

- Proxy behavior for Flight SQL or any caller-owned HTTP client.
- New capability formats, signed uploads, stage discovery changes, cloud
  credential discovery, or object-store implementation changes.
- HTTP retries, caching, retries through Query, transport telemetry, or
  support for proxy allowlists.
