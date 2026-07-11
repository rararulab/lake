spec: task
name: "flight-transport-security"
inherits: project
tags: [security, flight, tls, auth, sdk, query, metasrv]
---

## Intent

Secure every Lake Flight hop with authenticated RPCs and configurable TLS,
while preserving an explicit loopback-only development path. The same client
security abstraction must cover SDK to Query, Query to Metasrv, and Metasrv
follower to leader so internal forwarding cannot silently downgrade.

## Decisions

- A new `lake-flight` crate owns tonic-specific client/server security. Shared
  domain types remain free of transport dependencies.
- The first authenticator accepts one opaque bearer credential and produces an
  authenticated service principal. The boundary remains replaceable by a
  tenant-aware provider later.
- A server interceptor authenticates every Flight RPC. Handshake is not a
  privileged bypass.
- Client security owns endpoint TLS configuration and bearer injection for
  both `FlightClient` and `FlightSqlServiceClient`.
- Non-loopback servers reject missing TLS or authentication unless deployment
  explicitly opts into insecure serving. Loopback tests/examples use the
  explicit insecure configuration.
- Credentials are process configuration only: they are redacted from Debug,
  absent from errors and descriptors, and never stored in SQL rows.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-flight/**`
- `crates/lake-query/**`
- `crates/lake-metasrv/**`
- `crates/lake-sdk/**`
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
does not edit root CI, mise configuration, or those workflow guides.

### Forbidden
- `crates/lake-common/**`
- Logging, serializing, or returning bearer credential values
- Authenticating only Handshake while leaving other Flight RPCs anonymous
- Hard-coded HTTP for Query-to-Metasrv or follower-to-leader connections
- Treating bearer authentication alone as tenant authorization
- Silently allowing plaintext anonymous serving on non-loopback addresses

## Completion Criteria

Scenario: bearer authentication fails closed and secrets stay redacted
  Test:
    Package: lake-flight
    Filter: bearer_authenticator_rejects_missing_and_wrong_credentials
  Given a Flight server configured with an opaque bearer credential
  When requests omit the credential, send a wrong value, or send the valid one
  Then only the valid request receives an authenticated principal and no
  diagnostic representation contains the credential

Scenario: client security applies TLS and authorization consistently
  Test:
    Package: lake-flight
    Filter: client_security_configures_tls_and_authorization
  Given a client CA, server-name override, and bearer credential
  When raw Flight and Flight SQL clients are constructed
  Then both clients use the same verified TLS channel and authorization value

Scenario: secured Query rejects anonymous managed-stage discovery
  Test:
    Package: lake-query
    Filter: secured_query_rejects_anonymous_discovery
  Given Query configured with server TLS and bearer authentication
  When anonymous and authenticated clients invoke stage discovery
  Then anonymous access is unauthenticated and authenticated discovery returns
  the configured credential-free descriptor

Scenario: SDK reaches secured Query and Metasrv with query-only connection
  Test:
    Package: lake-sdk
    Filter: sdk_tls_bearer_roundtrip_reaches_secured_query_and_meta
  Given self-signed TLS services with distinct Query and Metasrv credentials
  When the SDK builder connects to Query and inserts then queries a FILE
  Then SDK-to-Query and Query-to-Metasrv authentication succeed and object
  bytes still move directly through the discovered managed stage

Scenario: secured Metasrv follower forwards with peer identity
  Test:
    Package: lake-metasrv
    Filter: secured_follower_forwards_with_peer_identity
  Given a follower and leader protected by TLS and bearer authentication
  When a write lands on the follower
  Then forwarding uses the configured secure channel and service identity
  rather than anonymous plaintext HTTP

Scenario: non-loopback serving requires explicit production security
  Test:
    Package: lake-flight
    Filter: non_loopback_server_security_fails_closed
  Given a non-loopback listen address
  When TLS or authentication is absent
  Then server configuration fails unless the caller explicitly allows
  insecure serving

## Out of Scope

- Tenant catalog/object policy and row-level authorization
- JWT/OIDC validation and key rotation
- Per-tenant quotas and query cancellation
- Browser sessions or presigned URLs
- Certificate issuance and rotation automation
