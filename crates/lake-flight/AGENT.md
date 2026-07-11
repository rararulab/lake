# lake-flight

Shared Arrow Flight transport security for every network hop.

## Invariants

- Every production Flight RPC is authenticated by a server interceptor;
  Handshake is not a privileged exception.
- Bearer values are opaque process credentials: never Debug, log, serialize,
  return in an error, or place in protocol payloads.
- TLS verification and authorization injection use one client configuration
  across SDK→Query, Query→Metasrv, and Metasrv follower→leader.
- Plaintext anonymous serving is loopback-only unless deployment makes an
  explicit insecure override.

## Layout

- `lib.rs` — bearer authentication, TLS client/server configuration, exposure
  policy, and Arrow Flight client credential injection
