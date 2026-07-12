spec: task
name: "encrypted-statement-tickets"
inherits: project
tags: [query, flight-sql, security, tickets, aead, tenancy, rotation]
---

## Intent

Raw SQL in a replayable Flight statement handle is not a production
capability. Preserve standard Flight SQL framing while making the self-contained
handle confidential, authenticated, expiring, and bound to the identity that
requested it, without adding Query-local durable state.

## Decisions

- Use a versioned AES-256-GCM envelope with a random 64-bit per-process nonce
  prefix, monotonic 32-bit suffix, and 128-bit derived key id. The fixed header
  and audience are authenticated as AAD; counter exhaustion fails closed.
- Encrypt issue/expiry time, exact principal id, exact tenant id, and SQL.
- Extract the authenticated principal and bound ciphertext size before opening;
  validate all claims and SQL size before authorization, admission, catalog
  refresh, planning, or execution.
- One active key seals and at most three verification keys open old tickets.
  Key material is derived with domain-separated SHA-256 and never formatted in
  Debug/errors/logs.
- Remote Query listeners require an explicit protected shared key-ring file.
  Loopback development alone may generate an ephemeral key.
- Ticket TTL defaults to five minutes and is bounded to one second through one
  hour. Verifiers honor an unexpired ticket's sealed lifetime up to the hard
  protocol maximum even if the local sealing TTL changes.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-query/**
crates/lake-cli/**
deploy/kubernetes/lake.yaml
README.md
docs/architecture.md
docs/design/sql-api-over-s3.md
docs/guides/kubernetes.md
docs/plans/2026-07-12-encrypted-statement-tickets.md
specs/issue-106-encrypted-statement-tickets.spec.md
verification/issue-106-encrypted-statement-tickets.md

### Forbidden
raw SQL as statement-ticket plaintext
credentials or storage locations in ticket payloads
unbounded key rings, ticket lifetimes, or ciphertext allocation
metadata calls before ticket/tenant validation
Query-local durable ticket state
changing the outer Flight SQL ticket type
claiming exact table snapshot pinning in this issue

## Completion Criteria

Scenario: Statement ticket is confidential and identity bound
  Test:
    Package: lake-query
    Filter: statement_ticket_is_confidential_and_identity_bound
  Given SQL and an authenticated principal and tenant
  When Query seals and opens a statement ticket
  Then ciphertext contains none of those plaintext values and only that identity can open it

Scenario: Invalid ticket claims fail closed
  Test:
    Package: lake-query
    Filter: statement_ticket_rejects_tamper_time_audience_and_unknown_key
  Given tampered, expired, future-issued, wrong-audience, or unknown-key tickets
  When Query opens them
  Then every variant returns the same invalid-ticket class

Scenario: Key rotation is bounded and preserves configured old tickets
  Test:
    Package: lake-query
    Filter: statement_ticket_rotation_preserves_only_configured_old_keys
  Given an old active key and a new active key with the old verifier
  When tickets cross the rollout boundary
  Then configured old tickets open and new tickets remain unavailable to old-only replicas

Scenario: TTL changes do not revoke unexpired capabilities
  Test:
    Package: lake-query
    Filter: verifier_ttl_change_does_not_revoke_unexpired_ticket
  Given a ticket sealed with a longer valid TTL
  When a verifier lowers its local sealing TTL
  Then the ticket remains valid until its sealed expiry within the protocol maximum

Scenario: Nonce exhaustion fails closed
  Test:
    Package: lake-query
    Filter: statement_ticket_nonce_counter_exhaustion_fails_closed
  Given one Query process has exhausted its active-key nonce counter
  When it attempts to seal another statement ticket
  Then issuance fails instead of reusing an AES-GCM nonce

Scenario: Cross-principal replay stops before planning
  Test:
    Package: lake-query
    Filter: statement_ticket_replay_is_rejected_before_planning
  Given Alice's valid encrypted handle
  When Bob submits it to DoGet
  Then Query returns uniform unauthenticated status before admission or metastore planning

Scenario: Remote listeners require shared ticket keys
  Test:
    Package: lake-query
    Filter: remote_query_requires_shared_ticket_keys_before_startup
  Given loopback and remote Query listeners
  When no shared ring or an invalid TTL is configured
  Then only loopback may use ephemeral keys and remote startup fails closed

Scenario: CLI key files are protected bounded and redacted
  Test:
    Package: lake-cli
    Filter: query_ticket_key_ring_requires_protected_bounded_unique_secrets
  Given valid, duplicate, malformed, and overly permissive key files
  When CLI loads Query security
  Then only protected bounded unique configuration succeeds without secret disclosure

Scenario: TLS replay isolation works over real Flight SQL
  Test:
    Package: lake-query
    Filter: tls_statement_ticket_rejects_cross_principal_replay
  Given TLS Query with Alice and Bob credentials
  When Alice executes GetFlightInfo and Bob replays its standard outer ticket
  Then Bob is rejected and Alice can execute the same encrypted capability

Scenario: Kubernetes replicas share ticket rotation configuration
  Test:
    Package: lake-cli
    Filter: kubernetes_reference_is_secure_and_matches_runtime_contract
  Given the production reference Deployment and runbook
  When Query scales and rotates keys
  Then every replica mounts the protected ring with bounded TTL and documented staged rotation

## Out of Scope

- Exact table-version snapshot pinning across GetFlightInfo and DoGet.
- Async result/PollFlightInfo capability envelopes.
- Online file watching; rotation uses bounded Kubernetes rollouts.
- Per-tenant query quotas or distributed admission.
