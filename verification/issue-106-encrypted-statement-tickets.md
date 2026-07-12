# Verification: Encrypted statement tickets

## Required evidence

- A standard Flight SQL statement ticket contains no raw SQL or identity text.
- Tamper, expiry, future issue time, audience/key/identity mismatch fail before
  authorization, admission, catalog refresh, planning, or execution.
- Stateless replicas can perform bounded staged key rotation without exposing
  secrets or invalidating unexpired configured old tickets.
- Remote Query refuses startup without shared keys; CLI and Kubernetes load a
  protected common key ring and bounded TTL.
- Full workspace, e2e, lint, docs, and deployment tests pass.

## RED/GREEN evidence

- The first codec test failed to compile because `QueryTicketKeyRing`,
  `StatementTicketCodec`, and `QueryTicketError` did not exist. It now proves
  AES-GCM ciphertext contains none of SQL, principal id, or tenant id and opens
  only for the exact identity.
- The RPC replay test failed because `FlightSqlServiceImpl` had no ticket codec.
  It now proves Bob receives uniform `Unauthenticated` for Alice's ticket while
  the metastore planning counter remains zero; Alice executes successfully.
- The protected-file test failed because no CLI key loader existed. It now
  covers mode `0600`, unknown fields, duplicate/short/over-limit keys, bounded
  file size, and redacted Debug/errors.
- The remote-listener test was run after deliberately removing the provisional
  fail-closed branch and failed because the listener resolver did not exist. It
  now proves only loopback may generate ephemeral keys and invalid TTL/remote
  missing keys fail before catalog warm or bind.
- The Kubernetes test failed because Query had no shared key-ring or TTL env.
  It now validates the protected mounted file, five-minute TTL, Secret command,
  and preload/activate/retire runbook.
- The verifier-TTL regression failed because a lower local sealing TTL revoked
  an otherwise unexpired old ticket. Verification now enforces the one-hour
  protocol maximum rather than the current sealer TTL.
- The nonce-exhaustion test failed because no nonce counter existed. Issuance
  now uses a random 64-bit process prefix plus shared atomic 32-bit suffix and
  fails closed before reuse.

## GREEN evidence

- All 42 `lake-query` unit/TLS tests passed, including a real TLS Flight SQL
  Alice→Bob replay rejection and same-principal execution.
- All 29 CLI unit tests, the Kubernetes contract test, and four logging tests
  passed.
- All ten lane-1 scenarios passed with non-zero selector matches.
- `cargo clippy --workspace --all-targets -- -D warnings` passed.
- `cargo doc --workspace --no-deps` passed.

## Review result

- Review replaced per-request AES key scheduling with immutable pre-expanded
  keys and widened derived key ids to 128 bits.
- Review replaced probabilistic per-ticket nonces with a random per-process
  prefix plus monotonic shared counter, including exhaustion rejection.
- Principal/tenant comparison uses `subtle::ConstantTimeEq`; invalid ticket
  telemetry has one fixed label and external errors do not distinguish claim,
  key, AEAD, time, or identity failures.
- Ciphertext allocation is capped at configured SQL bytes plus 512 bytes before
  decryption; key count, secret size, file size, TTL, and nonce lifetime are all
  finite.
- No remaining P0-P2 correctness, security, performance, or removal findings.
- Exact table snapshot pinning remains explicitly out of scope and is the next
  protocol task. First adoption from raw-ticket binaries requires blue/green or
  drained cutover; insecure legacy acceptance was intentionally not added.

## Final gate

- `mise run gate` passed on the reviewed production tree in 34.35 seconds with
  exit code 0: workspace tests, e2e self-test, hooks, and site install/check all
  completed with zero failures.
