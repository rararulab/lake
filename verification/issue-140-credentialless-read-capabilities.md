# Verification: credentialless managed read capabilities

Issue: #140
Base: `990146066bbcca06640489cd995569887ec7636c`

## Delivered contract

- `LakeClient::connect_query_only` establishes only the authenticated Query
  Flight connection. It does not discover a stage, construct an object-store
  client, require cloud credentials, or permit accidental direct object I/O.
- `LakeClient::presign_read_via_query` sends one bounded
  `lake.managed_read_capability.v1` action and decodes one opaque
  `PresignedRead`. Existing `presign_read` retains its direct SDK-IAM and zero
  RPC behavior.
- Query authenticates before dispatch, derives the caller's exact
  `tenants/<tenant-id>` stage, validates the requested identity before issuing,
  and returns only safe Flight status messages on malformed, foreign, or
  unconfigured requests.
- `lake query` installs `S3ReadCapabilityIssuer` only for an S3 stage. The
  issuer derives a tightly scoped `S3ObjectStore`, rejects query-bearing S3
  identities before signing, and uses the Query process's AWS client without
  exposing AWS credentials to the SDK.
- Capability request/response wires are versioned, JSON `deny_unknown_fields`,
  and capped at 16 KiB. URL and header values are redacted by `Debug` and are
  not written to Arrow, metadata, or checkpoints.

## Test evidence

- `cargo test -p lake-objects managed_read_capability_ -- --nocapture`:
  PASS, 2/2 bounded wire and redaction tests.
- `cargo test -p lake-objects
  s3_read_capability_issuer_rejects_unsafe_identity_before_signing --
  --nocapture`: PASS.
- `cargo test -p lake-query managed_read_capability_action_ -- --nocapture`:
  PASS, 2/2 tenant scoping and fail-closed tests. The denial selector covers
  malformed wire, zero expiry, foreign tenant identity, and absent issuer
  without invoking the issuer.
- `cargo test -p lake-cli query_server_capability_issuer_is_s3_only --
  --nocapture`: PASS.
- `cargo test -p lake-sdk
  sdk_remote_read_capability_uses_query_action_without_stage_store --
  --nocapture`: PASS. It verifies the Query-only client sends exactly one
  action, does no discovery, redacts the returned bearer values, and rejects
  local object reads.
- `mise run spec-lifecycle
  specs/issue-140-credentialless-read-capabilities.spec.md`: PASS, 4/4
  scenarios and every selector executed at least one test.
- `mise run gate`: PASS. Hooks, workspace all-target Rust tests, upstream ADBC
  interoperability (3/3), local E2E selftest, and site typecheck/test/build
  all completed successfully.

## Notes

The macOS linker emitted its pre-existing compact-unwind-size warning during
some Rust links. It is a warning only; no test, hook, E2E, or gate step failed.
