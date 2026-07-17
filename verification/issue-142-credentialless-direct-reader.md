# Issue #142 verification: credentialless direct managed reader

## Candidate

- Base: `1fc515896c03c19fd42940fc9db373e695d6785c` (main after #150).
- Scope: Query-only Rust SDK direct full and range reads; no Query, Metasrv,
  CLI, or credential-discovery changes.

## Delivered behavior

- `LakeClient::open_via_query` validates the immutable `DataLocation`, asks
  Query for one bounded capability, streams the object directly through Rustls,
  and reuses the constant-memory size/SHA-256-at-EOF verifier.
- `LakeClient::open_range_via_query` validates the non-empty half-open range,
  sends its exact HTTP `Range`, and exposes bytes only after an exact `206`,
  `Content-Range`, and `Content-Length` response check. It intentionally makes
  no whole-object SHA-256 claim.
- The shared direct HTTP client disables redirects and all caller/system
  proxies. URL/header-bearing transport failures are opaque, issuer-provided
  `Range` headers are rejected, and an unfinished response is owned by (and
  dropped with) the returned reader.

## Red/green and selector evidence

- The full-reader selector initially failed because `open_via_query` did not
  exist. It now proves one Query action, required direct-HTTP header forwarding,
  streaming before the second body chunk is released, and EOF integrity
  verification.
- The range-reader selector initially failed because `open_range_via_query` did
  not exist. It now proves the exact `bytes=3-6` request and exact `206 bytes
  3-6/10` response. A companion test rejects a mismatched `Content-Range`.
- The fail-closed selector proves a `307` is not followed, signed URL and header
  test tokens are absent from both display and debug output, and a same-length
  corrupt body fails its EOF SHA-256 check.
- The proxy regression first failed because the direct-read client had no
  security-specific builder. It now injects a proxy into that builder and
  observes one object request, zero proxy requests, and the required signed
  header only at the object endpoint.
- `mise run spec-lint specs/issue-142-credentialless-direct-reader.spec.md`:
  PASS, quality 100%.
- `mise run spec-lifecycle specs/issue-142-credentialless-direct-reader.spec.md`:
  PASS, 3/3 scenarios with each selector executing at least one test.

## Quality gate

- `mise run fmt-check`: PASS.
- `cargo nextest run -p lake-sdk`: PASS, 58 passed and 2 LocalStack-only
  tests skipped as designed.
- `mise run gate`: PASS: hooks, workspace all-target tests, CLI selftest, the
  three ADBC interoperability tests, and site typecheck/test/build.
- `mise run test-integration`: PASS against checkout-scoped LocalStack: all 20
  ignored S3/DynamoDB integration tests passed. Nextest reported one existing
  leaky test process (`interrupted_s3_upload_is_aborted`), but no test failed
  and the LocalStack container was removed successfully.
- `git diff --check`: PASS.

The only observed diagnostics were existing macOS linker compact-unwind
performance warnings; they did not affect compilation or test outcomes.
