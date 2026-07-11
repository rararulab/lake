# Verification: issue #14 Flight transport security

## RED evidence

- `cargo test -p lake-flight bearer_authenticator_rejects_missing_and_wrong_credentials`
  failed because bearer authentication, client security, server security, and
  authenticated principals did not exist.
- `cargo test -p lake-query secured_query_rejects_anonymous_discovery` failed
  because Query had no security-aware configuration/server entry point.
- The first real Query TLS run exposed a rustls provider-selection panic when
  the dependency graph enabled both ring and aws-lc. `lake-flight` now installs
  ring explicitly at every TLS entry point.
- `cargo test -p lake-metasrv secured_follower_forwards_with_peer_identity`
  failed because follower forwarding hard-coded anonymous `http://` and
  Metasrv had no secure server/peer configuration.
- `cargo test -p lake-sdk sdk_tls_bearer_roundtrip_reaches_secured_query_and_meta`
  failed because the SDK had no authenticated TLS builder.
- `cargo test -p lake-cli security_files_require_complete_tls_and_redact_credentials`
  failed because process security file loading did not exist.

## Focused GREEN evidence

- `cargo test -p lake-flight`: PASS, bearer constant-time comparison,
  credential redaction, TLS/client authorization configuration, and
  non-loopback fail-closed policy.
- `cargo test -p lake-query`: PASS, including real self-signed TLS where
  anonymous stage discovery receives `Unauthenticated` and valid bearer
  discovery returns the credential-free descriptor.
- `cargo test -p lake-metasrv`: PASS, including two TLS Metasrv nodes where a
  follower forwards a write to the leader using its configured peer identity.
- `cargo test -p lake-sdk`: PASS, 12 passed and 2 LocalStack tests ignored
  outside the integration gate. The new full-chain test uses distinct Query
  and Metasrv bearers over TLS, then inserts, queries, and directly opens FILE.
- `cargo test -p lake-cli`: PASS, including complete cert/key pairing and
  secret-file redaction behavior.
- Strict clippy passed incrementally for all five touched crates.

## Final gates

- `mise run spec-lint specs/issue-14-flight-security.spec.md`: PASS, quality
  100%.
- `mise run spec-lifecycle specs/issue-14-flight-security.spec.md`: PASS, all
  six scenarios, explicit boundaries, and zero-match guard.
- `cargo clippy -p lake-flight -p lake-query -p lake-metasrv -p lake-sdk
  -p lake-cli --all-targets -- -D warnings`: PASS.
- `mise run test-integration`: PASS, 7/7 real LocalStack S3/DynamoDB tests.
- `mise run gate`: PASS in 19.62s, including repository hooks, all workspace
  targets/tests, local self-check, and site checks/build.

The macOS linker emitted its existing debug-binary `__eh_frame section too
large` warning. It did not affect strict linting, linking, or any test.
