# Verification: issue #11 managed-stage discovery

## RED evidence

- `cargo test -p lake-common managed_stage_descriptors_roundtrip_without_credentials`
  failed because the shared descriptor and discovery action did not exist.
- `cargo test -p lake-query managed_stage_action_returns_configured_descriptor`
  failed because the Flight service had no configured stage or custom action.
- `cargo test -p lake-sdk client_discovers_local_stage_from_query` failed at
  compile time because `LakeClient::connect` still required an injected store.
- `cargo test -p lake-cli managed_stage_descriptor` failed because the local
  and cloud process configuration did not expose managed-stage descriptors.
- The managed-file example contract test failed while the example still used
  `connect_with_store`; it passed after the example became query-only.

## Focused GREEN evidence

- The common descriptor round-trips local and S3 topology, rejects unsupported
  protocol versions, and its serialized form contains no credential, signed
  URL, or object-payload field.
- Query returns exactly one immutable descriptor through
  `lake.managed_stage.v1` and advertises the custom Flight action.
- The SDK connects with only a query endpoint, then completes local SQL FILE
  insert, query, full open, and range open through the discovered stage.
- `connect_with_store` remains covered as the explicit embedding/test seam.
- The ignored LocalStack test
  `sdk_discovers_s3_stage_and_streams_directly_localstack` passed directly: a
  multipart-sized object produced a stable `s3://` DataLocation and exact full
  and range reads while the SDK was given only the query endpoint.
- `cargo run -p lake-sdk --example managed_file`: PASS and returned a
  `file://.../managed-objects/...` DataLocation.
- `cargo test -p lake-cli -p lake-sdk`: PASS, 15 passed and 2 LocalStack tests
  intentionally ignored outside the integration gate.
- `cargo clippy -p lake-cli -p lake-sdk --all-targets -- -D warnings`: PASS.

## Final gates

- `cargo +nightly fmt --all -- --check`: PASS.
- `mise run spec-lint specs/issue-11-stage-discovery.spec.md`: PASS, quality
  100%.
- `mise run spec-lifecycle specs/issue-11-stage-discovery.spec.md`: PASS, all
  six scenarios, boundary verification, and zero-match guard.
- `mise run test-integration`: PASS, 7/7 real LocalStack S3/DynamoDB tests.
- `cargo clippy -p lake-common -p lake-query -p lake-sdk -p lake-cli
  --all-targets -- -D warnings`: PASS.
- `mise run gate`: PASS in 36.80s, including repository hooks, all workspace
  tests and targets, local create/ingest/query self-check, and site checks/build.
- After rebasing onto `3f5e1e3` (PR #12), `mise run spec-lifecycle` passed
  again and `mise run gate` passed again in 6.64s.

The macOS linker emitted its existing `__eh_frame section too large` warning
while building large debug test binaries. It did not affect compilation,
strict Rust linting, test execution, or the release-facing behavior verified
above.
