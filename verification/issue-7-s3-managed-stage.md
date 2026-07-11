# Verification: issue #7 S3 managed stage

## RED evidence

- `cargo test -p lake-objects s3_reader_rejects_locations_outside_managed_prefix`
  failed because `S3ObjectStore`, S3 URI errors, and AWS dependencies did not
  exist.
- `cargo test -p lake-objects --test s3_localstack --no-run` failed because
  `S3ObjectStore` had no `put_reader` implementation.
- `cargo test -p lake-sdk sdk_file_insert_uses_s3_stage --no-run` failed because
  the SDK fixture had no S3 dependencies or generic managed-store setup.
- The first LocalStack upload exposed an AWS SDK/LocalStack checksum mismatch:
  `UploadPart` sent CRC32 while multipart creation declared no checksum. The
  implementation now declares CRC32 and completes each part with the returned
  checksum.

## GREEN evidence

- `mise run test-integration`: PASS, 5/5 ignored infrastructure tests:
  multipart round trip, interrupted-upload abort, SDK SQL FILE over S3,
  DynamoDB metadata, and Lance-on-S3.
- `cargo test -p lake-objects`: PASS, 6 tests passed; 2 infrastructure tests
  intentionally ignored outside LocalStack.
- `cargo test -p lake-sdk`: PASS, 9 tests total: 8 passed and the LocalStack
  SQL/S3 test intentionally ignored outside the integration gate.
- `cargo clippy -p lake-objects -p lake-sdk --all-targets -- -D warnings`:
  PASS.
- `mise run spec-lint specs/issue-7-s3-managed-stage.spec.md`: PASS, quality
  100%.
- `mise run spec-lifecycle specs/issue-7-s3-managed-stage.spec.md`: PASS, all
  five scenarios and the zero-match guard.
- `mise run gate`: PASS in 51.54s, including workspace tests, local e2e,
  repository hooks, and site checks/build.

The integration tests use a real LocalStack S3 protocol endpoint. Their
non-ignored companion tests verify that both packages remain wired into
`scripts/test-integration.ts`; the actual ignored tests are the behavior
evidence and were executed successfully above.
