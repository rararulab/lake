# Verification — issue #150 S3 multipart scale

Date: 2026-07-14

## TDD boundary

Before the implementation, the required focused test failed to compile because
`checked_multipart_part_number` and
`ObjectError::S3MultipartPartLimit` did not exist. After the implementation:

```text
cargo test -p lake-objects multipart_part_number_limit_rejects_10001st_part
test result: ok. 1 passed; 0 failed; 0 ignored
```

The small-chunk pipeline regression also passed: it uploads part 10,000, then
observes one more non-empty one-byte chunk. That 10,001st chunk returns the
typed error before the uploader is called, so the test covers the real
cross-boundary pipeline sequence without a large fixture.

```text
cargo test -p lake-objects multipart_pipeline_accepts_10000th_part_and_rejects_10001st_before_upload
test result: ok. 1 passed; 0 failed; 0 ignored
```

## V1 checkpoint compatibility

The two focused regressions were red before the migration: a valid 5 MiB V1
checkpoint was rejected against the new 64 MiB default, while a one-byte
checkpoint size was accepted. After the change, only the explicit legacy 5
MiB and current 64 MiB sizes validate; restore uses the checkpoint's accepted
size to rehash completed parts, read the first remaining part, and configure
the rest of the pipeline.

```text
cargo test -p lake-objects resumable_checkpoint_
test result: ok. 3 passed; 0 failed; 0 ignored
```

The LocalStack integration regression
`resumable_s3_upload_finishes_legacy_5m_checkpoint_without_reupload_localstack`
seeds a real remote 5 MiB part plus its V1 checkpoint, then completes it with
a store whose default is 64 MiB. The source ends at the seeded part, so a
successful completion proves recovery rehashed and completed that persisted
part without admitting another upload.

The separate production-helper regression covers the previously unobserved
remaining pipeline. It supplies an accepted legacy checkpoint with 5 MiB plus
one byte remaining and asserts the uploader receives exactly `[5 MiB, 1 B]`.
This would fail if either resumed first-read or pipeline refill reverted to the
64 MiB default.

```text
cargo test -p lake-objects resumable_pipeline_keeps_legacy_checkpoint_part_size_for_remaining_input
test result: ok. 1 passed; 0 failed; 0 ignored
```

## Required checks

```text
mise run fmt
exit 0

cargo test -p lake-objects
unit tests: 32 passed; 0 failed
s3_localstack wiring tests: 14 passed; 15 ignored

mise run spec-lifecycle specs/issue-150-s3-multipart-scale.spec.md
[PASS] The 10,001st multipart part is rejected before S3 I/O
[PASS] A legacy V1 checkpoint resumes without repartitioning its source
[PASS] A legacy V1 checkpoint partitions its remaining pipeline at 5 MiB
spec-lifecycle-guard: OK — every Test selector executed >=1 test

mise run test-integration
Summary: 20 tests run: 20 passed, 170 skipped

mise run gate
exit 0
```

The LocalStack run includes multipart round-trip, interrupted and cancelled
abort cleanup, resumable checkpoint reuse/recovery (including the legacy 5
MiB checkpoint), range reads, and SDK S3 stage tests using the production 64
MiB part size for new uploads.
