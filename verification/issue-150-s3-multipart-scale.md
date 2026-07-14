# Verification — issue #150 S3 multipart scale

Date: 2026-07-14

## TDD boundary

Before this implementation was replayed onto the current main baseline, its
three BDD selectors resolved to zero tests. After the implementation:

```text
cargo nextest run -p lake-objects -E 'test(multipart_part_number_limit_rejects_10001st_part) | test(resumable_checkpoint_accepts_legacy_part_size_when_default_grows) | test(resumable_pipeline_keeps_legacy_checkpoint_part_size_for_remaining_input)'
Summary: 3 tests run: 3 passed, 0 skipped
```

The small-chunk pipeline regression also passed: it uploads part 10,000, then
observes one more non-empty one-byte chunk. That 10,001st chunk returns the
typed error before the uploader is called, so the test covers the real
cross-boundary pipeline sequence without a large fixture.

## V1 checkpoint compatibility

Only the explicit legacy 5 MiB and current 64 MiB sizes validate; restore uses
the checkpoint's accepted size to rehash completed parts, read the first
remaining part, and configure the rest of the pipeline.

```text
cargo nextest run -p lake-objects
Summary: 51 tests run: 51 passed, 15 skipped (LocalStack-only)
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

## Required checks

```text
cargo clippy -p lake-objects --all-targets -- -D warnings
PASS

cargo +nightly fmt --all -- --check
PASS

mise run spec-lifecycle specs/issue-150-s3-multipart-scale.spec.md
[PASS] The 10,001st multipart part is rejected before S3 I/O
[PASS] A legacy V1 checkpoint resumes without repartitioning its source
[PASS] A legacy V1 checkpoint partitions its remaining pipeline at 5 MiB
spec-lifecycle-guard: OK — every Test selector executed >=1 test

mise run gate
PASS
```

The LocalStack run includes multipart round-trip, interrupted and cancelled
abort cleanup, resumable checkpoint reuse/recovery (including the legacy 5
MiB checkpoint), range reads, and SDK S3 stage tests using the production 64
MiB part size for new uploads.
