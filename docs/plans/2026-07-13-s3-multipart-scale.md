# S3 multipart scale implementation plan

## Goal

Raise the bounded default multipart part size used by `S3ObjectStore` and
reject an input stream before it can issue S3 part number 10,001. The result
must preserve one-pass direct streaming, incremental SHA-256, and the existing
abort-on-failure path.

## Baseline before #150

- `crates/lake-objects/src/s3.rs` owns `S3ObjectStore`, reads fixed
  `MULTIPART_PART_BYTES` chunks in source order, and admits a finite window of
  `UploadPart` requests with `part_number: i32`.
- The prior `checked_add` protected only integer overflow, whereas S3 accepts
  part numbers only from 1 through 10,000. A 5 MiB part size therefore reaches
  an invalid S3 request after roughly 48.8 GiB.
- `ObjectError` in `crates/lake-objects/src/lib.rs` is the crate's public
  Snafu error surface. Preserve its redaction and typed-error conventions.
- Existing `crates/lake-objects/tests/s3_localstack.rs` owns real S3 protocol
  coverage. Do not require a 625 GiB test fixture; use a unit-level numeric
  boundary test for this change.

## Scope

Modify only:

- `crates/lake-objects/src/lib.rs`
- `crates/lake-objects/src/checkpoint.rs`
- `crates/lake-objects/src/s3.rs`
- `crates/lake-objects/tests/s3_localstack.rs`
- `crates/lake-objects/AGENT.md`
- `README.md`
- `docs/design/managed-objects.md`
- `specs/issue-150-s3-multipart-scale.spec.md`
- `verification/issue-150-s3-multipart-scale.md`

Do not change the managed object trait, `DataLocation`, Query, Metasrv, SDK
API, S3 credentials, or object persistence layout. Do not pre-read the source
or retain more than the existing finite part-request window plus small read
buffer.

## Steps

1. Add a unit test in `s3.rs` that proves part 10,000 is valid and asking for
   a successor returns a typed `ObjectError` before any request construction.
   Run `cargo test -p lake-objects multipart_part_number_limit_rejects_10001st_part`;
   it must initially fail because no boundary helper/error exists.
2. Define the S3-specific typed error in `ObjectError`. In `s3.rs`, replace the
   fixed 5 MiB constant with a documented 64 MiB default and centralize the
   `1..=10_000` check. Apply that check only after reading another non-empty
   part and before constructing its upload future, so exactly 10,000 parts
   still complete successfully. Reuse the existing `upload_nonempty` error
   path so it aborts the multipart upload; the non-recoverable resumable path
   must abort and remove its checkpoint too.
3. Update reader allocation to use the new bounded part size, leaving the
   64 KiB read buffer and existing source-order bounded request window
   unchanged.
4. Preserve V1 checkpoint compatibility by accepting only the former 5 MiB
   and new 64 MiB persisted sizes. On recovery, use the checkpoint's accepted
   size to rehash completed parts and partition the remaining pipeline; new
   checkpoints continue to record 64 MiB. Add unit coverage for legacy
   acceptance and unknown-size rejection, plus a production-helper regression
   that drives remaining `5 MiB + 1 B` input as `[5 MiB, 1 B]`. Keep the
   LocalStack recovery case with one completed 5 MiB part and no remaining
   part to upload.
5. Update `lake-objects/AGENT.md`, README, and managed-object design text to
   state the 64 MiB bound, the approximate default part-count capacity, and
   the deliberately typed ceiling for unknown-length streams and the V1
   checkpoint compatibility boundary.
6. Run `mise run fmt`, the bound test, `cargo test -p lake-objects`,
   `mise run spec-lifecycle specs/issue-150-s3-multipart-scale.spec.md`, and
   `mise run gate`. Record exact outcomes in the verification document.

## Done criteria

- The 10,001st-part boundary has a unit test bound by the task spec.
- The code never hands S3 a part number outside 1..=10,000.
- A stream with exactly 10,000 parts completes the numeric boundary path.
- A valid 5 MiB V1 checkpoint resumes with its original partitioning, while an
  unrecognized persisted part size is rejected before upload work begins; the
  remaining pipeline emits `[5 MiB, 1 B]`, not one 64 MiB-default part.
- Object bytes remain direct SDK-to-S3 streaming with bounded memory.
- Existing LocalStack multipart/abort tests remain enabled and pass when the
  integration environment is present.

## Stop conditions

- Stop if the AWS Rust SDK rejects a 64 MiB `ByteStream` or an existing S3
  contract requires the 5 MiB value exactly.
- Stop if enforcing the boundary requires changing Query, Metasrv, the SDK
  public API, or a persisted `DataLocation` representation.
- Stop if an exact 10,000-part completion needs a multi-gigabyte fixture;
  test the pure numeric boundary instead and report the limitation.
