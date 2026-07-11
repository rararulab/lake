# lake-objects

Managed large-object values and direct storage access for the Rust SDK.

## Invariants

- A `DataLocation` identifies one immutable, fully uploaded object.
- Object bytes move directly between the SDK and the managed stage; servers only
  receive table metadata and RecordBatch streams.
- Arrow conversion is the only bridge between object values and Lance tables.
- Storage backends must stream files in bounded chunks; never buffer whole
  videos or models in memory.

## Layout

- `lib.rs` — public object value, Arrow conversion, and storage interfaces.
- `local.rs` — local-development managed-object implementation.
- `s3.rs` — production managed-prefix validation, multipart upload/abort, and
  direct S3 reads.
- `tests/s3_localstack.rs` — ignored real-protocol multipart and failure tests.
