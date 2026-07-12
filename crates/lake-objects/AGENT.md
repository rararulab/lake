# lake-objects

Managed large-object values and direct storage access for the Rust SDK.

## Invariants

- A `DataLocation` identifies one immutable, fully uploaded object.
- Object bytes move directly between the SDK and the managed stage; servers only
  receive table metadata and RecordBatch streams.
- Arrow conversion is the only bridge between object values and Lance tables.
- Storage backends must stream files in bounded chunks; never buffer whole
  videos or models in memory.
- Full SDK reads validate the expected SHA-256 before storage I/O and verify
  exact size plus SHA-256 only at EOF, using constant memory. An early drop is
  not a successful verification.
- Byte ranges are non-empty half-open intervals checked against immutable
  `DataLocation.size_bytes` before local or S3 I/O.
- GC never scans table rows, never trusts draft output, and never deletes
  without a fully verified immutable plan plus page checkpoint.

## Layout

- `lib.rs` — public object value, Arrow conversion, and storage interfaces.
- `integrity.rs` — bounded full-read size/SHA-256 verification and typed errors.
- `local.rs` — local-development managed-object implementation.
- `s3.rs` — production managed-prefix validation, multipart upload/abort, and
  direct S3 reads, bounded GET presigning, inventory, and deletion.
- `reference_index.rs` — bounded external merge of retained reference deltas.
- `inventory.rs`, `gc.rs` — bounded inventory and age-gated merge planning.
- `gc_plan.rs`, `gc_apply.rs` — immutable content-addressed plans and resumable
  application.
- `tests/s3_localstack.rs` — ignored real-protocol upload, read, inventory, and
  GC-resume tests.
