# lake-sdk

The Rust SDK's typed write and direct-read surface.

## Invariants

- Object bytes go straight between this SDK and object storage, never through
  query or metadata Flight services.
- SQL is a narrow parameterized `FILE` INSERT binding, not a second SQL engine.
- Schema and parameter validation complete before the SDK begins an upload.
- A `DataLocation` row is appended only after every referenced object upload
  succeeds; per-table visibility remains owned by `Metasrv::append`.

## Layout

- `lib.rs` — public client, parameter values, SQL binding, and tests.
