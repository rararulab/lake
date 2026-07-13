# lake-sdk

The Rust SDK's typed write and direct-read surface.

## Invariants

- Object bytes go straight between this SDK and object storage, never through
  query or metadata Flight services.
- SQL is a narrow parameterized `FILE` INSERT binding, not a second SQL engine.
- Schema and parameter validation complete before the SDK begins an upload.
- A `DataLocation` row is appended only after every referenced object upload
  succeeds; per-table visibility remains owned by `Metasrv::append`.
- Ambiguous append retries reuse one UUIDv7 identity, digest, and encoded Arrow
  payload; uploaded video/model bytes are never uploaded again by that retry.
- The retry window exceeds metadata lease expiry so ungraceful leader failover
  does not force callers to generate a second logical operation.
- Retry expiry returns a `PendingAppend`; resuming it must preserve the UUIDv7
  operation identity, encoded Arrow payload, and already-uploaded objects.
- With an operator-owned checkpoint directory, the exact append operation is
  atomically durable before its first RPC. Restart loading is bounded and
  validates file identity, stage identity, integrity, descriptor operation,
  and payload digest before replay; only conclusive outcomes remove it.
- The primary public client receives only a query endpoint, discovers the
  credential-free managed-stage descriptor once, and uses process credentials;
  the production crate must not depend on, construct, or start `lake-metasrv`.
- `connect_with_store` is an explicit test/embedding seam, not the normal
  application connection path.
- `LakeClientBuilder` applies one redacted TLS/bearer configuration to every
  Query Flight client; credentials never enter stage discovery or SQL data.
- `query` streams Flight RecordBatches; `data_location` decodes a logical
  `FILE`, and `open` reads directly while verifying size/SHA-256 at EOF.
- `open_range` delegates one validated half-open interval directly to the
  configured stage and independently rejects a short stream; query and metasrv
  never proxy range bytes.
- `open_via_query` and `open_range_via_query` are the credentialless direct-read
  paths: they obtain one Query-issued capability and stream directly from
  object storage without constructing a managed-stage client. Full reads retain
  EOF size/SHA-256 verification; range reads require an exact `206`
  `Content-Range` response and make no whole-object integrity claim.
- Query-only direct-read HTTP follows no redirects or caller/system proxies,
  keeps capability URL/header values out of public errors, and drops an
  unfinished response on reader drop; it never buffers or proxies an object
  through a Lake service.
- `presign_read` delegates a bounded S3 GET capability only after the same
  managed-prefix validation; capability URLs and headers are redacted by
  default and never enter SQL rows or Flight services.

## Layout

- `lib.rs` — public client, parameter values, SQL binding, and tests.
