# lake

Lake is a lakehouse for embodied-AI data: episodes, videos, point clouds, and
model weights live in disaggregated object storage while SQL exposes their
metadata and immutable file references.

The query layer is stateless and reads storage directly. The metadata layer
coordinates table versions; it never becomes a proxy for multi-gigabyte object
bytes.

## Quick start

Install [mise](https://mise.jdx.dev/), then let the repository manage the
remaining development tools:

```bash
mise run doctor
mise run e2e
```

`mise run e2e` creates a local table, ingests data, commits a snapshot, and
runs a SQL query.

## SQL-managed files

Lake models a video, model checkpoint, or similar large payload as a logical
SQL `FILE`. The Rust SDK uploads the bytes directly to a Lake-managed stage;
the table stores an immutable `DataLocation` containing the durable URI, media
type, byte size, and SHA-256. A caller reads the object through the SDK rather
than through the query or metadata service.

Run the complete local example:

```bash
cargo run -p lake-sdk --example managed_file
```

The SDK connects only to the public query endpoint; it does not construct,
embed, or connect directly to `lake-metasrv`:

```rust
let client = LakeClient::connect("http://127.0.0.1:50051").await?;
```

At connection time the SDK asks query for a versioned, credential-free managed
stage descriptor. Query derives `tenants/<tenant-id>` below the configured local
root or S3 prefix and returns only that child stage's location hints. The SDK
then opens storage directly and rejects `DataLocation` values outside the child
prefix. Discovery happens once per client, not per query or object read.

For production S3, configure the query process instead of constructing a stage
in application code:

```bash
LAKE_S3_BUCKET=embodied-data \
LAKE_MANAGED_OBJECT_PREFIX=lake/managed-files \
AWS_REGION=us-east-1 \
LAKE_AUTH_PRINCIPALS_FILE=/run/secrets/query-principals.json \
LAKE_TLS_CERT_FILE=/run/tls/query.crt \
LAKE_TLS_KEY_FILE=/run/tls/query.key \
LAKE_METADATA_AUTH_TOKEN_FILE=/run/secrets/meta-token \
LAKE_METADATA_CA_FILE=/run/tls/ca.crt \
LAKE_METADATA_SERVER_NAME=lake-meta.internal \
lake query --addr 0.0.0.0:50051 \
  --metadata-addr https://lake-meta.internal:50052
```

Lake stores the stable `s3://` identity. AWS credentials come from the SDK
process's default credential chain or workload identity; they never enter SQL,
table rows, or the discovery descriptor. Embedders and tests that need a custom
backend can use the explicit `LakeClient::connect_with_store` constructor.

The principal map is immutable for the process lifetime and must be a regular
file with no group/other permissions on Unix (`chmod 600`). Restart replicas to
rotate it. Tokens are opaque; `role` is one of `user`, `query_service`,
`metadata_peer`, or `admin`:

```json
{
  "bindings": [
    {
      "token": "replace-with-secret-from-your-secret-manager",
      "principal_id": "robotics-ingest",
      "tenant_id": "acme-robotics",
      "role": "user",
      "namespaces": ["acme_episodes", "acme_models"]
    }
  ]
}
```

Query's map contains SDK/user credentials. Metasrv's map contains the
Query→Meta credential as `query_service`, follower credentials as
`metadata_peer`, and any direct administrative identities. Keep each internal
client token in its existing `LAKE_METADATA_AUTH_TOKEN_FILE` or
`LAKE_PEER_AUTH_TOKEN_FILE`. `LAKE_AUTH_TOKEN_FILE` remains a backward-compatible
single deployment-admin credential and is mutually exclusive with the map.
For S3, also restrict each SDK workload identity to
`<base-prefix>/tenants/<tenant-id>/*`; the software prefix check is defense in
depth, not a replacement for IAM.

Production SDK connections verify TLS and send the Query credential on every
Flight RPC, including discovery, schema lookup, SQL, and FILE append:

```rust
let client = LakeClient::builder("https://query.internal:50051")
    .with_bearer_token(std::fs::read_to_string("/run/secrets/query-token")?.trim())?
    .with_ca_certificate_pem(std::fs::read("/run/tls/ca.crt")?)
    .with_server_name("query.internal")
    .connect()
    .await?;
```

Tokens are read from files rather than command-line flags and are redacted from
all Debug/error output. Query authorizes parsed table references before planning;
Metasrv repeats authorization before registry or engine access. Hidden resources
return the same `PermissionDenied` response as other authorization failures.

Create the table with a first-class `file` column through either the local or
remote administrative CLI:

```bash
lake table create robots.episodes \
  --column episode_id:utf8 \
  --column video:file
```

Its essential write path is:

```rust
let client = LakeClient::builder(query_endpoint)
    .with_upload_checkpoint_dir("/var/lib/lake/upload-checkpoints")
    .connect()
    .await?;
client.insert(
    "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
    vec![
        InsertValue::Utf8("episode-42".into()),
        InsertValue::File(FileUpload::from_path("episode.mp4", "video/mp4")),
    ],
).await?;
```

The checkpoint directory makes path-backed S3 uploads restart-resumable. Lake
stores a credential-free, versioned checkpoint after every completed 5 MiB
part. A retry locks and validates the checkpoint, reconciles it with S3,
rehashes the completed local prefix, and uploads only missing parts. Changed
source files or stage identities fail closed. Checkpoints disappear only after
verified multipart completion; `ManagedObjectStore::cancel_upload` explicitly
aborts an abandoned upload. Reader-backed uploads remain one-shot because an
arbitrary stream cannot be replayed after process restart.

Query results stream back through the same SDK connection. Decode the logical
`FILE` value into its stable `DataLocation`, then open the object directly:

```rust
let mut results = client
    .query("SELECT video FROM lake.robots.episodes")
    .await?;
let batch = results.try_next().await?.expect("one result batch");
let location = lake_sdk::data_location(&batch, "video", 0)?;
let reader = client.open(&location).await?;
```

Video decoders and model loaders can fetch only the bytes they need. Ranges
are half-open (`start..end`), validated against the immutable object size, and
stream directly from local storage or one S3 Range GET:

```rust
// Read exactly 1 MiB starting at byte 8 MiB; no prefix download.
let reader = client
    .open_range(&location, 8 * 1024 * 1024..9 * 1024 * 1024)
    .await?;
```

Empty, reversed, and out-of-bounds ranges return a typed SDK object error
before storage I/O.

The example performs insert, query, `DataLocation` decoding, and direct open
through `LakeClient`. Local development discovers a `file://` stage; production
discovers an `s3://` stage. Non-empty S3 objects stream through
bounded 5 MiB multipart parts, with incremental SHA-256 and abort-on-error.
The query service forwards only the Arrow row to the metadata leader;
video/model bytes travel directly between the SDK and the managed stage.
Tenant child-prefix authorization is enforced by Query, Metasrv, and the SDK;
presigned browser access remains outside this Rust API.

## Managed-object garbage collection

Every version-producing Lance operation writes an immutable reference-delta
sidecar before its version can become registry-visible. `lake gc` traverses
those sidecars, not table rows, and externally sorts the live URI set with
bounded memory. Inventory is paginated for S3 and bounded for local storage.

Planning is always the default and never deletes objects. Choose a safety age
longer than the maximum upload-to-INSERT/retry interval and a new plan path:

```bash
lake --data-dir ./data gc \
  --plan ./gc-plans/2026-07-11 \
  --safety-age-secs 86400 \
  --json
```

The resulting directory is an immutable, content-addressed plan. Its manifest
is published only after every lineage page, inventory entry, age check, and
candidate page has been validated. Review the candidate/byte totals, then
apply that exact plan explicitly:

```bash
lake --data-dir ./data gc \
  --plan ./gc-plans/2026-07-11 \
  --apply \
  --checkpoint ./gc-plans/2026-07-11.apply.json \
  --json
```

Apply fsyncs progress after each bounded page and resumes without replaying
completed pages. Already-absent S3/local objects are successful idempotent
outcomes. The plan binds the managed stage and registry roots; a table create,
drop, append, or maintenance commit makes it stale and GC fails closed. Run
apply in a write-quiescent window. The safety age is also a protocol
requirement: an object older than it must never become newly referenced; retry
such an abandoned workflow by uploading a fresh object.

Cloud mode uses the same `LAKE_S3_*`, `LAKE_DYNAMODB_*`, and AWS workload
credentials as the other CLI commands. GC is a separate worker; it does not
start Query or Metasrv. On a versioned S3 bucket, `DeleteObject` creates a
delete marker rather than reclaiming old versions, so configure an S3
noncurrent-version lifecycle policy when physical byte reclamation is required.

For a local deployment, start metadata and query separately. The query process
is told where metadata lives; clients are not:

```bash
lake meta --addr 127.0.0.1:50052
lake query --addr 127.0.0.1:50051 --metadata-addr http://127.0.0.1:50052
```

Plaintext anonymous serving is allowed only on loopback and receives the named
development principal. A non-loopback Query or Metasrv listener requires either
`LAKE_AUTH_PRINCIPALS_FILE` or `LAKE_AUTH_TOKEN_FILE`, plus
`LAKE_TLS_CERT_FILE` and `LAKE_TLS_KEY_FILE`. Set `LAKE_ALLOW_INSECURE=true` only
when a trusted service mesh terminates TLS before Lake; Lake bearer
authentication remains mandatory. Metasrv nodes use
`LAKE_PEER_AUTH_TOKEN_FILE`, `LAKE_PEER_CA_FILE`, and
`LAKE_PEER_SERVER_NAME` for follower-to-leader forwarding.

Each Query replica also enforces finite admission limits. Defaults are 64
concurrent queries, 100 ms queue wait, 30 minutes execution time, and 1 MiB of
SQL/ticket text. Override them at process startup:

```bash
LAKE_QUERY_MAX_CONCURRENT=32 \
LAKE_QUERY_QUEUE_TIMEOUT_MS=250 \
LAKE_QUERY_EXECUTION_TIMEOUT_MS=900000 \
LAKE_QUERY_MAX_SQL_BYTES=262144 \
lake query ...
```

Saturation returns gRPC `ResourceExhausted`; execution expiry terminates the
result stream with `DeadlineExceeded`. The concurrency permit remains owned by
the DoGet stream, so completing, timing out, or dropping the stream releases
capacity. These are per-replica safety limits; tenant quotas and distributed
fair queuing remain separate policy layers.

`lake query` and `lake meta` handle SIGINT and SIGTERM as graceful shutdowns.
They stop accepting new Flight connections, allow existing RPCs to drain for
30 seconds, then close any remaining connections. Override the bound with a
positive millisecond value:

```bash
LAKE_SHUTDOWN_GRACE_MS=10000 lake query ...
```

Both processes join their background tasks before exiting. Metasrv keeps its
lease while accepted writes drain, then immediately resigns it, so a standby
can take over without waiting for the 10-second lease TTL. Exceeding the drain
window is reported as an error and makes the process exit non-zero.

For the design and invariants, see [managed objects](docs/design/managed-objects.md)
and [architecture](docs/architecture.md).
