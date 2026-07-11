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
stage descriptor. Query returns either a local root or the S3 bucket, prefix,
region, endpoint, and path-style policy configured by the deployment. The SDK
then opens storage directly. This discovery happens once per client, not per
query or object read.

For production S3, configure the query process instead of constructing a stage
in application code:

```bash
LAKE_S3_BUCKET=embodied-data \
LAKE_MANAGED_OBJECT_PREFIX=lake/managed-files \
AWS_REGION=us-east-1 \
LAKE_AUTH_TOKEN_FILE=/run/secrets/query-token \
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
all Debug/error output. This deployment credential authenticates the caller;
tenant catalog/object authorization remains a separate policy layer.

Create the table with a first-class `file` column through either the local or
remote administrative CLI:

```bash
lake table create robots.episodes \
  --column episode_id:utf8 \
  --column video:file
```

Its essential write path is:

```rust
client.insert(
    "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
    vec![
        InsertValue::Utf8("episode-42".into()),
        InsertValue::File(FileUpload::from_path("episode.mp4", "video/mp4")),
    ],
).await?;
```

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
Presigned browser access, resumable uploads, tenant authorization, and object
garbage collection remain outside this first Rust API.

For a local deployment, start metadata and query separately. The query process
is told where metadata lives; clients are not:

```bash
lake meta --addr 127.0.0.1:50052
lake query --addr 127.0.0.1:50051 --metadata-addr http://127.0.0.1:50052
```

Plaintext anonymous serving is allowed only on loopback. A non-loopback Query
or Metasrv listener requires `LAKE_AUTH_TOKEN_FILE`, `LAKE_TLS_CERT_FILE`, and
`LAKE_TLS_KEY_FILE`. Set `LAKE_ALLOW_INSECURE=true` only when a trusted service
mesh terminates both TLS and authentication before Lake. Metasrv nodes use
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
