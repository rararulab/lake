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
lake query --addr 0.0.0.0:50051 --metadata-addr http://lake-meta:50052
```

Lake stores the stable `s3://` identity. AWS credentials come from the SDK
process's default credential chain or workload identity; they never enter SQL,
table rows, or the discovery descriptor. Embedders and tests that need a custom
backend can use the explicit `LakeClient::connect_with_store` constructor.

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

For the design and invariants, see [managed objects](docs/design/managed-objects.md)
and [architecture](docs/architecture.md).
