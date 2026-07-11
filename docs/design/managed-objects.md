# Managed Objects

## Decision

Lake treats videos, pointclouds, model weights, and other multi-gigabyte
payloads as immutable managed objects. SQL exposes them as `FILE` values;
Lance physically stores their immutable `DataLocation` representation, not
the object bytes. A client SDK resolves that location and reads the object
directly from storage, so query and metadata services do not become a
large-file proxy.

## Rust SDK vertical slice

The current Rust SDK connects only to a query Flight endpoint and accepts one
deliberately narrow SQL form:

```rust
let client = LakeClient::connect(query_endpoint, managed_stage).await?;
```

It then binds a `FILE` through a parameterized insert:

```rust
client.insert(
    "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
    vec![
        InsertValue::Utf8("episode-42".into()),
        InsertValue::File(FileUpload::from_path("episode.mp4", "video/mp4")),
    ],
).await?;
```

The administrative schema DSL exposes the same logical type as
`--column video:file`. Reads stream through `LakeClient::query`; callers use
`lake_sdk::data_location(&batch, "video", row)` and pass the result to
`LakeClient::open` for direct object I/O.

For random-access consumers, `LakeClient::open_range(&location, start..end)`
uses Rust half-open byte ranges. The managed stage rejects empty, reversed, or
out-of-bounds intervals against `DataLocation.size_bytes` before opening the
object. Local storage seeks once and limits the returned stream to
`end - start`; S3 sends one `Range: bytes=start-(end-1)` request. The returned
type is still a streaming `AsyncRead`, so callers can feed a decoder without
allocating the interval or downloading the object prefix.

The SDK validates the SQL subset, placeholders, column names, and Arrow types
before opening any file upload. It then streams each `FileUpload` into the
Lake-owned managed stage in bounded chunks while computing SHA-256. Only after
the upload succeeds does it build a RecordBatch containing the immutable
`DataLocation` physical representation. It sends that metadata-only batch to
query, which forwards it unchanged to the metadata leader's existing append
commit path. Once metadata acknowledges the commit, that query node evicts the
table's cached registration so the same SDK connection observes its write;
other query nodes retain the normal bounded-staleness contract.

`ManagedObjectStore` is the SDK's object-safe storage boundary. The local
backend publishes `file://` locations after an atomic rename. The production
S3 backend accepts an already-configured AWS SDK client plus one Lake-owned
bucket/prefix. It uses multipart upload for non-empty objects, keeps at most
one 5 MiB part plus a small read buffer in memory, and incrementally computes
size and SHA-256. Any source, part, or completion failure triggers
`AbortMultipartUpload`; only a successful completion produces the returned
`DataLocation`. Empty objects use one ordinary `PutObject`.

```text
Rust SDK INSERT (logical FILE)
  -> schema + parameter validation
  -> direct chunked managed-stage upload
  -> DataLocation { uri, content_type, size_bytes, sha256 }
  -> query DoPut proxy (stateless)
  -> metadata leader -> Lance append -> registry version CAS
```

If upload fails, no append is attempted and the table's current version does
not advance. If a later append CAS loses a race, the uploaded object is
unreferenced and future object GC can reclaim it. Readers of a committed row
open its `DataLocation` directly through the SDK.

## Boundaries

`DataLocation.uri` is a stable identity, never an expiring signed URL. The
managed stage is the only storage namespace accepted by this SDK; public SQL
never accepts arbitrary storage URIs or credentials. Local locations use
`file://`; cloud locations use `s3://bucket/key`. The S3 reader validates the
exact configured bucket and path-segment prefix before `GetObject`, preventing
one SDK stage from becoming an arbitrary S3 reader. Endpoint, region,
credentials, path-style behavior, and future presigning policy stay in the
caller-configured AWS client. No raw object bytes travel over Flight SQL or
the metasrv control plane.

Range reads use the same containment check and caller-configured credentials
as sequential reads. They do not introduce a query endpoint, signed URL, or
arbitrary URI escape hatch.

The current slice does not provide multipart resume across SDK restarts,
tenant authorization, browser presigning, object deduplication, or garbage
collection. Those additions must keep the same visibility rule: a SQL-visible
`DataLocation` always identifies a complete, immutable object.
