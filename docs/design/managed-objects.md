# Managed Objects

## Decision

Lake treats videos, pointclouds, model weights, and other multi-gigabyte
payloads as immutable managed objects. SQL exposes them as `FILE` values;
Lance physically stores their immutable `DataLocation` representation, not
the object bytes. A client SDK resolves that location and reads the object
directly from storage, so query and metadata services do not become a
large-file proxy.

## Rust SDK vertical slice

The current Rust SDK connects only to a query Flight endpoint. The query
service advertises a versioned, credential-free managed-stage descriptor once
at connection time, and the SDK constructs the matching storage client:

```rust
let client = LakeClient::connect(query_endpoint).await?;
```

The descriptor contains only storage topology: local root, or S3 bucket,
prefix, region, endpoint, and path-style policy. S3 credentials come from the
SDK process's AWS default credential chain or workload identity. Tests and
embedders retain `LakeClient::connect_with_store(query_endpoint, store)` as an
explicit injection seam.

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
S3 backend uses the discovered Lake-owned bucket/prefix and an AWS SDK client
configured from the descriptor plus the process credential chain. It uses
multipart upload for non-empty objects, keeps at most one 5 MiB part plus a
small read buffer in memory, and incrementally computes size and SHA-256.
Reader-backed uploads abort on failure. Path-backed uploads become resumable
when `LakeClientBuilder::with_upload_checkpoint_dir` is configured: a
credential-free versioned checkpoint records the random managed key, upload
id, source identity, and each part's ETag/checksum/SHA-256 after an atomic
fsync+rename. A retry takes an OS file lock, reconciles paginated S3
`ListParts`, rereads and verifies completed local parts while rebuilding the
whole-file SHA-256 state, then overwrites at most one uncheckpointed remote
suffix part and uploads only what remains. Explicit cancellation aborts the
exact upload and removes its checkpoint. If multipart completion succeeds but
its response is lost, the retry streams the random destination once and
requires its size and SHA-256 to match before clearing the checkpoint. Only
verified completion produces a `DataLocation`; empty objects use one ordinary
`PutObject`.

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
one SDK stage from becoming an arbitrary S3 reader. Endpoint, region, and
path-style behavior come from the query descriptor; credentials stay in the
SDK process and never cross the discovery protocol. No raw object bytes travel
over Flight SQL or the metasrv control plane.

Range reads use the same containment check and process credentials
as sequential reads. They do not introduce a query endpoint, signed URL, or
arbitrary URI escape hatch.

The current slice does not provide tenant authorization, browser presigning,
object deduplication, cross-host checkpoint sharing, or garbage collection.
Those additions must keep the same visibility rule: a SQL-visible
`DataLocation` always identifies a complete, immutable object.
