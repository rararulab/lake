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

The SDK validates the SQL subset, placeholders, column names, and Arrow types
before opening any file upload. It then streams each `FileUpload` into the
Lake-owned managed stage in bounded chunks while computing SHA-256. Only after
the upload succeeds does it build a RecordBatch containing the immutable
`DataLocation` physical representation. It sends that metadata-only batch to
query, which forwards it unchanged to the metadata leader's existing append
commit path. Once metadata acknowledges the commit, that query node evicts the
table's cached registration so the same SDK connection observes its write;
other query nodes retain the normal bounded-staleness contract.

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
never accepts arbitrary storage URIs or credentials. The local implementation
uses `file://`; cloud locations must be backed by an authenticated ticket or
short-lived direct-read capability generated at query time. No raw object
bytes travel over Flight SQL or the metasrv control plane.

The current slice does not provide S3 multipart presigning/resume, tenant
authorization, signed locations, object deduplication, or garbage collection.
Those additions must keep the same visibility rule: an SQL-visible
`DataLocation` always identifies a complete, immutable object.
