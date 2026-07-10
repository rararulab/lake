# Managed Objects

## Decision

Lake treats videos, pointclouds, model weights, and other multi-gigabyte
payloads as immutable managed objects. SQL rows contain a `DataLocation`, not
the object bytes. A client SDK resolves that location and reads the object
directly from storage, so query and metadata services do not become a
large-file proxy.

## Rust SDK vertical slice

The current local SDK accepts one deliberately narrow SQL form:

```rust
client.insert(
    "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
    vec![
        InsertValue::Utf8("episode-42".into()),
        InsertValue::Object(ObjectFile::from_path("episode.mp4", "video/mp4")),
    ],
).await?;
```

The SDK validates the SQL subset, placeholders, column names, and Arrow types
before opening any object file. It then streams each `ObjectFile` into the
Lake-owned object prefix in bounded chunks while computing SHA-256. Only after
the upload succeeds does it build a RecordBatch containing the immutable
`DataLocation` and call the existing `Metasrv::append` commit path.

```text
Rust SDK INSERT
  -> schema + parameter validation
  -> direct chunked object upload
  -> DataLocation { uri, content_type, size_bytes, sha256 }
  -> Lance append
  -> registry version CAS
```

If upload fails, no append is attempted and the table's current version does
not advance. If a later append CAS loses a race, the uploaded object is
unreferenced and future object GC can reclaim it. Readers of a committed row
open its `DataLocation` directly through the SDK.

## Boundaries

`DataLocation.uri` is a stable identity, never an expiring signed URL. The
local implementation uses `file://`; cloud locations must be backed by an
authenticated ticket or short-lived direct-read capability generated at query
time. No raw object bytes travel over Flight SQL or the metasrv control plane.

The current slice does not provide remote Flight data writes, S3 multipart
presigning/resume, tenant authorization, signed locations, object
deduplication, or garbage collection. Those additions must keep the same
visibility rule: an SQL-visible `DataLocation` always identifies a complete,
immutable object.
