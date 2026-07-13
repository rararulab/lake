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

Full opens are integrity-verified by default. Before touching storage the SDK
requires the DataLocation SHA-256 to be exactly 64 hexadecimal characters. It
then caps the backend stream at the declared size, hashes only bytes delivered
to the caller, and privately probes one byte beyond the cap. EOF succeeds only
when the object is neither short nor long and the incremental SHA-256 matches.
The wrapper retains constant memory regardless of object size. Verification is
an EOF property: dropping the reader early makes no success claim. Terminal
integrity failures are `InvalidData` I/O errors with a public typed source.

For random-access consumers, `LakeClient::open_range(&location, start..end)`
uses Rust half-open byte ranges. The managed stage rejects empty, reversed, or
out-of-bounds intervals against `DataLocation.size_bytes` before opening the
object. Local storage seeks once and S3 sends one
`Range: bytes=start-(end-1)` request. Both the stage and SDK cap the returned
stream at `end - start`; draining it either yields exactly that many bytes or
ends with `InvalidData` carrying `ObjectIntegrityError::PrematureEof` with the
range's expected and observed byte counts. The returned type is still a
streaming `AsyncRead`, so callers can feed a decoder without allocating the
interval or downloading the object prefix.

Range and presigned reads do not claim the full-object SHA-256. A partial
interval cannot establish the identity of bytes it did not read; per-range
checksums require a future chunk/Merkle identity format rather than pretending
the whole-file digest was verified.

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
backend publishes `file://` locations after an atomic rename. From staging
creation until that rename, a path-only cleanup task owns an unpublished local
upload: caller-task cancellation removes the `.uploading` file without
retaining source bytes, while a published final object is never removed by
that cleanup. The production S3 backend uses the discovered Lake-owned
bucket/prefix and an AWS SDK client
configured from the descriptor plus the process credential chain. It uses
multipart upload for non-empty objects. It reads and hashes source bytes in
order while polling four `UploadPart` requests concurrently by default. Each
request owns one 64 MiB buffer, so the default request-body ceiling is 256 MiB
per object; the public S3 store configuration accepts `1..=16`, whose hard
ceiling is 1 GiB. Response order cannot change the completed-part order or
whole-object SHA-256.
Reader-backed uploads abort on failure. Each ordinary multipart upload owns
one metadata-only cleanup task while it is active. Normal completion disarms
the task; explicit failure cancels all part futures and waits for abort; caller
cancellation drops the decision channel and makes the task attempt abort for
at most 30 seconds. The task never owns source bytes or part buffers. A process
or host crash cannot execute client cleanup, so production S3 buckets must
still configure an `AbortIncompleteMultipartUpload` lifecycle rule.
Path-backed uploads become resumable
when `LakeClientBuilder::with_upload_checkpoint_dir` is configured: a
credential-free versioned checkpoint records the random managed key, upload
id, source identity, and each part's ETag/checksum/SHA-256 after an atomic
fsync+rename. Checkpoints also record the concurrency window that could have
completed remotely before its ordered response was published; legacy
checkpoints default that field to one. A retry takes an OS file lock,
reconciles paginated S3 `ListParts`, rereads and verifies completed local parts
while rebuilding the whole-file SHA-256 state, then overwrites every bounded,
untrusted remote suffix part from the source and uploads only what remains.
New V1 checkpoints record 64 MiB parts. Existing V1 checkpoints recorded with
the former 5 MiB size remain resumable: recovery uses their persisted size for
completed-prefix verification and the subsequent pipeline, rather than
repartitioning the source. The only accepted resumable V1 sizes are 5 MiB and
64 MiB; any other persisted value fails closed before source or S3 progress.
Explicit cancellation aborts the
exact upload and removes its checkpoint. If multipart completion succeeds but
its response is lost, the retry streams the random destination once and
requires its size and SHA-256 to match before clearing the checkpoint. Only
verified completion produces a `DataLocation`; empty objects use one ordinary
`PutObject`.

S3 accepts only part numbers `1..=10,000`. After a non-empty 10,000th part,
the stage reads once more to distinguish EOF from another part: EOF completes
normally, while a non-empty 10,001st part returns a typed local error before
constructing `UploadPart`. Ordinary uploads abort through their existing
failure path; the same terminal error aborts a resumable upload and removes
its checkpoint. With full 64 MiB parts, this bounds unknown-length streams to
roughly 625 GiB. It does not claim support for S3's full maximum object range.

After all values have become `DataLocation`s, the SDK persists a separate
versioned append checkpoint before the first Flight RPC when the same
operator-owned directory is configured. It contains the exact UUIDv7 operation
ID, encoded Flight messages, credential-free managed-stage identity, and an
integrity digest; it never contains object bytes or credentials. Publication is
file sync, atomic rename, then directory sync. Restart recovery first lists at
most 1,024 entries without loading payloads, then loads one selected operation
under the normal 64 MiB Flight ceiling plus fixed format overhead. It validates
the filename/content operation ID, stage identity, checkpoint digest, FILE
descriptor operation, and declared Flight payload digest before any replay.
If rename publishes the final file but parent sync fails, the typed error
returns the exact `PendingAppend` and published path so the caller never loses
operation ownership behind an ordinary preparation failure.

Ambiguous transport, Flight response protocol/decoding, and invalid-result
metadata failures retain the checkpoint because the commit may already be
durable. Success or an explicit server rejection removes and directory-syncs
it. Cleanup I/O failure is logged
but never changes a conclusive append result; a leftover checkpoint remains
replay-safe. Thus a crash after commit but before deletion merely replays the
same server-side idempotency identity and converges to the original version; it
cannot upload the large objects again or append a second row. The directory is
local trusted SDK state, not a cross-tenant operation queue.

The checkpoint is replayable only inside the coordinator's append-operation
retention horizon, configured by `LAKE_APPEND_OPERATION_RETENTION_SECS` and
defaulting to seven days. An older UUIDv7 operation is rejected before replay;
the SDK classifies that explicit server rejection as terminal and removes the
local checkpoint. Operators therefore recover and alert on pending entries
before the shortest retention horizon used by any metadata server.

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
unreferenced and object GC can reclaim it after the safety horizon. Readers of
a committed row open its `DataLocation` directly through the SDK.

## Reference journal and garbage collection

Lance append extracts `DataLocation` identities while consuming RecordBatches
and releases each consumed batch immediately. Before the new version is
returned to the registry CAS, the engine writes canonical, chunked
parent→child reference deltas under the dataset's `_lake/object_refs/`
namespace. Version-producing maintenance writes the same lineage edge. A
missing, corrupt, mismatched, or non-monotonic edge makes reference enumeration
fail closed.

The registry version is the traversal root. The GC worker pages every table's
retained lineage, spills additions into bounded sorted runs, and performs a
bounded-fan-in external merge into one globally URI-sorted live index. Current
tables are append-only; a non-empty removal delta is rejected until
retained-snapshot-aware row deletion exists. This is conservative: committed
objects may be retained longer, but cannot be deleted early.

Candidate inventory and the live index are merged in URI order. Only objects
inside the exact managed stage, older than the configured cutoff, and absent
from the live index enter draft pages. Draft output never authorizes deletion.
The writer validates the complete stream, then publishes a content-addressed
page chain and its manifest last. The manifest binds stage, cutoff, totals,
and the registry-root fingerprint.

Apply reopens and verifies the full plan before mutation, checks the registry
fingerprint before each page, validates the next content hash from its durable
checkpoint, deletes only that bounded page, and atomically fsyncs progress.
Local `NotFound` and S3's idempotent `DeleteObject` are success. This worker is
owned by `lake gc`; it never runs in Query or Metasrv maintenance. A root
validation reads every table registration through one typed metastore prefix
scan and canonicalizes it into table order before equality or fingerprinting;
it does not list namespaces or point-read registrations individually.

The cost is linear in retained reference sidecars plus managed inventory, not
table row count. Memory is bounded by one reference run, a finite merge fan-in,
one inventory page, and one deletion page. Temporary disk must hold the live
index and inventory spool; the immutable plan holds only orphan candidates.

Operationally, the safety horizon must exceed the maximum time from completed
upload to committed INSERT, including retries. Apply should run in a
write-quiescent window; any observed registry change invalidates the plan.

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
as sequential reads. A credentialed SDK process may explicitly mint a bounded
S3 GET capability after that same containment check. The capability URL and
required headers are redacted by default, live for at most one hour, and may
be used with an additional HTTP Range header. Stable `DataLocation` rows never
contain this expiring credential.

Tenant authorization derives one exact managed-stage child prefix per validated
tenant and the SDK refuses locations outside it. The current slice does not
provide server-issued signing for credentialless SDK processes, object
deduplication, cross-host upload-checkpoint sharing, or row-level DELETE.
Those additions must keep the same visibility rule: a SQL-visible
`DataLocation` always identifies a complete, immutable object.
