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

The example uploads two `FILE` values, queries their `DataLocation` rows, and
streams each direct read to a temporary file sink rather than retaining an
object-sized buffer in the SDK process.

The SDK connects only to the public query endpoint; it does not construct,
embed, or connect directly to `lake-metasrv`:

```rust
let client = LakeClient::connect("http://127.0.0.1:50051").await?;
```

Batch multiple episode rows into one bounded Arrow append and one table
version. Each `FILE` still streams directly to managed storage; only its
`DataLocation` enters the batch. Batches contain 1..=10,000 rows, cap caller
metadata and accumulated returned locations at 16 MiB each, use the exact
protobuf size for the 64 MiB Flight limit, and validate every row before the
first upload:

```rust
let version = client
    .insert_many(
        "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
        vec![
            vec![InsertValue::Utf8("ep-1".into()), InsertValue::File(video_1)],
            vec![InsertValue::Utf8("ep-2".into()), InsertValue::File(video_2)],
        ],
    )
    .await?;
```

At connection time the SDK asks query for a versioned, credential-free managed
stage descriptor. Query derives `tenants/<tenant-id>` below the configured local
root or S3 prefix and returns only that child stage's location hints. The SDK
then opens storage directly and rejects `DataLocation` values outside the child
prefix. Discovery happens once per client, not per query or object read.

Flight SQL catalog discovery is also cache-only. New table registrations carry
the table's Arrow IPC schema, and each Query replica loads names, versions, and
schemas through one authenticated, conditional `catalog_snapshot` action. The
Metasrv authority scans in 64-entry pages, accounts each entry before retaining
it, admits only one full snapshot per process until its response is dropped,
and returns either `not_modified` or one directory fenced by matching opaque
generation reads. Remote snapshots fail closed until the monotonic directory
authority marker exists. `GetTables(include_schema=true)` returns the real schema
without request-path metadata I/O; warm listing and registration cache hits
issue zero RPCs. Served `lake query` connects this remote source before binding
and never falls back to direct registry reads. Registrations created by older
Lake versions remain queryable and listable; schema-inclusive discovery for
such a table fails explicitly until it is recreated or migrated, rather than
claiming the table has an empty schema.

For production S3, configure the query process instead of constructing a stage
in application code:

```bash
LAKE_S3_BUCKET=embodied-data \
LAKE_MANAGED_OBJECT_PREFIX=lake/managed-files \
LAKE_MANIFEST_DYNAMODB_TABLE=lake_manifests \
LAKE_ASYNC_QUERIES=true \
LAKE_ASYNC_DYNAMODB_TABLE=lake_async_queries \
LAKE_ASYNC_RESULT_PREFIX=lake/async-query-results \
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

Lake stores the stable `s3://` identity. In the standard direct-stage SDK mode,
AWS credentials come from the SDK process's default credential chain or
workload identity; they never enter SQL, table rows, or the discovery
descriptor. Embedders and tests that need a custom backend can use the explicit
`LakeClient::connect_with_store` constructor. The credentialless read-capability
path below instead gives the SDK only a Query connection.

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
    .with_schema_cache(1_024, std::time::Duration::from_secs(60))?
    .connect()
    .await?;
```

The schema cache is shared by client clones, singleflights same-table misses,
and stores only successful lookups. Defaults are 1,024 tables for 60 seconds;
capacity is capped at 65,536 and TTL at one hour. Expiration bounds stale
schema exposure after drop/recreate. A coordinator that replaces a table can
call `invalidate_table_schema(&table).await` or `clear_schema_cache()` for
immediate local refresh. Metasrv remains authoritative and still validates
every append.

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

Remote administration sends only the table identifier and schema. Metasrv
derives the dataset location from its own local `--data-dir` or trusted
`LAKE_S3_BUCKET` and optional `LAKE_TABLE_PREFIX` configuration; clients cannot
select an arbitrary storage URI:

```bash
lake client --addr 127.0.0.1:50052 create-table robots.episodes \
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
stores a credential-free, versioned checkpoint after every completed 64 MiB
part. A retry locks and validates the checkpoint, reconciles it with S3,
rehashes the completed local prefix, and uploads only missing parts. Changed
source files or stage identities fail closed. Checkpoints disappear only after
verified multipart completion; `ManagedObjectStore::cancel_upload` explicitly
aborts an abandoned upload. Reader-backed uploads remain one-shot because an
arbitrary stream cannot be replayed after process restart.

V1 checkpoints created before this default used 5 MiB parts. A current store
resumes those uploads using the checkpoint's recorded 5 MiB size for both
prefix rehashing and the remaining pipeline; newly created checkpoints still
record 64 MiB. Resume accepts only those two explicit V1 sizes, so a tampered
or unknown size fails closed before source or S3 progress.

S3 uploads overlap four parts by default while hashing and checkpointing in
source order. That is an exact 256 MiB request-body bound per object at Lake's
64 MiB part size. Advanced embedders using `S3ObjectStore` directly may set
`with_upload_concurrency(1..=16)`; zero and larger values fail before S3 I/O.
On restart, any remote responses ahead of the durable contiguous checkpoint
are treated as untrusted and overwritten from the verified source.
Cancelling an ordinary reader-backed upload also cancels its owned part
requests and starts one bounded best-effort multipart abort. Because no client
can run cleanup after a process or host failure, production buckets must also
configure an `AbortIncompleteMultipartUpload` lifecycle rule as the final
orphan-safety boundary.

The same directory also makes the metadata append durable after every object
has uploaded. Before its first append RPC, the SDK atomically fsyncs the exact
UUIDv7 operation ID and encoded Arrow metadata (never object bytes or
credentials). If the process dies while the commit result is unknown, a new
client can enumerate the bounded recovery set and resume one exact operation:

If the final rename succeeds but directory sync fails, preparation returns a
typed uncertainty error whose `into_pending_append()` preserves that exact
operation and published path; do not start a fresh insert.

```rust
for operation_id in client.pending_append_ids().await? {
    let committed_version = client.resume_pending_append(&operation_id).await?;
    println!("recovered {operation_id} at version {committed_version:?}");
}
```

Success and explicit server rejection remove the append checkpoint; ambiguous
transport, response protocol/decoding, and invalid-result failures retain the
same operation. Cleanup I/O failure is logged without changing a
conclusive commit result, and any leftover remains replay-safe. A crash after
server commit but before local cleanup safely replays to the original version.
Checkpoint discovery is capped at 1,024 directory entries and each file is
size-bounded and integrity-checked. Use one checkpoint directory only within a
single trusted SDK/tenant boundary.

Recovery is finite, not a permanent work queue: resume within the metadata
server's `LAKE_APPEND_OPERATION_RETENTION_SECS` horizon (seven days by
default). After that horizon the server rejects the UUIDv7 operation as
expired; the SDK treats that rejection as conclusive and removes the local
append checkpoint. Monitor pending checkpoints and run the startup recovery
loop before the shortest retention configured across the deployment.

Query results stream back through the same SDK connection. Decode the logical
`FILE` value into its stable `DataLocation`, then open the object directly:

The Flight SQL statement capability is snapshot-stable across its two RPCs.
`GetFlightInfo` resolves every physical table in joins, subqueries, and CTEs
to `{location, engine, incarnation, version}`, plans against those exact
providers, and encrypts the bounded set into the tenant/principal-bound
ticket. `DoGet` reconstructs the same request-local catalog on any Query
replica without reading a mutable current-version pointer. A concurrent append
therefore cannot change the advertised schema or rows; a reclaimed historical
snapshot fails explicitly and never falls forward to latest. Ticket protocol
upgrades require blue/green or drained cutover because older replicas are
intentionally unable to ignore new snapshot claims.

```rust
let mut results = client
    .query("SELECT video FROM lake.robots.episodes")
    .await?;
let batch = results.try_next().await?.expect("one result batch");
let location = lake_sdk::data_location(&batch, "video", 0)?;
let mut reader = client.open(&location).await?;
let mut destination = tokio::fs::File::create("episode.mp4").await?;
tokio::io::copy(&mut reader, &mut destination).await?; // drain to verified EOF
```

For queries whose result generation should survive one client connection or
Query replica, use the same client with standard Flight `PollFlightInfo`:

```rust
use futures::TryStreamExt;

let mut results = client
    .query_async("SELECT * FROM lake.robots.training_samples")
    .await?;
while let Some(batch) = results.try_next().await? {
    consume(batch).await?;
}
```

For workflow engines and long jobs, persist the opaque handle and resume after
a process or Query-connection restart:

```rust
let handle = client.submit_async("SELECT * FROM lake.robots.training_samples").await?;
checkpoint.write(handle.to_json()?).await?;

// A later process may connect through another Query replica.
let restored = lake_sdk::AsyncQueryHandle::from_json(&checkpoint.read().await?)?;
let mut results = restarted_client.resume_async(restored).await?;
```

`poll_async` performs one non-blocking poll and returns a refreshed handle that
the caller can checkpoint; `cancel_async` uses standard `CancelFlightInfo`.
Handles contain only versioned encrypted capability bytes and expiry, enforce a
16 KiB bound, and redact capability contents from `Debug`. Initial submission
retries reuse one 128-bit ID, so a lost response cannot execute the SQL twice.

Enable it on `lake query` with `LAKE_ASYNC_QUERIES=true`. Local mode creates
separate `async-query-state` and `async-query-results` directories beside the
catalog. S3 mode uses the dedicated `LAKE_ASYNC_DYNAMODB_TABLE` (default
`lake_async_queries`) and `LAKE_ASYNC_RESULT_PREFIX` (default
`async-query-results`). Query replicas share encrypted, identity-bound jobs,
CAS-fenced leases, immutable Arrow IPC parts, and one atomic result manifest;
polling never starts or embeds a metadata server.

Each Query replica runs at most four async workers by default, with one running
job per tenant and a 30-minute absolute execution deadline. Configure these
with `LAKE_ASYNC_WORKER_CONCURRENCY`,
`LAKE_ASYNC_WORKER_CONCURRENCY_PER_TENANT`, and
`LAKE_ASYNC_EXECUTION_TIMEOUT_MS`. The scheduler scans bounded durable pages,
selects eligible tenants round-robin without parking worker slots behind a
saturated tenant, and cancels both execution and lease renewal at the deadline.
These limits are process-local; CAS worker leases preserve single ownership but
do not turn them into cluster-global tenant quotas.

Retained async storage is bounded separately and durably across replicas.
`LAKE_ASYNC_MAX_OUTSTANDING_PER_TENANT` defaults to 8 (range 1..=128), and
`LAKE_ASYNC_MAX_RESULT_BYTES` defaults to 17179869184 bytes / 16 GiB (range
64 MiB..=256 GiB). Submission reserves a tenant slot in the shared async state
store before uploading its encrypted job. A schema-v2 job persists its byte
ceiling, so restarting a worker with looser configuration cannot enlarge an
existing result. Crashes may conservatively retain a slot until bounded
point-read reconciliation, but cannot under-count a live job. Quota metrics and
Flight errors contain no tenant, principal, digest, query, or configured-size
labels. These are retained-object limits, not CPU, memory, billing, or
cluster-global execution-fairness guarantees.

Async result parts stay bounded while crossing the storage boundary. The IPC
encoder writes fixed 64 KiB chunks through a four-slot channel and rejects an
encoded part before publication when it crosses the 64 MiB ceiling. `DoGet`
feeds verified object chunks incrementally into Arrow's `StreamDecoder` and
buffers at most two decoded batches, so the first batch can be returned before
object EOF without collecting the complete part. A bounded framing validator
rejects oversized metadata, impossible body lengths, and compressed IPC before
Arrow decoding, preventing declared decompression sizes from bypassing the
encoded-byte ceiling. The returned Flight stream owns the reader pump, decoder,
deadline, and admission permit; completion, failure, timeout, cancellation, or
client drop tears down that whole pipeline, and the permit remains held until
the blocking decoder actually exits.

The wire surface is also exercised by Apache Arrow's official ADBC Flight SQL
driver, independently of the Rust SDK. `mise run test-adbc` starts real
loopback Query listeners and verifies a typed 20,000-row multi-batch result,
stable read-only DML rejection, and bearer success/failure using pinned ADBC
and PyArrow wheels from `interop/adbc/uv.lock`. Standard Rust Flight tests
cover the lower-level `PollFlightInfo`, multi-endpoint `DoGet`, and
`CancelFlightInfo` lifecycle. This matrix does not claim ADBC transactions,
DML, prepared statements, bulk ingestion, or catalog metadata support.

`open` is integrity-verified by default. It validates the stored SHA-256 shape
before storage I/O, caps the stream at `DataLocation.size_bytes`, and computes
SHA-256 incrementally with constant memory. A full read succeeds only after
EOF proves both size and hash. If the caller drops the reader early, no
integrity claim has completed; short, overlong, and same-size corrupt objects
end with `InvalidData` carrying a typed `ObjectIntegrityError` source.

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
before storage I/O. Draining a valid range either yields exactly `end - start`
bytes or returns `InvalidData` with a typed `ObjectIntegrityError::PrematureEof`
source; the reader never exposes bytes beyond the requested interval. A partial
range cannot prove the full-object SHA-256, so `open_range` intentionally makes
no full-object integrity claim.

For S3, Lake also rejects the one Range GET response before yielding its body
unless `Content-Range` is exactly `bytes start-(end-1)/size_bytes` and
`Content-Length` is exactly `end-start`. This rejects a proxy that ignores a
Range header without issuing a HEAD request or buffering the payload.

A credentialed Rust process can delegate one S3 read without handing its AWS
credentials to the eventual HTTP consumer. This local-IAM path is valid for 1
second through 1 hour, is scoped to the already validated tenant child-prefix,
and performs no object GET while being minted:

```rust
let capability = client
    .presign_read(&location, std::time::Duration::from_secs(300))
    .await?;
let url = capability.url();       // sensitive: do not log
let headers = capability.headers(); // send these with the HTTP GET
```

`Debug` redacts the URL and header values. HTTP clients may add a `Range`
header for video/model seeking while also sending `capability.headers()`.
Local stages and custom stores without signing support return a typed error.
External presigned-URL consumers own their own integrity policy; minting a
capability does not imply that its eventual body was drained and verified.

For an SDK process with no cloud-storage credentials, use a Query-only
connection and read the `DataLocation` directly through the SDK.
`connect_query_only` does not discover a stage, construct an S3 client, or
permit local object I/O; each reader sends only one authenticated Flight action
to obtain a bounded capability, then streams bytes directly from object
storage. A production `lake query` installs this issuer only for its configured
S3 managed stage. Query scopes the requested `DataLocation` to
`tenants/<tenant-id>` before signing with its own AWS identity and never
proxies video/model bytes or exposes the URL/header values in logs, Arrow rows,
or metadata.

```rust
let client = LakeClient::builder("https://query.internal:50051")
    .with_bearer_token(std::fs::read_to_string("/run/secrets/query-token")?.trim())?
    .with_ca_certificate_pem(std::fs::read("/run/tls/ca.crt")?)
    .connect_query_only()
    .await?;
let mut object = client
    .open_via_query(&location, std::time::Duration::from_secs(300))
    .await?;
tokio::io::copy(&mut object, &mut tokio::io::sink()).await?;

let mut window = client
    .open_range_via_query(
        &location,
        8 * 1024 * 1024..9 * 1024 * 1024,
        std::time::Duration::from_secs(300),
    )
    .await?;
// Read only the requested 1 MiB window from `window`.
```

The full reader keeps constant memory and verifies declared size plus SHA-256
when it is drained to EOF. The range reader sends the exact HTTP `Range` header
and accepts only an exact matching `206 Content-Range` response; it cannot make
a whole-object SHA-256 claim. The SDK uses Rustls, does not follow redirects,
and keeps the signed URL and required headers internal. Advanced callers that
need to pass the capability to another HTTP client may still use
`presign_read_via_query`, but its URL and headers are bearer values and must not
be logged.

The example performs a multi-row insert, query, `DataLocation` decoding, and
direct open through `LakeClient`. Local development discovers a `file://`
stage; production discovers an `s3://` stage. Non-empty S3 objects stream through
bounded 64 MiB multipart parts, with incremental SHA-256 and abort-on-error.
An unknown-length input may contain at most 10,000 non-empty parts (roughly
625 GiB with full default parts); a 10,001st part returns a typed local error
before an invalid S3 request. This is a stream ceiling, not a claim to support
the full S3 maximum-object range.
The query service forwards only the Arrow row to the metadata leader;
video/model bytes travel directly between the SDK and the managed stage.
Tenant child-prefix authorization is enforced by Query, Metasrv, and the SDK;
presigned capabilities retain that same SDK-side boundary.

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

### DynamoDB prefix-layout migration

Cloud deployments use a companion table named
`$LAKE_DYNAMODB_TABLE_prefix_v2`. Its `(family#shard, full-key)` primary key
lets catalog and maintenance prefix reads use strongly consistent `Query`
requests instead of evaluating unrelated append-operation and manifest keys.
Every current binary dual-writes the legacy and prefix tables atomically.

After upgrading every commit-capable metadata node, run bounded backfill pages
until `complete` is true, then finalize with an explicit rollout acknowledgement:

```bash
lake dynamo-migrate --page-size 500 --json
lake dynamo-migrate --page-size 500 \
  --finalize --acknowledge-dual-rollout \
  --acknowledge-write-quiescence --json
```

The cursor is durable, so rerunning after a crash resumes safely. Before the
second command, pause metadata write admission. Finalization installs a durable
write barrier, checks exact key/value parity in both directions, and publishes
the authority marker while the barrier is still held. Restart Query and
Metasrv pods immediately afterward: refreshed nodes read v2, while the retained
barrier rejects stale pre-finalization writers. Reads may keep serving their
last-good cache during this short write-quiescent window. Retain the legacy
table for at least `LAKE_APPEND_OPERATION_RETENTION_SECS`; rollback after
finalization requires a dual-capable binary.

If exact verification reports an incomplete or divergent copy, the barrier is
left in place deliberately. Keep write admission paused, run bounded backfill
again, and retry finalization; do not manually remove the barrier while a
verifier or stale dual node may still be running.

For a local deployment, start metadata and query separately. The query process
is told where metadata lives; clients are not:

```bash
lake meta --addr 127.0.0.1:50052
lake query --addr 127.0.0.1:50051 --metadata-addr http://127.0.0.1:50052
```

In cloud mode, `lake meta` and local administrative commands derive table
datasets as `s3://$LAKE_S3_BUCKET/$LAKE_TABLE_PREFIX/<namespace>/<table>.lance`.
`LAKE_TABLE_PREFIX` defaults to the bucket root for compatibility. It is
process configuration, never a Flight DDL field.

Catalog and physical-manifest DynamoDB authority are deliberately separate.
`LAKE_DYNAMODB_TABLE` (default `lake_registry`) belongs to Metasrv;
`LAKE_MANIFEST_DYNAMODB_TABLE` (default `lake_manifests`) stores only Lance
external manifest pointers/history. The two names must differ. Served Query
constructs no catalog `MetaStore` or local Metasrv and does not provision
DynamoDB tables; its workload identity needs read access only to the manifest
table pair and no access to the registry pair. Metasrv needs registry writes
and manifest reads/writes. Existing shared-table deployments must ensure every
dataset has a fixed `lance-manifest-latest/` pointer, then copy and verify the
`lance-manifest/`, `lance-manifest-latest/`, and `lance-manifest-cleanup/`
families during a write-quiescent cutover. Query's read-only adapter never
installs a missing legacy pointer; there is intentionally no runtime fallback
to the registry table.

Plaintext anonymous serving is allowed only on loopback and receives the named
development principal. A non-loopback Query or Metasrv listener requires either
`LAKE_AUTH_PRINCIPALS_FILE` or `LAKE_AUTH_TOKEN_FILE`, plus
`LAKE_TLS_CERT_FILE` and `LAKE_TLS_KEY_FILE`. Set `LAKE_ALLOW_INSECURE=true` only
when a trusted service mesh terminates TLS before Lake; Lake bearer
authentication remains mandatory. Metasrv nodes use
`LAKE_PEER_AUTH_TOKEN_FILE`, `LAKE_PEER_CA_FILE`, and
`LAKE_PEER_SERVER_NAME` for follower-to-leader forwarding.

Every non-loopback Query also requires `LAKE_QUERY_TICKET_KEYS_FILE`, a
mode-`0600` JSON key ring shared by all replicas behind the Flight endpoint.
Statement handles are versioned AES-256-GCM ciphertext bound to their exact
principal, tenant, audience, issue time, and expiry; SQL is not present as
ticket plaintext. The active key seals new tickets and up to three verification
keys keep in-flight tickets valid during staged rotation. Loopback development
uses an ephemeral process-local key. `LAKE_QUERY_TICKET_TTL_SECS` defaults to
300 and is restricted to `1..=3600`; see the Kubernetes runbook for the
preload/activate/retire procedure.

Each Query replica also enforces finite admission limits. Defaults are 64
concurrent queries globally, 8 concurrent queries per authenticated tenant,
4096 tracked tenant gates, 100 ms total queue wait, 30 minutes execution time,
and 1 MiB of SQL/ticket text. Override them at process startup:

```bash
LAKE_QUERY_MAX_CONCURRENT=32 \
LAKE_QUERY_MAX_CONCURRENT_PER_TENANT=4 \
LAKE_QUERY_MAX_TRACKED_TENANTS=4096 \
LAKE_QUERY_QUEUE_TIMEOUT_MS=250 \
LAKE_QUERY_EXECUTION_TIMEOUT_MS=900000 \
LAKE_QUERY_MAX_SQL_BYTES=262144 \
LAKE_QUERY_TICKET_KEYS_FILE=/run/secrets/ticket-keys.json \
LAKE_QUERY_TICKET_TTL_SECS=300 \
lake query ...
```

Requests acquire their tenant gate before joining the global FIFO and use one
absolute queue deadline, so a noisy tenant cannot reserve global capacity while
waiting for its own share. Saturation returns gRPC `ResourceExhausted`;
execution expiry terminates the result stream with `DeadlineExceeded`. The
concurrency permit owns both gates for the full DoGet stream, so completion,
timeout, cancellation, or drop releases both. Inactive gates are weakly held
and synchronously pruned; a full active tracker fails closed instead of growing
memory. These are per-replica limits, not cluster-global quotas.

Lance maintenance preserves the ten most recent dataset versions by default.
Set `LAKE_LANCE_RETAIN_VERSIONS` to a value in `1..=10000` to choose a different
snapshot window for both local and S3 tables:

```bash
LAKE_LANCE_RETAIN_VERSIONS=100 lake meta ...
```

The value is parsed before RocksDB, DynamoDB, local paths, or S3 are opened, so
invalid configuration fails startup without partial storage initialization.
Lance tags and referenced branches remain protected even when they fall
outside the recent-version window; external manifest history is reclaimed only
after Lance confirms the corresponding manifest is obsolete.

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

## Kubernetes deployment

The repository includes a hardened multi-stage container and separate
Kubernetes references for the stateless Query tier and the bounded Metasrv
authority. They preserve authenticated TLS gRPC health, loopback-only metrics,
finite shutdown, non-root execution, explicit resources, disruption budgets,
and DynamoDB/S3 authority. See the
[Kubernetes deployment guide](docs/guides/kubernetes.md) before applying the
reference; image digests, cloud identity, certificates, and Secrets are
deployment inputs and are intentionally not embedded.

## Production telemetry

Query and Metasrv emit structured JSON logs and authenticated standard gRPC
Health. Set `LAKE_METRICS_ADDR` to a loopback IP socket to add Prometheus
metrics at `/metrics`; collect it with a localhost sidecar or node agent:

```bash
LAKE_METRICS_ADDR=127.0.0.1:9090 lake meta --addr 127.0.0.1:50052
```

Lake rejects non-loopback metrics listeners and never places SQL, tenant,
table, object URI, operation ID, or credentials in metric labels. See the
[CLI guide](docs/guides/cli.md#prometheus-metrics) for the exported series.
Dynamo-backed processes also expose their v1/v2 authority mode, finalization
barrier, and physical prefix evaluated/returned counts, so operators can prove
the prefix-layout rollout without exporting a logical key or prefix.

Set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` to opt into OTLP/gRPC traces. Lake
continues W3C context across SDK/Flight, Query-to-Metasrv, and follower-to-leader
hops using a bounded, process-owned exporter; workload identities and data are
not exported as span attributes. See the
[CLI tracing guide](docs/guides/cli.md#otlp-distributed-tracing).
