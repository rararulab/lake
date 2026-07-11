# Architecture

System design for lake. `goal.md` says why; this file says how. Agent
entry points (`AGENT.md`, `CLAUDE.md`) are catalogs — the substance lives
here.

## Design ethos

**Stateless fan-out in front, bounded stateful authority behind.** The read
flood (fleet nodes requesting episode data, DDoS-like) lands on a stateless
query layer that scales horizontally and reads storage directly. The
metadata authority — which tables exist, where, what version — is a small
stateful tier the query layer shields behind a cache. Compute and storage
are disaggregated: throughput scales by adding query nodes, not by growing a
central store.

## Three tiers

```
Fleet nodes / users            millions of reads, DDoS-like fan-out
    │  Flight SQL, load-balanced
    ▼
Query layer   (lake-query)     STATELESS — fan out freely
    - accept SQL, plan + execute via DataFusion
    - read data files directly from object storage (disaggregated storage)
    - cache the catalog (db→table→location+version) with TTL;
      serve list/resolve from cache, rarely touching the metadata tier
    │  cache miss / refresh / writes
    ▼
Metadata layer (lake-metasrv)  STATEFUL — bounded, leader-elected
    - authority for the db→table registry and current versions
    - serialize writes / durable idempotent commit coordination
    - background coordination (GC, compaction scheduling)
    │
    ▼
Metastore     (lake-meta)      HA KV: DynamoDB (prod) / RocksDB (dev)
    - registry pointers + compact operation records (durable, HA)
    ▼
Storage engine (lake-engine + lake-engine-lance)
    - per-table datasets on object storage (immutable, cacheable)
```

The query layer is both the compute fan-out and the cache shield. Because
the registry is small (~10⁴ tables) it fits in memory on every query node,
so catalog reads are served locally and the metadata authority sees only
cache-miss and write traffic. That is why the metadata tier being hard to
fan out is acceptable — it is not on the hot read path.

### Why the tiers scale differently

- **Query layer is stateless** — no durable state, no coordination. HA and
  scale come from running N replicas behind a load balancer. This is the
  tier that grows with read load.
- **Metadata layer is stateful** — writes must be serialized, background
  jobs need a single coordinator, and the in-memory authority must not
  diverge across replicas. So it is a small leader-elected group (leader +
  standby), durable state in the HA KV. It does not fan out freely; it does
  not need to, because the query cache absorbs the reads.

### Mapping to GreptimeDB

lake reuses GreptimeDB's frontend/metasrv split and drops its datanode tier:

| GreptimeDB | lake | property |
|---|---|---|
| `frontend` (stateless SQL, catalog cache) | `lake-query` | fan out freely |
| `metasrv` (leader-elected authority, DDL coordination) | `lake-metasrv` | stateful, bounded |
| `datanode` (owns region storage) | *none* | storage is shared object store; query reads it directly |

Adopted: the `KvBackend` trait shape (`src/common/meta/src/kv_backend.rs`),
the `KvBackendCatalogManager` + moka cache pattern
(`src/catalog/src/kvbackend/`), lease-in-KV leader election
(`src/common/meta/src/election/`). Rejected: etcd as the backend (we lean on
DynamoDB's managed HA instead), and the datanode tier (disaggregated
storage removes it).

## Storage engine abstraction

Lake must be able to swap Lance for a self-built engine, so no crate above
`lake-engine-lance` may name a `lance::` type — the same confinement rule
RocksDB has inside `lake-meta`. The engine trait exposes only what the
catalog and metadata layers call:

```rust
#[async_trait]
pub trait TableEngine: Send + Sync {
    async fn create(&self, loc: &TableLocation, schema: SchemaRef) -> Result<TableHandleRef>;
    async fn open(&self, loc: &TableLocation) -> Result<Option<TableHandleRef>>;
    async fn maintain(&self, loc: &TableLocation, version: Version) -> Result<Option<Version>>;
}

#[async_trait]
pub trait TableHandle: Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn current_version(&self) -> Version;
    /// A DataFusion table at a specific snapshot — this is how the query
    /// layer reads.
    async fn table_provider(&self, version: Version) -> Result<Arc<dyn TableProvider>>;
    /// Append one identified operation, producing one immutable version.
    async fn append(
        &self,
        operation: &AppendOperation,
        batches: SendableRecordBatchStream,
    ) -> Result<Version>;
    /// Discover an earlier commit after a lost response or failover.
    async fn reconcile_append(&self, operation: &AppendOperation) -> Result<Option<Version>>;
}
```

Lance-on-S3 commit arbitration stores one mutable current manifest pointer per
physical dataset plus immutable historical records. The current pointer is a
single O(1) metastore read. To advance it, the adapter archives the exact prior
pointer and CASes current from exact old bytes to the new staging manifest;
Lance has already durably written staging before that call. A legacy dataset
without the fixed pointer performs one history scan and CAS-installs the
maximum record. External history retention is separate from latest resolution:
cleanup may delete a record only after Lance's tag/branch-aware cleanup proves
the corresponding manifest object obsolete (#42).

Drop never returns the fixed key to an absent state: it CASes current to
`deleting`, clears immutable history, then CASes to a durable `deleted` marker.
Recreate replaces only `deleted`. This tombstone prevents a migration that
read legacy history before drop from winning an ABA `None` CAS afterward.

The fixed pointer is a commit-protocol boundary. A pre-pointer binary can write
a newer per-version record without advancing it, so commit-capable binaries on
both sides must not run concurrently. Deployments drain writes, upgrade every
metadata node that may lead, then resume. Dataset data itself needs no offline
migration; the first post-upgrade open installs its pointer lazily.

`Version` is an opaque engine-defined identifier; the registry stores it but
does not interpret it. The Lance impl (`lake-engine-lance`) maps `append`
and versioning onto Lance's own commit + `ExternalManifestStore` (which is
itself a put-if-not-exists KV — see below). A self-built engine implements
the same trait over its own format, using `lake-meta`'s CAS directly.

## Metadata: two levels, not one

There are three distinct pieces of metadata, owned by different layers:

1. **Registry** (lake's, in `lake-meta`): which tables exist and where —
   `tbl/<namespace>/<name> → { incarnation_id, location, current_version,
   engine, schema_ipc? }`. The incarnation changes on every successful create,
   so retained operation records cannot cross a drop/recreate boundary.
   The optional opaque Arrow IPC schema keeps old JSON readable while allowing
   Query to answer schema-inclusive Flight SQL discovery locally. Tiny (~10⁴
   entries), fully cacheable, the metadata layer is its authority.
2. **Operation coordination** (lake's, in `lake-meta`): compact CAS records
   keyed by tenant, table, and UUIDv7 operation identity. They contain only a
   payload digest, base/result versions, state, and timestamps. Arrow rows,
   object bytes, credentials, and signed URLs are forbidden.
3. **Drop coordination** (lake's, in `lake-meta`): immutable tombstones keyed
   `drop/<namespace>/<name>`. A tombstone retains the exact old incarnation and
   registration needed to conditionally detach the registry and resume
   idempotent engine cleanup after a crash.
4. **Per-table manifest** (the engine's): the file list, schema, and
   version history of one table. For Lance this is the Lance dataset
   manifest; lake does not reimplement it.

The registry is key/value prefix-scannable with pagination. Metasrv can list a
single `tbl/<namespace>/…` prefix, while each Query replica refreshes its whole
table/name/schema snapshot with one `tbl/` scan. Discovery reads that immutable
process-local generation and performs no request-path authority lookup.

### Server-authoritative table placement

Remote DDL carries only a table identifier and Arrow schema. The metadata
server derives `TableLocation` from a trusted `TablePlacement`: a local table
root in development, or an S3 bucket plus optional key prefix in production.
Namespace and table names must be safe single path segments before the engine
or registry is touched. Legacy location-bearing requests fail closed; the
server never ignores and never consumes a caller-selected URI.

The policy lives in `lake-metasrv`, above the engine boundary. Engines still
receive an ordinary storage-neutral `TableLocation`, so placement authority
does not couple the metadata or query tiers to Lance. Each HA replica must use
the same placement configuration; only the elected leader materializes a new
table.

## Commit protocol

Writes go through the metadata layer's leader to serialize per-table commits,
then delegate the data commit to the engine. One logical append is identified
by `(authenticated tenant, table, UUIDv7 operation ID)` and a verified SHA-256
digest of its ordered Flight control payload:

1. The SDK uploads object bytes directly to managed storage, encodes only
   `DataLocation` rows, and generates the operation ID once. Ambiguous Flight
   failures reuse the same encoded messages, identity, and digest for a
   30-second bounded window, longer than the 10-second metadata lease. If that
   window expires ambiguously, the error returns a `PendingAppend`; callers can
   resume it throughout operation retention with the same identity and without
   uploading the object again.
2. Metasrv authenticates the tenant, verifies the digest, claims a durable
   per-table fence, and CAS-creates a compact `reserved` operation record.
3. The engine writes the new immutable version. Lance disables automatic
   append rebase and stores tenant, operation ID, digest, and reference-stage
   identity in transaction properties. Object-reference chunks are staged
   before the manifest is visible.
   A freshly reserved operation takes a no-eager-scan engine path; full
   transaction history is consulted only for replay/recovery or commit
   collision, not for every healthy append.
4. Metasrv records `engine_committed`, CAS-advances the registry pointer only
   after reference lineage is complete, and records the terminal version.
5. A replay or replacement leader loads the durable record and reconciles
   Lance transaction history. An identical replay returns the original
   version; a changed digest conflicts; corrupt or missing recovery evidence
   fails closed.

Terminal coordination records have a configurable retention horizon (seven
days by default). Leader-only cleanup scans bounded metastore pages; pending
records are reconciled before deletion. IDs older than retention fail closed,
and timestamps more than five minutes in the future are rejected. A FILE
Flight control stream is capped at 64 MiB because multi-GB video/model bytes
belong in object storage, not in Query or Metasrv memory.

Readers (through the query layer's cache) never observe a half-written
version: the pointer only ever advances to a fully-written one. Consistency
is snapshot-by-version with at-most-one-commit staleness on cache-served
reads — acceptable for training/eval, see `goal.md`.

## SQL over object storage

The public query protocol keeps arbitrary SQL execution read-only. Query nodes
resolve the exact registry version and stream its files directly from S3; SQL
text cannot register arbitrary object-store locations. The one typed write
surface is a Flight `DoPut` command for already-uploaded SQL `FILE` rows:
the SDK sends `DataLocation` Arrow values to query, query proxies the stream
without persisting it, and the metadata leader performs the idempotent append
protocol above. Query forwards tenant scope derived from its authenticated
principal; a caller-supplied tenant string is never trusted.
The original object bytes never enter query or metadata.

After a query node receives the metadata leader's append acknowledgement, it
evicts that table's local registration entry. The same SDK Flight connection
therefore observes its own write immediately; independent query nodes retain
the normal bounded-staleness window until their cache refreshes.

Interactive results stream over `DoGet`. The planned large-result tier
materializes Arrow/Parquet parts to a service-owned S3 prefix and publishes
short-lived HTTPS locations through `PollFlightInfo`. The complete API and
security boundary are in
[`docs/design/sql-api-over-s3.md`](design/sql-api-over-s3.md).

## Crate map

| Crate | Owns | Tier |
|-------|------|------|
| `lake-common` | shared newtypes: `Namespace`, `TableName`, `Version`, `TableLocation` | — |
| `lake-flight` | shared Flight TLS, bearer authentication, exposure policy, and secure Channel construction | transport |
| `lake-objects` | SQL `FILE` physical representation (`DataLocation`), Arrow encoding, direct object I/O | storage |
| `lake-meta` | `MetaStore` (KvBackend) trait; `RocksMeta` (dev), `DynamoMeta` (prod) | metastore |
| `lake-engine` | `TableEngine` / `TableHandle` traits + shared types | storage |
| `lake-engine-lance` | Lance impl and `ExternalManifestStore` adapter; the ONLY crate that names `lance::` | storage |
| `lake-catalog` | db→table registry logic, DataFusion `CatalogProvider`, moka cache | query + metadata |
| `lake-query` | stateless query-layer server (Flight SQL, DataFusion execution) | query |
| `lake-metasrv` | stateful metadata-layer server (registry and table-placement authority, write coordination, leader election) | metadata |
| `lake-cli` | thin `clap` binary: subcommands to run each server + client | — |
| `lake-sdk` | Rust streaming SQL query, parameterized `FILE` INSERT, `DataLocation` decoding, and direct reader | client |

Conventions: **thin libs** (`lib.rs` is module docs + re-exports; logic in
sub-files), **async-first** (engine, metastore, catalog, servers are async;
sync bridges only at framework boundaries, each with a `ponytail:` note),
per-crate snafu errors (`{CrateName}Error` + `Result<T>`), `anyhow` only in
`lake-cli`. Each crate carries an `AGENT.md` card. Workspace lints/deps live
in the root `Cargo.toml`.

`schema` is not a crate: it is Arrow `SchemaRef`, owned by the engine and
surfaced through `lake-catalog`.

## HA

- **Query layer**: stateless → N replicas behind a load balancer.
- **Metadata layer**: leader + standby; leader elected via a lease in the
  HA KV (GreptimeDB's `election` pattern). Durable state lives in the KV, so
  a failed leader loses no data — a standby takes the lease and resumes. The
  lease record carries a monotonic fencing epoch. `MetaStore::guarded_mutate`
  atomically checks the exact lease record together with an exact target
  create, update, or delete: RocksDB uses one writer critical section and
  write batch; DynamoDB uses one `TransactWriteItems` request. Backends without
  a native atomic implementation fail closed.
- **Metastore**: DynamoDB is multi-AZ HA by construction; RocksDB is
  single-node, dev only.

No self-built consensus: read HA comes from stateless replicas, write HA
from lease-election over an already-HA KV.

Production Metasrv wraps its raw metastore in a lease-fenced view after
election starts. Each registry, append-operation, and maintenance CAS/delete
loads the latest exact lease bytes immediately before publication and executes
through `guarded_mutate`; election renew/resign continues on the raw store.
Within one process, the metastore transaction takes a shared publication
barrier while renewal/resign holds its exclusive side from durable lease CAS
through local guard publication. This closes the exact-bytes rotation window
without holding the barrier across long engine work. Long engine operations
can therefore span same-holder renewals, while a takeover changes the guard
and rejects a paused former leader. If the old leader already committed an
engine version, the successor reconciles that immutable commit before
publishing it.

Destructive table drop is a persisted idempotent procedure because object
deletion cannot share the KV transaction. Metasrv first guarded-CAS creates an
immutable incarnation tombstone, guarded-deletes the exact registry pointer,
idempotently removes the old engine location, and finally guarded-deletes the
exact tombstone. Leader maintenance resumes a cursor-paged bounded set of
unfinished tombstones. Remote creates use
`<root>/<namespace>/<table>/<uuid>.lance` (or the equivalent S3 prefix), so an
old leader that continues object deletion after takeover can touch only the old
physical generation. A replacement incarnation therefore remains safe even
though object-store deletion itself is not lease-transactional.

## Deliberate simplifications (ponytail markers)

Grep for `ponytail:` in code for shortcuts with known ceilings. Current
design-level ones:

- Both servers speak real Arrow Flight: `lake-query` a streaming Flight SQL
  read endpoint plus a typed metadata-only FILE `DoPut` proxy, and
  `lake-metasrv` a Flight control plane accepting DDL actions and leader-aware
  FILE append streams.
  `lake-metasrv::serve` runs deadline-aware lease election, forwards follower
  writes to the observed leader, serializes mutations per table, and gates
  maintenance on the same lease. Ceiling: there is no durable query scheduler
  or asynchronous large-result service yet.
- Every production Flight hop shares `lake-flight`: a server interceptor
  authenticates every RPC, TLS configuration is verified by the client, and
  non-loopback plaintext/anonymous listeners fail closed. This covers
  SDK→Query, Query→Metasrv, and follower→leader. A protected immutable
  principal map binds opaque credentials to validated tenant, role, and finite
  namespace grants. Query rejects unauthorized SQL before planning and filters
  discovery from its local catalog snapshot. Metasrv independently checks every
  table action and FILE append; only authenticated Query/metadata-peer roles may
  preserve an exact authorized namespace across internal hops. Loopback
  development installs an explicit development principal rather than treating a
  missing identity as authority.
- Managed-stage discovery derives `tenants/<tenant-id>` below the configured
  local root or S3 prefix. The SDK opens that child stage directly and rejects a
  `DataLocation` outside it. Production workload IAM must independently restrict
  each SDK identity to the same S3 child prefix; Lake never sends AWS credentials
  or large-object bytes through Query or Metasrv.
- The full prod path (Lance on S3 + DynamoDB commit pointer via
  `ExternalManifestStore`) is wired end to end: `LanceEngine::for_object_store`
  threads S3 storage options + the commit handler through create/open/append,
  and `lake-cli` selects it via `LAKE_S3_BUCKET` (+ `LAKE_DYNAMODB_*` / `AWS_*`
  / `LAKE_S3_PROXY_EXCLUDES`). Verified against localstack both directly
  (`tests/s3_lance_localstack.rs`, `#[ignore]`) and through `lake selftest` in
  cloud mode.
- No client-side SDK cache yet — the query layer caches, the SDK does not.
  Add SDK-side catalog caching when fleet-node QPS demands another tier.
- Each stateless Query replica has finite process-local admission: bounded
  concurrent planning/execution, bounded queue wait, a stream-held execution
  deadline, and pre-planning SQL/ticket size checks. This protects one replica
  without adding metadata traffic. Tenant quotas, fair queuing, and distributed
  admission remain production policy work.
- Query's DataFusion runtime is also process-bounded: all concurrent operators
  share one fair execution-memory pool and one aggregate size-limited spill
  manager under an operator-owned local directory. Spill is ephemeral replica
  state and is deleted with its DataFusion runtime; it never becomes table or
  query-result durability.
- Query and Metasrv expose injected shutdown futures; the CLI maps SIGINT and
  SIGTERM into them. Tonic stops accepting connections and drains existing
  Flight RPCs within a finite deployment-configured grace period. Query joins
  catalog refresh before return. Metasrv stops maintenance immediately, keeps
  renewing leadership while accepted writes drain, drops the server on clean
  completion or timeout, and only then resigns and joins the campaign. This
  ordering prevents an accepted write from committing after the node has
  released authority. Drain timeout is a typed process error, not a clean exit.
- Managed large objects have a query-connected Rust SDK vertical slice:
  `INSERT ... VALUES (?, ?)` binds `InsertValue::File(FileUpload)`, streams it
  into either a local or S3 Lake-owned managed stage, and stores an immutable
  `DataLocation` physical representation in Lance. S3 uses bounded multipart
  upload and exact bucket/prefix validation for direct reads. SDK-configured
  local checkpoints make path-backed multipart writes resumable across process
  restarts: source/stage identity and per-part hashes are revalidated against
  paginated S3 state before only the missing suffix is sent. Sequential and
  half-open byte-range readers share the same SDK/storage
  boundary; local ranges seek + limit and S3 ranges issue one bounded GET. The
  SDK receives only the query endpoint, discovers a credential-free managed
  stage descriptor once, and constructs local/S3 access using process
  credentials; query forwards metadata to the leader-aware metasrv. Browser
  presigning, authenticated expiring locations, and codec indexes are
  deferred.
- Managed-object reachability is incremental rather than a table-row scan.
  Every version-producing Lance commit publishes a canonical chunked
  reference edge before the registry pointer can advance. The separate
  `lake gc` worker pages those edges and storage inventory, externally merges
  live URIs with bounded memory/open files, age-gates candidates, and publishes
  an immutable content-addressed plan only after full validation. Explicit
  apply verifies the stage and registry-root fingerprint before each bounded
  page and fsyncs a resumable checkpoint. Missing lineage, registry movement,
  stale/corrupt plans, prefix escapes, and future removal deltas all fail
  closed. The operational safety horizon must exceed upload-to-commit retries;
  apply runs in a write-quiescent window.

## Phasing

- **v0 (core)** ✅ — `lake-common` + `lake-meta` (RocksMeta) + `lake-engine`
  + `lake-engine-lance` + `lake-catalog`, exercised by `lake-cli`'s e2e
  self-check (create → ingest → SQL). All tiers present as libraries.
- **v1 (wires + prod backend)** ✅ — `DynamoMeta` (prod HA KV), the Lance
  `ExternalManifestStore` adapter for S3 commits, the `lake-query` Arrow
  Flight SQL server, and the `lake-metasrv` Flight `do_action` control plane.
- **v2 (metadata HA + ops)** — lease-election, follower forwarding,
  leadership-gated writes, per-table serialization, and leader-only
  maintenance are wired. Remaining: durable operation state, production
  observability, and client-side SDK caching. A self-built engine slots in
  behind `TableEngine` if/when Lance's ceiling is hit.

Invariant across all phases: fleet reads go through the stateless query
layer, never directly at the metadata authority.
