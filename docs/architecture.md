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
    - serialize writes / commit coordination
    - background coordination (GC, compaction scheduling)
    │
    ▼
Metastore     (lake-meta)      HA KV: DynamoDB (prod) / RocksDB (dev)
    - registry entries + version pointers (durable, HA)
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
    /// Append rows, producing a new immutable version.
    async fn append(&self, batches: SendableRecordBatchStream) -> Result<Version>;
}
```

`Version` is an opaque engine-defined identifier; the registry stores it but
does not interpret it. The Lance impl (`lake-engine-lance`) maps `append`
and versioning onto Lance's own commit + `ExternalManifestStore` (which is
itself a put-if-not-exists KV — see below). A self-built engine implements
the same trait over its own format, using `lake-meta`'s CAS directly.

## Metadata: two levels, not one

There are two distinct pieces of metadata, owned by different layers:

1. **Registry** (lake's, in `lake-meta`): which tables exist and where —
   `tbl/<namespace>/<name> → { location, current_version, engine }`. Tiny
   (~10⁴ entries), fully cacheable, the metadata layer is its authority.
2. **Per-table manifest** (the engine's): the file list, schema, and
   version history of one table. For Lance this is the Lance dataset
   manifest; lake does not reimplement it.

The registry is prefix-scannable (`tbl/<namespace>/…`) with pagination so
"list tables" never loads everything, even though at 10⁴ tables it would
fit in memory anyway — the discipline matters if table count grows.

## Commit protocol

Writes go through the metadata layer's leader to serialize per-table
commits, then delegate the data commit to the engine:

1. Writer submits an append for `<namespace>/<name>` to the metadata leader.
2. Engine writes the new immutable version (Lance: stage manifest under a
   UUID → put-if-not-exists to the external store → finalize). This is the
   manifest-first-then-pointer discipline, implemented by the engine.
3. Metadata layer CAS-updates the registry's `current_version` pointer.
   Losers of the race fail cleanly and retry.

Readers (through the query layer's cache) never observe a half-written
version: the pointer only ever advances to a fully-written one. Consistency
is snapshot-by-version with at-most-one-commit staleness on cache-served
reads — acceptable for training/eval, see `goal.md`.

## SQL over object storage

The public query protocol is read-only Arrow Flight SQL. Query nodes resolve
the exact registry version and stream its files directly from S3; SQL text
cannot register arbitrary object-store locations. Interactive results stream
over `DoGet`. The planned large-result tier materializes Arrow/Parquet parts to
a service-owned S3 prefix and publishes short-lived HTTPS locations through
`PollFlightInfo`. The complete API and security boundary are in
[`docs/design/sql-api-over-s3.md`](design/sql-api-over-s3.md).

## Crate map

| Crate | Owns | Tier |
|-------|------|------|
| `lake-common` | shared newtypes: `Namespace`, `TableName`, `Version`, `TableLocation` | — |
| `lake-objects` | SQL `FILE` physical representation (`DataLocation`), Arrow encoding, direct object I/O | storage |
| `lake-meta` | `MetaStore` (KvBackend) trait; `RocksMeta` (dev), `DynamoMeta` (prod); Lance `ExternalManifestStore` adapter | metastore |
| `lake-engine` | `TableEngine` / `TableHandle` traits + shared types | storage |
| `lake-engine-lance` | Lance impl; the ONLY crate that names `lance::` | storage |
| `lake-catalog` | db→table registry logic, DataFusion `CatalogProvider`, moka cache | query + metadata |
| `lake-query` | stateless query-layer server (Flight SQL, DataFusion execution) | query |
| `lake-metasrv` | stateful metadata-layer server (registry authority, write coordination, leader election) | metadata |
| `lake-cli` | thin `clap` binary: subcommands to run each server + client | — |
| `lake-sdk` | Rust parameterized `FILE` INSERT binding and direct `DataLocation` reader | client |

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
  a failed leader loses no data — a standby takes the lease and resumes.
- **Metastore**: DynamoDB is multi-AZ HA by construction; RocksDB is
  single-node, dev only.

No self-built consensus: read HA comes from stateless replicas, write HA
from lease-election over an already-HA KV.

## Deliberate simplifications (ponytail markers)

Grep for `ponytail:` in code for shortcuts with known ceilings. Current
design-level ones:

- Both servers speak real Arrow Flight: `lake-query` a read-only, streaming
  Flight SQL endpoint, and `lake-metasrv` a Flight `do_action` control plane.
  `lake-metasrv::serve` runs deadline-aware lease election, forwards follower
  writes to the observed leader, serializes mutations per table, and gates
  maintenance on the same lease. Ceiling: there is no durable query scheduler
  or asynchronous large-result service yet.
- The full prod path (Lance on S3 + DynamoDB commit pointer via
  `ExternalManifestStore`) is wired end to end: `LanceEngine::for_object_store`
  threads S3 storage options + the commit handler through create/open/append,
  and `lake-cli` selects it via `LAKE_S3_BUCKET` (+ `LAKE_DYNAMODB_*` / `AWS_*`
  / `LAKE_S3_PROXY_EXCLUDES`). Verified against localstack both directly
  (`tests/s3_lance_localstack.rs`, `#[ignore]`) and through `lake selftest` in
  cloud mode.
- No client-side SDK cache yet — the query layer caches, the SDK does not.
  Add SDK-side catalog caching when fleet-node QPS demands another tier.
- Managed large objects currently have a local Rust SDK vertical slice:
  `INSERT ... VALUES (?, ?)` binds `InsertValue::File(FileUpload)`, streams it
  into a Lake-owned managed stage, and stores an immutable `DataLocation`
  physical representation in Lance. Remote Flight writes, S3 multipart
  presigning, authenticated expiring locations, and object GC are deliberately
  deferred.

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
