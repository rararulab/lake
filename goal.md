# lake — North Star

## What lake is

lake is a lakehouse for embodied-AI data, in the spirit of LanceDB. Robot
fleets write episode data (images, video, pointclouds, sensor streams) as
immutable files; large batches of training/eval nodes read it back
concurrently through SQL. Scale target: ~10⁴ tables holding ~10¹¹ episodes
total (many episodes per table), under DDoS-like read fan-out.

The product loop is **ingest -> inspect -> select -> freeze -> train ->
derive**. Lake owns dataset membership, exact revisions, access, retention, and
training provenance. Format-aware tools such as Rerun provide episode-local
visualization, timeline alignment, decoding, and sampling without becoming a
second catalog authority. A logical episode is independent of physical file and
shard boundaries; RRD, MCAP, and training layouts such as LeRobot are adapters,
not the core data model. See
[`docs/design/robot-training-lakehouse.md`](docs/design/robot-training-lakehouse.md).

lake is organized as three tiers with disaggregated compute and storage:

- **Query layer** — truly stateless SQL compute (DataFusion). Fleet nodes
  fan out onto it; it reads data files directly from object storage and
  caches catalog info. This is the tier that absorbs the read flood.
- **Metadata layer** — the stateful catalog authority: which tables exist,
  where they live, their current version, and write coordination. Bounded,
  leader-elected replication — NOT freely fan-out.
- **Storage** — per-table datasets on object storage via a pluggable
  storage engine (Lance is the default; a self-built engine is a
  first-class future).

The bet: **put a stateless query layer in front of a bounded stateful
metadata authority.** The query layer fans out with load and shields the
metadata tier behind a cache, so the authority sees only cache-miss and
write traffic. Compute and storage are separate, so read throughput scales
by adding query nodes, not by growing a central store.

## What lake is NOT

- **NOT a general-purpose data warehouse.** The workload is embodied-AI
  training/eval reads: huge scans, few point lookups, bursty fan-out.
  We do not optimize for BI dashboards or OLTP.
- **NOT a MySQL clone.** The SQL dialect is DataFusion's; the wire protocol
  is Arrow Flight SQL. We will not implement the MySQL wire protocol.
- **NOT a design where reader count hits the metadata authority
  directly.** Fleet nodes talk to the stateless query layer; if a design
  puts per-query load on the metadata tier proportional to reader count,
  it is wrong — the query layer must shield it via cache.
- **NOT locked to one storage engine.** Lance is the default and confined
  to a single crate. Everything above programs against the engine trait so
  a self-built engine can replace it.
- **NOT a cross-table transaction engine.** Versioning and commit are
  per-table (per dataset). No cross-table transactions, no MVCC beyond
  snapshot-by-version. Losers of a commit race retry.
- **NOT a storage-node system.** Storage is disaggregated object storage;
  the query layer reads it directly. There is no datanode tier.
- **NOT a model-training orchestrator or a Rerun Hub clone.** Lake makes
  robot-training data inspectable, reproducible, and directly readable. Model
  execution, annotation applications, and general workflow scheduling remain
  external systems.

## What working lake looks like

- A fleet of N reader nodes fans out onto the query layer; the metadata
  authority sees ~O(cache-miss) traffic, not O(N) per query.
- A writer commits a new table version while readers stream the old one;
  no reader ever observes a half-written snapshot.
- A new user lists the tables in a namespace and runs
  `SELECT ... FROM <db>.<table>` with zero schema setup.
- The metadata leader fails; a standby takes over from HA-KV-durable state;
  the query layer keeps serving reads from cache throughout.
- The storage engine is swapped from Lance to a self-built engine without
  the query or metadata layers changing.
- `mise run e2e` proves the whole path end-to-end (ingest → commit → SQL)
  in one command, on a laptop, with RocksDB.
