# Meta-server design

> **Status.** `docs/architecture.md` is authoritative for the tier layout.
> lake is a **three-tier** system — stateless `lake-query` fan-out in front
> of a bounded stateful `lake-metasrv` authority over an HA KV. The earlier
> "no server / library mode is enough" conclusion in the later sections of
> this file is **superseded**: a real metadata authority is first-class
> (v1), and metadata HA via lease-election is on the roadmap (v2). What
> remains authoritative here is **section 1** — the GreptimeDB metasrv
> study — and the deep-dive on which metasrv pieces (election, procedures,
> meta-client caching) lake lifts and when. Read this as the metasrv
> reference behind `architecture.md`, not as a competing design.

Direction for lake's metadata layer ("metasrv"), derived from a study of
GreptimeDB's metasrv. `goal.md` says why lake exists; `docs/architecture.md`
says how the three tiers fit together; this file is the GreptimeDB-grounded
reference for what the metadata layer lifts from metasrv, and when.

GreptimeDB paths below are relative to the greptimedb repo root
(studied at `src/meta-srv/`, `src/meta-client/`, `src/common/meta/`).

## 1. What GreptimeDB metasrv does

GreptimeDB runs a dedicated metadata service between all nodes and the
backing KV store. Its pieces:

- **KvBackend abstraction.** `src/common/meta/src/kv_backend.rs`
  defines `KvBackend: TxnService` with `range`, `put`, `batch_put`,
  `batch_get`, `delete_range`, `batch_delete`, plus default-impl
  `get`, `compare_and_put`, `exists`, `delete`. The `TxnService`
  supertrait (`src/common/meta/src/kv_backend/txn.rs`) adds an
  etcd-style conditional transaction: `when(compares)` /
  `and_then(ops)` / `or_else(ops)`. Backends: etcd
  (`kv_backend/etcd.rs`), memory (`kv_backend/memory.rs`),
  Postgres/MySQL (`kv_backend/rds/`), and a raft-engine-backed local
  store for standalone mode (`src/log-store/src/raft_engine/backend.rs`).
- **Election.** `src/common/meta/src/election.rs` defines an
  `Election` trait (`campaign`, `leader`, `resign`,
  `subscribe_leader_change`); impls use etcd leases
  (`election/etcd.rs`) or an RDS lease row with expiry timestamps
  updated by SQL CAS (`election/rds/postgres.rs`, `rds/mysql.rs`).
  Mutating RPCs are leader-only (`src/meta-srv/src/state.rs`,
  `src/meta-srv/src/service/procedure.rs`); lease TTL is 5s with 2.5s
  keep-alive (`src/common/meta/src/distributed_time_constants.rs`).
- **Heartbeat + registry.** Datanodes stream heartbeats carrying
  per-region load stats (`src/common/meta/src/datanode.rs`) through a
  handler chain (`src/meta-srv/src/handler/` — lease keeping, stat
  collection, region lease renewal, failure detection). Liveness keys
  live in metasrv's *in-memory* KV, not the durable backend
  (`handler/keep_lease_handler.rs`). Heartbeat responses double as a
  mailbox pushing instructions (cache invalidation, region migration)
  back to nodes (`src/common/meta/src/heartbeat/mailbox.rs`).
- **DDL procedures.** `src/common/procedure/src/procedure.rs` is a
  persisted state-machine framework: idempotent `execute` steps,
  `Status::Executing { persist }`, rollback, poison keys; state is
  persisted through any KvBackend (`src/common/meta/src/state_store.rs`)
  so a new leader resumes half-done DDL. Concrete multi-step DDLs
  (create/drop/alter table across datanodes) live in
  `src/common/meta/src/ddl/`, orchestrated by
  `src/common/meta/src/ddl_manager.rs`.
- **meta-client.** `src/meta-client/src/client/` bundles heartbeat,
  KV, and procedure sub-clients; `ask_leader.rs` discovers and caches
  the leader, retrying on "not leader" responses. Frontends wrap the
  remote KV in a moka `CachedKvBackend`
  (`src/catalog/src/kvbackend/client.rs`) invalidated by heartbeat
  mailbox messages (`src/meta-srv/src/cache_invalidator.rs`).
- **Standalone mode bypasses all of it.** `src/cmd/src/standalone.rs`
  wires a local `RaftEngineBackend` directly into the same
  `TableMetadataManager` / procedure manager / catalog stack
  (`src/standalone/src/metadata.rs`) — no metasrv process, no
  meta-client. The entire distributed apparatus is an optional layer
  over `KvBackend`.

## 2. What lake actually needs

Gate each capability against `goal.md`:

| GreptimeDB capability | Why greptime needs it | Does lake? |
|---|---|---|
| KV abstraction w/ CAS | pluggable etcd/RDS/local | **Yes** — already have it (`MetaStore`) |
| Rich KV surface (txn, batch, range-delete) | many mutable keys per table (regions, routes) | **No** — one mutable key per table; CAS is the whole model ("NOT a transaction engine") |
| Election | exactly-one procedure runner / region balancer | **Not yet** — no singleton role exists until background GC does |
| Heartbeat + region leases | datanodes *host* data; failover must move regions | **No** — lake nodes host nothing; data lives in object storage. A dead writer loses a CAS race and nothing else |
| Mailbox / push cache invalidation | mutable metadata cached on frontends | **No** — lake caches only immutable manifests; the one mutable pointer is TTL-polled, O(1) per version change |
| Procedure framework | multi-step DDL touching many datanodes | **Later, small** — drop-table + data-file GC is the first real multi-step op |
| meta-client (leader discovery, retries) | all nodes talk to metasrv | **No for readers, ever** — the read path must not gain a service hop |

The structural difference: GreptimeDB's metasrv scales with *cluster
metadata churn* because datanodes are stateful and regions move. Lake
has no regions and no stateful nodes; the only coordination point is
the per-table version pointer, and its commit protocol already works
from any client. Readers must be served from immutable cached
artifacts — a meta-server on the read path is disqualified by
`goal.md` ("NOT a metadata service"), not merely deferred.

So the question is confined to the **write/control plane**: commits,
DDL (create/drop table), and eventually garbage collection of
unreferenced data files.

## 3. Proposed design

### 3.1 Keep `MetaStore` as the KvBackend analog — stay narrow

`crates/lake-meta/src/store.rs` (`get` / `cas` / `list_prefix`) is the
lake analog of `KvBackend`, deliberately ~10% of greptime's surface.
Both hide backend types behind one trait and treat conditional-put as
the concurrency primitive (greptime's `compare_and_put`; DynamoDB
conditional put for lake). Differences we keep on purpose:

- **No blind `put`.** CAS stays the only mutation (lake-meta
  invariant). Greptime needs `put`/`batch_put` for high-churn stat and
  route keys; lake has none.
- **No `Txn`.** Greptime's etcd-style multi-op transaction exists to
  update several metadata keys atomically. Lake's atomic unit is one
  pointer; multi-step operations get idempotent-step orchestration
  (§3.3), not multi-key transactions.

Extensions, added only when a consumer lands:

- **`cas_delete(key, expected) -> bool`** — drop-table needs to remove
  a pointer; a conditional delete preserves the CAS-only invariant.
  (DynamoDB: `DeleteItem` + `ConditionExpression`; RocksDB: same mutex.)
- **`batch_get(keys)`** — catalog listing N tables currently costs N
  gets; add when table counts make it measurable. Maps to DynamoDB
  `BatchGetItem`.
- **Lease/TTL: no trait change.** Encode leases as values —
  `{holder, expires_at}` — renewed and stolen via `cas`, exactly the
  scheme greptime's RDS election uses when no etcd lease primitive
  exists (`src/common/meta/src/election/rds/postgres.rs`). This gives
  us election later without touching the trait or requiring etcd.
- **No `watch`.** Etcd watch powers greptime's push invalidation; lake
  readers TTL-poll one tiny pointer. DynamoDB has no watch, and adding
  Streams plumbing to save a sub-second of staleness buys nothing.

### 3.2 v0 is library-mode: no meta-server process

GreptimeDB itself proves the layering: standalone mode runs the full
catalog + DDL stack directly on a local `KvBackend` with no metasrv
and no meta-client (`src/cmd/src/standalone.rs`). Lake starts — and
stays as long as possible — in the equivalent posture, including in
production: writers embed `lake-meta`/`lake-manifest` and CAS-commit
straight to DynamoDB. DynamoDB is already a managed, replicated,
serialized CAS arbiter; putting a single-writer gRPC service in front
of it would *reduce* availability and add an op to run, while
providing serialization we already have.

What library-mode handles fine:

- Concurrent commits from many writers (CAS losers retry — the
  designed behavior).
- Table create (CAS pointer from `None`) and simple drops.
- Readers: unaffected by any of this, by construction.

### 3.3 What eventually forces a control-plane component

Not commit throughput and not reader count — those never route through
metadata. The real triggers are **multi-step operations that outlive a
client process**:

1. **Drop table with data-file GC.** Deleting the pointer is one CAS;
   deleting orphaned manifests/Parquet must survive the client dying
   mid-way. This needs greptime's *idea* — persisted, idempotent,
   resumable steps (`src/common/procedure/src/procedure.rs`) — at
   perhaps 1/20 the machinery: a `proc/<id>` key in the metastore
   holding `{op, step, args}`, advanced by CAS, executed by whoever
   holds the janitor lease. We adapt the pattern (idempotent
   `execute`, persist-between-steps, resume-on-takeover) and skip the
   framework (loaders, poison store, rollback DAGs) until more than
   one procedure type exists.
2. **Orphan sweep / compaction coordination.** A background janitor
   that must run as exactly-one instance. This is where election
   appears — as the KV-lease scheme from §3.1, a few dozen lines over
   `cas`, not an etcd dependency and not a new process: the janitor is
   a role a writer node (or a cron job) claims, greptime-RDS-style.

Only if lake later gains multi-tenant writers needing central policy,
auth brokering, or audit does a standalone `lake-metasrv` *process*
earn its keep. If that day comes, the greptime pieces worth lifting
are the shape of `ask_leader` client failover
(`src/meta-client/src/client/ask_leader.rs`) and leader-only mutation
gating (`src/meta-srv/src/state.rs`) — over our existing KV-lease
election, serving writers only. The read path never learns it exists.

## 4. Phasing

- **v0 (now):** library-mode. `MetaStore` unchanged; DynamoDB impl
  (conditional put = CAS) is the only missing piece and is already on
  the roadmap. No server, no election, no heartbeat.
- **v1 — trigger: drop-table with GC, or first orphaned-file sweep.**
  Add `cas_delete` to `MetaStore`; add a `proc/<id>` persisted-step
  record and a janitor role claimed via KV lease (`lease/janitor` key,
  `{holder, expires_at}` value, CAS renew/steal). Janitor runs inside
  an existing writer or as a scheduled job — still no long-running
  service.
- **v2 — trigger: multi-tenant writers needing centralized authz/
  audit/quota, or >1 procedure type making ad-hoc step records
  unwieldy.** Extract the janitor + procedure runner into a
  `lake-metasrv` process: gRPC for writer-side DDL, leader election on
  the same KV lease, `ask_leader`-style client retry. Explicit
  non-goal at every phase: readers never connect to it.

## 5. Rejected alternatives

- **Standalone meta-server from day one (greptime's default
  topology).** Adds a process, an availability dependency, and a
  network hop to gain serialization DynamoDB already provides.
  Greptime needs the process because it manages region placement and
  node liveness; lake has neither.
- **Adopting `KvBackend`'s full surface (txn, batch ops, range
  deletes) into `MetaStore`.** Unused surface invites storing more
  than pointers in the metastore, which `goal.md` explicitly forbids.
  Grow the trait per consumer, not per precedent.
- **etcd as the metadata backend.** Would hand us leases, watch, and
  txn for free — and a quorum cluster to operate, for a workload of
  one tiny key per table. DynamoDB's conditional put covers the actual
  requirement with zero ops burden.
- **Heartbeat/registry for readers or writers.** Reader liveness is
  irrelevant (stateless cache-and-scan). Writer liveness matters only
  to the janitor lease, which the lease value's expiry already
  encodes. A heartbeat mailbox exists to invalidate mutable caches and
  move regions; lake has neither.
- **Push cache invalidation (watch / streams) for the version
  pointer.** TTL polling already gives O(1) KV load per version change
  and bounded staleness; push machinery would re-couple readers to a
  live service, which is the exact failure mode `goal.md` rules out.
- **Lifting `common-procedure` wholesale.** ~Framework-sized solution
  (loaders, poison stores, rollback) for what is initially one
  procedure (drop + GC). Adapt the persisted-idempotent-step pattern;
  revisit the framework if procedure types multiply (the v2 trigger).
