# lake-metasrv

The metadata layer: the stateful registry authority. `Metasrv` owns the
write path — create tables, resolve/list, and the append commit protocol.

## Invariants

- **Stateful, bounded.** Not a fan-out tier — the query layer shields it via
  cache, so it sees only cache-miss and write traffic. HA is leader + standby
  (lease-in-KV election), not free replication. (Election is v2.)
- Writes are per-table serialized and durably fenced. Identical operation
  replays converge on one engine version; changed digests conflict.
- Registry publication follows complete engine manifest/reference lineage;
  ambiguous results and leader failover reconcile from HA-KV plus history.
- Never bypass the engine trait — table creation/append delegate to
  `TableEngine`, so the storage engine stays swappable.
- FILE `DoPut` contains Arrow `DataLocation` rows only. Followers forward the
  authenticated tenant scope and stream to the observed leader; metasrv never
  accepts the object payload. Buffered control streams are bounded.
- Production inbound RPCs and follower-to-leader forwarding share the
  `lake-flight` TLS/auth boundary; a follower must never downgrade to anonymous
  hard-coded HTTP.

## Layout

- `lib.rs` — `Metasrv`, `MetasrvServerConfig`, and server lifecycle
- `control.rs` — Flight actions, FILE append decoding, and follower forwarding
- `operation.rs` / `maintenance.rs` — durable state machine and bounded GC
