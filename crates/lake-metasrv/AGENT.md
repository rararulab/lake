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
- Operation records bind the table incarnation and fail closed after a
  same-name drop/recreate.
- Drop publishes an incarnation-bound tombstone before registry or object
  deletion. Server placement gives every incarnation a unique object prefix,
  so delayed old-leader cleanup cannot touch a replacement.
- Never bypass the engine trait — table creation/append delegate to
  `TableEngine`, so the storage engine stays swappable.
- FILE `DoPut` contains Arrow `DataLocation` rows only. Followers forward the
  authenticated tenant scope and stream to the observed leader; metasrv never
  accepts the object payload. Every process reserves one concurrency slot and
  one worst-case control-buffer budget before polling a stream; the permit
  covers forwarding or local commit through response construction.
- Remote DDL never accepts a dataset URI. `TablePlacement` derives a unique
  generation-qualified location from trusted server configuration after
  validating both identifier path segments.
- Production inbound RPCs and follower-to-leader forwarding share the
  `lake-flight` TLS/auth boundary; a follower must never downgrade to anonymous
  hard-coded HTTP.
- Shutdown has one finite process deadline covering Flight drain, maintenance,
  and leadership-campaign cleanup. Maintenance stops at table boundaries;
  unfinished owned tasks are aborted and reaped at the deadline.
- Leader table maintenance reads one bounded registry page per configured tick,
  resumes from an opaque process-local cursor, and re-resolves each candidate
  under its table lock before touching the engine.
- Append-operation GC drains consecutive metadata pages under finite per-tick
  page and wall-clock budgets. It advances the cursor only after a whole page,
  stops without wrapping at end-of-scan, and preserves cancellation, per-table
  serialization, exact-stage cleanup, and fencing.

## Layout

- `lib.rs` — `Metasrv`, `MetasrvServerConfig`, and server lifecycle
- `placement.rs` — trusted local/S3 placement and path-segment validation
- `control.rs` — Flight actions, FILE append decoding, and follower forwarding
- `operation.rs` / `maintenance.rs` — durable state machine and bounded GC
