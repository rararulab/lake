# lake-metasrv

The metadata layer: the stateful registry authority. `Metasrv` owns the
write path — create tables, resolve/list, and the append commit protocol.

## Invariants

- **Stateful, bounded.** Not a fan-out tier — the query layer shields it via
  cache, so it sees only cache-miss and write traffic. HA is leader + standby
  (lease-in-KV election), not free replication. (Election is v2.)
- Writes go through here to be serialized. The append commit protocol is
  engine-writes-new-version-then-CAS-registry-pointer; a lost race is a
  registry conflict the caller retries.
- Never bypass the engine trait — table creation/append delegate to
  `TableEngine`, so the storage engine stays swappable.

## Layout

- `lib.rs` — `Metasrv` (`create_table`/`append`/`resolve`/`list_*`) + `serve`
  (v1 gRPC wire is a ponytail stub)
