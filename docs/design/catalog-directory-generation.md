# Catalog directory generations

Query replicas cache the complete table-name/schema directory. The durable
registry remains authoritative, but an unchanged directory must cost one
strong point read per refresh instead of one read and decode per table.

## Protocol

The metastore reserves two opaque keys: a directory generation and a monotonic
authority marker. Registry create and exact delete replace the generation in
the same RocksDB batch or DynamoDB transaction as the conditional table
mutation. A failed condition changes neither value. Version advances and
incarnation backfills do not signal because they cannot change listings or
schemas.

Before authority exists, every Query refresh scans `tbl/`. This is the safe
mode for deployments that may still contain an old writer. Authority is
enabled only by `lake catalog-finalize` after the operator acknowledges both a
complete writer rollout and quiescent write admission. The transition is
idempotent and monotonic; routine rollback to legacy-writer mode is forbidden.

After authority exists, a warmed Query point-reads the generation. An equal
value refreshes local health without scanning. A changed value triggers a full
scan followed by another generation read. If DDL moved the generation during
the scan, the candidate is discarded and retried up to three times. Exhaustion
returns an error while the replica continues serving its immutable last-good
snapshot.

The generation is control data. It is intentionally absent from SQL results,
Flight schemas, logs, and metric labels. Old Query binaries remain safe because
they ignore both internal keys and continue scanning. An old writer after
finalization is unsafe and violates the acknowledged deployment boundary.

## Cost model

For `T` stable tables and `Q` Query replicas, legacy steady refresh costs
roughly `O(Q*T)` registration reads and decodes per interval. Authoritative
steady refresh costs `O(Q)` point reads. Directory DDL still costs one bounded
`O(T)` scan on each replica; append commits stay on the point-read path.

The design deliberately does not add a global table manifest or a change log.
The metastore transaction remains small and changed-directory recovery keeps
the existing full-snapshot semantics.
