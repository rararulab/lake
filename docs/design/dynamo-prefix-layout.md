# DynamoDB prefix-isolated metadata layout

## Problem

The v1 Dynamo table stores every logical metadata key in one HASH primary key
named `pk`. Prefix reads therefore use `Scan` with a `begins_with(pk, ...)`
filter. Dynamo applies `Limit` to evaluated items before the filter, so a
catalog refresh for `tbl/` pays for retained append operations, manifest
history, leases, and tombstones. Seven days of append-operation records make
reader cache refresh cost grow with write throughput rather than table count.

## V2 physical layout

V2 is a companion on-demand table named `<LAKE_DYNAMODB_TABLE>_prefix_v2`:

| Attribute | Dynamo key | Meaning |
|---|---|---|
| `bucket` | HASH | `<family>#<00..3f>` |
| `pk` | RANGE | complete engine-neutral `MetaStore` key |
| `val` | — | exact binary value |

`family` is the first path segment (`tbl`, `append-operation`,
`lance-manifest`, and so on); keys without `/` use `root`. The shard is a
stable 64-way SHA-256 shard of the complete logical key. One hot family can
therefore use multiple Dynamo partitions without changing the `MetaStore`
contract.

A point read computes one bucket and uses strongly consistent `GetItem`.
Prefix reads query all 64 family shards with
`bucket = :bucket AND begins_with(pk, :prefix)`. A page cursor contains the
current shard and last complete `pk`; one backend request evaluates at most the
requested limit and never evaluates another family or non-matching sort-key
prefix. Full-prefix APIs drain the same paged primitive.

## Authority and migration state machine

V1 remains available during a rolling upgrade. New binaries support these
durable states:

1. `v1`: reads and writes use the legacy table.
2. `dual`: v1 remains read authority; each mutation atomically updates v1 and
   v2 with one cross-table Dynamo transaction. A v2 record may be absent for a
   pre-upgrade key, but a conflicting non-equal value fails closed.
3. `backfill`: a bounded migrator scans v1 and conditionally creates or
   validates v2 records. If concurrent dual-write moved a record, the migrator
   reloads v1 and converges without overwriting the newer value.
4. `v2-authoritative`: after every commit-capable node is dual-capable and a
   full backfill verification succeeds, the migrator CAS-publishes a durable
   completion marker. Reads switch to v2. Dual nodes continue mirroring v1;
   v2-only nodes may later stop doing so.

The marker is monotonic and exact-value guarded. Publishing it while a
v1-only writer still runs is forbidden: operators first roll out dual mode to
all metadata nodes, then finalize migration. Query readers may continue serving
their last-good cache throughout; write admission need not be stopped.

Each dual mutation increments an internal generation in the same Dynamo
transaction as both physical copies. Finalization reads the generation,
performs bounded bidirectional key/value verification, reads it again, and
publishes the marker under an exact generation condition. A concurrent write
therefore makes finalization fail closed. Backfill progress is stored as a
durable v1 scan cursor only after every evaluated item converges; replay after
a crash is idempotent.

## Mutation invariants

- `cas` checks the current authority value and applies both physical writes in
  one Dynamo transaction.
- `guarded_mutate` checks the exact authority guard and exact target while
  updating both target copies atomically.
- Delete removes both copies under the same exact expected value.
- Backfill never overwrites a different v2 value from a concurrent writer.
- After the completion marker, prefix reads never fall back to a full v1 scan.
- A stale dual node encountering a newer v2 value fails its v1/v2 conditions;
  it cannot regress v2 and retries after observing the marker.

## Operational rollout

1. Deploy binaries in dual mode everywhere; confirm a metric reports zero
   v1-only peers.
2. Run `lake dynamo-migrate --page-size N` repeatedly or let it resume its
   durable cursor until backfill verification completes.
3. Finalize the marker only after the binary rollout check passes.
4. Verify v2-only prefix reads and evaluated-item metrics.
5. Roll to v2-authoritative mode. Retain v1 for rollback through at least one
   append-operation retention horizon before destructive removal.

Rollback before finalization is v1-only. After finalization, rollback must use
a dual-capable binary because v2 is the authority; an old v1-only binary is not
safe.
