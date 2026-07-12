# Durable PollFlightInfo async results

Issue: #110

## Architecture gate

The Query tier remains stateless. Async execution introduces state, but that
state belongs in a dedicated coordination store and service-owned result
storage—not in a Query process and not in the table catalog authority. Poll
traffic therefore scales independently of registry/cache-miss traffic.

## Protocol state

An initial standard `PollFlightInfo(FlightDescriptor{CommandStatementQuery})`
performs the same identity authorization and exact snapshot pinning as
interactive `GetFlightInfo`. It creates a random query id, encrypts the pinned
job specification, stores that spec as an immutable control object, and CAS
creates a compact state record. The returned descriptor contains a versioned
encrypted poll handle bound to query id, tenant, principal, audience, and
expiry.

States are `queued -> running -> completed|failed|cancelled|expired`.
`running` carries lease epoch, owner token, and deadline. Every renew/progress/
terminal mutation compares the entire prior record and current epoch. An
expired lease can be claimed by another replica; the stale worker cannot
publish progress or completion afterward.

## Data plane

Workers decrypt the immutable job spec, reconstruct the ticket-pinned catalog,
and stream DataFusion batches through an Arrow IPC writer. A part rolls at
finite row/byte thresholds and uploads through an `AsyncResultStore` rooted at
`<tenant>/<query-id>/`. Each object is immutable. A bounded manifest lists
schema, exact ordered parts, row/byte totals, hashes, completion time, and
expiry. Only the fenced CAS to `completed(manifest)` makes results visible.

Polling turns manifest parts into short-lived identity-bound endpoint tickets.
DoGet redeems an exact local or S3 immutable part on any Query replica. This
keeps standard Flight semantics: a Flight location names a Flight service, not
a raw object GET URL. Polling never sends object URLs, credentials, SQL,
snapshot locations, or raw coordination records to the client.

## Cancellation and cleanup

Flight `CancelFlightInfo` CASes any nonterminal state to cancelled. Workers
check cancellation between input batches, part rolls, and uploads; dropping an
upload invokes its cleanup owner. Repeated cancellation is idempotent. Expired
terminal records and their complete or partial result prefixes are removed by
a bounded, checkpointed sweeper, with the state record retained until cleanup
is conclusive.

## Implementation order

1. RED tests and types for bounded records, legal transitions, fencing, expiry,
   and a dedicated store contract over Rocks/controlled doubles.
2. Encrypted async job/poll capabilities reusing snapshot claims without
   weakening statement-ticket validation.
3. PollFlightInfo submission/polling on the traced Flight service, including
   cross-replica and catalog-isolation tests.
4. Bounded Arrow part writer, immutable manifest, local result store, crash
   takeover, and CancelFlightInfo.
5. S3 prefixes/exact DoGet endpoints, Kubernetes/Dynamo configuration, SDK API
   and example, lifecycle sweeper, LocalStack hostile probes.
6. Full lane-1 lifecycle, workspace gate, security/performance/removal review,
   PR, and merge.
