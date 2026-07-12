# SQL API over S3

## Decision

Yes: lake can expose SQL over datasets stored in S3 without adding a storage
node or a second SQL protocol. The public protocol remains Arrow Flight SQL;
query workers resolve a registered table snapshot, read Lance files directly
from S3, and return Arrow data.

"Over S3" describes the data plane, not the SQL transport. Clients submit SQL
over Flight SQL. They do not submit arbitrary `s3://` paths and they do not get
S3 credentials for source data.

## Current path

```text
Flight SQL client
  -> lake-query / DataFusion
  -> cached registry: table -> {location, exact version}
  -> Lance TableProvider at that version
  -> S3 objects
  -> streaming Arrow FlightData
```

This path is implemented for interactive results. `GetFlightInfo` resolves and
plans every physical SQL reference at an exact table location, incarnation,
and version, publishes that schema, and encrypts the canonical bounded
snapshot set into its opaque tenant/principal-bound ticket. `DoGet` rebuilds a
request-local catalog from those claims on any stateless replica and executes
with `DataFrame::execute_stream`; it never consults current-version pointers
or substitutes latest when a historical provider is unavailable. Flight SQL
defines this `FlightInfo`/ticket/`DoGet` flow and uses Arrow as the result
format ([Flight SQL specification](https://arrow.apache.org/docs/format/FlightSql.html)).

The inner encrypted ticket protocol is versioned. Snapshot-aware replicas
reject older SQL-only tickets, and older replicas reject the new envelope
instead of silently ignoring claims. Use blue/green or drain/cutover for this
protocol transition; mixed-version cross-routing is fail-closed.

The public planner accepts `SELECT` and `EXPLAIN`. DDL, DML, session statements,
and `CREATE EXTERNAL TABLE ... LOCATION 's3://...'` are rejected. Source S3
locations enter the system only through trusted registry/control-plane calls.

## Result delivery tiers

### Tier 1: interactive streaming (implemented)

Use the existing Flight SQL endpoint for bounded or latency-sensitive results:

1. Authenticate and submit a statement with `GetFlightInfo`.
2. Receive an opaque, expiring ticket.
3. Fetch Arrow IPC batches with `DoGet`.

This avoids an intermediate result copy and starts returning rows before the
query finishes. It is the default for schema discovery, sampling, filters, and
other results that fit the query service's streaming limits.

### Tier 2: durable asynchronous results

Large scans should not pin one gRPC connection for their full lifetime:

1. Submit the statement with `PollFlightInfo`.
2. Execute asynchronously under a CAS-fenced worker lease and materialize
   tenant/query-scoped Arrow IPC stream parts
   (default) or Parquet under a service-owned result prefix.
3. Publish one immutable manifest only after every bounded part succeeds.
4. Return short-lived identity-bound `FlightEndpoint` tickets; each `DoGet`
   redeems one exact local or S3 part on any Query replica.

Flight explicitly supports long-running queries through `PollFlightInfo` and
multiple result endpoints. Lake keeps those endpoints inside standard Flight:
the ticket is authenticated and a location, when present, names a Flight
service rather than a raw object URL
([Flight RPC specification](https://arrow.apache.org/docs/format/Flight.html)).
The server-side materialization remains compatible with the established
warehouse pattern of writing query results to S3
([Athena result files](https://docs.aws.amazon.com/athena/latest/ug/querying-finding-output-files.html)).

Use this result layout:

```text
s3://<result-bucket>/<tenant>/<query-id>/
  manifest.json
  part-00000.arrow
  part-00001.arrow
  ...
```

The `query-id` is random and non-semantic. The manifest records the schema,
format, partition list, row/byte counts, completion state, and expiry. Failed
or cancelled queries never publish a completed manifest; lifecycle rules reap
partial objects and incomplete multipart uploads.

## Security boundary

Lake now provides verified TLS and deployment-bearer authentication on every
Flight RPC and every internal forwarding hop. Plaintext anonymous listeners
are loopback-only unless deployment explicitly declares a trusted terminating
proxy. Direct Internet or multi-tenant exposure still requires all remaining
policy and resource controls below:

- Rotate/issue deployment credentials operationally, and replace the initial
  opaque bearer authenticator with tenant-aware identity validation. Handshake
  is already covered by the same interceptor as every other RPC.
- Tenant-aware catalog authorization before planning, with denied objects
  indistinguishable from missing objects where disclosure matters.
- Read-only SQL validation in the server. Never trust clients to omit DDL/DML.
- Source bucket and prefix allowlists owned by deployment configuration. SQL
  text can never introduce a source URI or object-store credential.
- Statement tickets are now opaque authenticated ciphertext bound to exact
  principal, tenant, audience, issued-at, and expiry, with a bounded shared key
  ring for stateless replica rotation. Raw SQL, storage locations, incarnation
  ids, and credentials are not ticket plaintext. Exact table snapshots are
  bound across `GetFlightInfo`/`DoGet`; async mode uses a separate encrypted
  durable job capability and exact snapshot-set envelope.
- Short-lived result tickets bound to principal, tenant, query, and exact part;
  storage stays below a result prefix unique to the tenant and query.
- Per-replica concurrency, queue wait, execution duration, SQL/ticket size,
  DataFusion memory, and local spill are now bounded. Add per-tenant limits for
  scanned bytes, result bytes, memory, spill, and egress plus fair queuing.
  Process ceilings are defense in depth, not tenant accounting. Cancellation
  must propagate to DataFusion and multipart uploads.
- Encryption at rest, result-bucket lifecycle deletion, audit logs, and
  metrics for planning, scanning, spilling, materialization, and download.

Direct S3 permission is a separate authorization channel: a principal with
`GetObject` on a result object can read it even if the SQL service later denies
that principal. AWS documents the same property for Athena result buckets
([Athena query results](https://docs.aws.amazon.com/athena/latest/ug/querying.html)).

## Client compatibility

Keep the server standards-first:

- Arrow Flight SQL for statements, metadata, tickets, and streaming results.
- Arrow IPC stream as the durable result format.
- ADBC Flight SQL for general client access. The Arrow project ships stable Go
  and beta Java/C# Flight SQL drivers, with language reuse through driver
  managers where available
  ([ADBC driver status](https://arrow.apache.org/adbc/main/driver/status.html)).

Do not add a bespoke `POST /sql` JSON API unless a concrete client cannot use
Flight SQL. Such an API would need to duplicate authentication, cancellation,
schema encoding, streaming, error mapping, and result-location semantics.

## Delivery sequence

1. Productionize Tier 1: TLS, immutable tenant principal maps, pre-planning
   authorization, per-replica limits, encrypted expiring statement tickets,
   and staged ticket-key rotation are wired; next pin exact table snapshots,
   finish cancellation coverage, and add load tests.
2. Add an async query state store and `PollFlightInfo`. **Implemented:** the
   Rust SDK exposes convenience and persistable-handle lifecycles, while Query
   uses an injected dedicated CAS store and result object store. Stable
   identity-bound submission ids make lost initial responses idempotent.
3. Bounded result materialization, atomic manifest publication, exact DoGet
   endpoints, and expiry cleanup are implemented.
4. Add ADBC compatibility tests for streaming, polling, endpoint downloads,
   errors, and cancellation.

The first step exposes the SQL API over S3-backed tables. The later steps
change how large *results* are delivered; they are not required for direct
query reads from S3.
