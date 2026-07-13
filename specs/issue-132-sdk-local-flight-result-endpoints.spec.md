spec: task
name: "sdk-local-flight-result-endpoints"
inherits: project
tags: [sdk, query, flight, flight-sql, endpoint, streaming, correctness]
---

## Intent

Make synchronous SDK SQL reads consume the complete Arrow Flight result rather
than silently returning only the first result partition. `LakeClient::query`
currently takes the first `FlightInfo.endpoint`, even though FlightInfo can
legally contain several endpoints whose streams together form one result.

Reproducer: a Flight SQL service returns `ordered=true` with two valid
endpoints: one has no locations and one has the exact reuse-connection URI.
Their distinct tickets yield distinct batches. The current SDK redeems only
the first ticket and returns an apparently successful but incomplete result.
Lake Query currently publishes one local endpoint, but a standards-compatible
distributed Query implementation, proxy, or service evolution would activate
this data-loss behavior.

## Decisions

- Introduce public
  `QueryResultStream = Pin<Box<dyn Stream<Item = Result<RecordBatch, FlightError>> + Send>>`
  (or one documented equivalent), and change `LakeClient::query` to return
  `Result<QueryResultStream>`. Keep `AsyncQueryResultStream` as its existing
  owned stream type or a documented compatible alias; never expose raw
  FlightData.
- This is a deliberate, narrow semver API change. Callers using inferred
  result types plus `futures::TryStreamExt` (`try_next` / `try_collect`) remain
  source-compatible. Callers explicitly naming `FlightRecordBatchStream` or
  reading per-DoGet headers/trailers migrate to `QueryResultStream`; those
  headers/trailers have no defined whole-result aggregation.
- Validate the complete endpoint set before the first DoGet: 1..=256
  endpoints, non-empty ticket up to 512 KiB each, and up to 8 MiB total ticket
  bytes.
- Accept only endpoints with no locations or locations consisting exclusively
  of exact `arrow-flight-reuse-connection://?`. Any external location,
  including one mixed with reuse, returns a typed redacted SDK error before
  DoGet. Do not forward bearer, TLS, or process credentials.
- For `ordered=true`, concatenate endpoint streams in declared order. For
  unordered FlightInfo, declared sequential order is a legal deterministic
  choice; no parallel fetching or reordering.
- Missing tickets and endpoint-limit violations fail before DoGet with typed
  errors. Errors must not render remote URI text, tickets, or credentials.
- Do not change the separate async-result manifest path.

## Constraints

- Use existing Arrow Flight 58 and futures/Tokio dependencies; no new Flight
  client, HTTP downloader, redirect mechanism, or remote connection pool.
- Validation happens immediately after Flight SQL `execute` and before any
  DoGet, including for malformed later endpoints.
- The SDK owns one configured Query authority, not cross-authority credential
  delegation.

## Boundaries

### Allowed Changes
crates/lake-sdk/**
README.md
docs/design/sql-api-over-s3.md
docs/plans/2026-07-13-sdk-flight-result-endpoints.md
specs/issue-132-sdk-local-flight-result-endpoints.spec.md
verification/issue-132-sdk-local-flight-result-endpoints.md

### Forbidden
crates/lake-query/**
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-objects/**
opening arbitrary remote endpoint connections
forwarding bearer TLS or process credentials to endpoint locations
buffering or collecting complete SQL results
changing async result manifest formats limits or ticket semantics
metadata-authority traffic on the SDK result-read path
a bespoke SQL or result protocol

## Completion Criteria

Scenario: SDK consumes one and ordered local/reuse endpoint results
  Test:
    Package: lake-sdk
    Filter: sdk_query_consumes_single_and_ordered_local_reuse_endpoints
  Given a Flight SQL mock serving one local result and then an ordered
  two-endpoint result with empty/reuse locations
  When LakeClient query is drained through EOF
  Then every ticket is redeemed once and all batches are returned in declared
  endpoint order without collecting the complete result

Scenario: Missing endpoint ticket fails before any redemption
  Test:
    Package: lake-sdk
    Filter: sdk_query_rejects_missing_ticket_before_doget
  Given a FlightInfo containing an endpoint without a ticket
  When LakeClient query validates the FlightInfo
  Then it returns the typed missing-ticket error and the mock observes zero
  DoGet RPCs

Scenario: Endpoint count and ticket metadata are bounded before redemption
  Test:
    Package: lake-sdk
    Filter: sdk_query_rejects_excessive_endpoint_metadata_before_doget
  Given a FlightInfo exceeding the endpoint count, per-ticket byte, or
  aggregate ticket-byte ceiling
  When LakeClient query validates that FlightInfo
  Then it returns a typed invalid-endpoint error before any DoGet and does not
  allocate or stream query batches

Scenario: External endpoint locations fail closed without credential disclosure
  Test:
    Package: lake-sdk
    Filter: sdk_query_rejects_external_location_before_doget
  Given a FlightInfo endpoint carrying a remote Flight or HTTPS location with
  a capability-like URI
  When LakeClient query validates the endpoint set
  Then it returns a typed redacted unsupported-location error before DoGet and
  its display/debug output contains neither URI nor client credentials

Scenario: QueryResultStream preserves normal streaming ergonomics
  Test:
    Package: lake-sdk
    Filter: sdk_query_result_stream_supports_try_stream_consumption
  Given a QueryResultStream containing a valid endpoint result
  When caller code consumes it with existing TryStreamExt methods
  Then batches and terminal Flight errors retain normal stream semantics
  without access to raw FlightData or per-DoGet metadata

## Out of Scope

- Remote endpoint routing, connection pools, redirects, HTTP(S) extended
  locations, or cross-authority credential delegation.
- Parallel consumption or reordering unordered endpoints.
- Query server endpoint production, async manifests, Flight SQL ticket
  formats, object storage, metadata authority, or SQL dialect changes.
