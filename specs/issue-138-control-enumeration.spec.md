spec: task
name: "control-enumeration"
inherits: project
tags: [metasrv, control-plane, pagination, bounded, production]
---

## Intent

Keep the stateful metadata authority bounded when an authenticated control
client enumerates catalog names. The existing `list_tables` action calls an
unbounded prefix list for one namespace, while `list_namespaces` materializes
the complete global catalog before JSON-encoding one Flight result.

Reproducer: register more tables than a control response can safely retain in
one namespace, then call Metasrv `DoAction("list_tables")`. Before a single
result is sent, the server has accumulated every matching key and the whole
JSON body. A global `tbl/` scan cannot be converted into exact, bounded pages
of *distinct* namespaces: Dynamo v2 distributes a namespace's table keys over
hash shards, so adjacent scan pages are neither namespace-complete nor globally
ordered. Under repeated control requests this sends unbounded work and memory
to the stateful tier, contradicting the bounded-authority design.

## Decisions

- Add `list_tables_page` and User-only `list_namespaces_page` Flight actions.
  Their JSON requests carry a positive, capped `limit` and an optional opaque
  continuation; their responses carry at most that many names plus the next
  continuation. The public CLI follows continuations and prints each page as
  it arrives instead of collecting a complete catalog.
- Implement table pages through `MetaStore::scan_prefix_page`, never
  `list_prefix` or `scan_prefix`.
- For a User principal, namespace pages enumerate only its already validated
  namespace grants. They do not read or filter the global registry. This is
  the control-plane meaning of an accessible namespace; an empty granted
  namespace remains visible.
- Preserve the legacy `list_tables` action type for small callers, but serve
  no more than one fixed, bounded page. If more data exists, fail with
  `resource_exhausted` rather than return a partial catalog. Global namespace
  enumeration (legacy or paged) fails with `failed_precondition` until a
  durable namespace index is introduced; it must never return duplicate or
  incomplete names. Update lake's CLI to use the paged actions where the
  caller has a namespace or User grants.
- Bound page input, page output serialization, and concurrent direct
  enumeration independently of the remote Query catalog snapshot admission.
  The response-held permit prevents slow control clients from accumulating
  unbounded in-flight enumeration bodies without delaying Query refreshes.

## Constraints

- Production Query remains on the authenticated remote `CatalogSource`; this
  task must not introduce Query-to-Metasrv reader calls, metadata
  schema/indexes, or a new storage protocol.
- A global namespace page without a durable namespace index is forbidden. It
  must fail closed rather than infer distinct namespaces from hash-sharded
  table scans.

## Boundaries

### Allowed Changes
crates/lake-meta/src/registry.rs
crates/lake-metasrv/src/control.rs
crates/lake-metasrv/src/lib.rs
crates/lake-metasrv/tests/two_node_forwarding.rs
crates/lake-cli/src/commands/client.rs
crates/lake-cli/src/commands/table.rs
specs/issue-138-control-enumeration.spec.md
verification/issue-138-control-enumeration.md

### Forbidden
crates/lake-query/**
crates/lake-sdk/**
crates/lake-engine*/**
crates/lake-objects/**
crates/lake-manifest/**
crates/lake-meta/src/dynamo.rs
crates/lake-meta/src/rocks.rs
docs/architecture.md
metadata schema changes, indexes, or new DynamoDB tables
changes to catalog snapshot wire formats or admission
Query connections to Metasrv for normal reader traffic
partial legacy enumeration results
buffering a complete paged catalog in the CLI

## Completion Criteria

Scenario: table enumeration has a bounded page and complete continuation walk
  Test:
    Package: lake-metasrv
    Filter: control_enumeration_pages_tables_without_full_prefix_scan
  Given one namespace with table registrations spanning more than one page
  When a client follows `list_tables_page` continuations with a small limit
  Then every table name appears exactly once, each response has no more than
  the requested limit, and the metastore receives only bounded page scans
  rather than `list_prefix`

Scenario: User namespace pages preserve boundaries without a global scan
  Test:
    Package: lake-metasrv
    Filter: control_enumeration_pages_user_grants_without_global_scan
  Given a User principal granted several namespaces, including namespaces with
  no table registrations
  When the User follows namespace-page continuations with a small limit
  Then every grant appears once in grant order and the path performs no global
  registry scan

Scenario: global namespace enumeration fails closed without an index
  Test:
    Package: lake-metasrv
    Filter: global_namespace_enumeration_fails_closed_without_namespace_index
  Given an Admin and a registry containing at least one table
  When the Admin invokes `list_namespaces_page` or the legacy
  `list_namespaces` action
  Then Metasrv returns `failed_precondition` explaining that a durable
  namespace index is required, with no partial or duplicate catalog response

Scenario: legacy enumeration fails closed at its fixed page boundary
  Test:
    Package: lake-metasrv
    Filter: legacy_control_enumeration_rejects_over_limit
  Given more names than the legacy one-page ceiling
  When a client invokes the existing `list_tables` or `list_namespaces` action
  Then Metasrv returns `resource_exhausted` with no partial JSON catalog and
  directs callers to the paged action

Scenario: CLI prints paged metadata enumeration without collecting it
  Test:
    Package: lake-cli
    Filter: client_list_follows_control_enumeration_pages
  Given a Metasrv control client whose paged action has more than one response
  When `lake client list` consumes the action
  Then it requests every continuation and writes names incrementally without
  decoding one complete catalog response

## Out of Scope

- SQL schema/table discovery, which remains served from Query's bounded local
  catalog generation.
- Creating a durable namespace index or changing Dynamo/Rocks implementations.
- Global policy quotas, rate limits beyond this action-local finite admission,
  cross-table transactions, or a custom wire protocol.
