spec: task
name: "query-rest-iceberg"
inherits: project
tags: [iceberg, rest, query, integration]
---

## Intent

Lake must prove the production composition for external Iceberg reads, not
only its individual halves. Today the REST connector is tested with a
DataFusion context and QueryEngine is tested with an in-memory Iceberg
catalog; a regression at their wiring boundary could therefore ship without
the SQL path used by Query being exercised.

## Decisions

- Use a loopback Apache Iceberg REST fixture serving a temporary local
  Iceberg table. The test must call `IcebergCatalog::connect`, so it exercises
  the actual REST connector rather than `IcebergCatalog::from_catalog`.
- Attach that connected catalog to `QueryEngine` and execute
  `SELECT ... FROM iceberg.analytics.episodes` through Lake's public SQL
  read path.
- Assert the external result is returned and the Lake metadata store remains
  untouched. The test must retain direct object reads and no Lake registry
  entry.

## Boundaries

### Allowed Changes
crates/lake-query/Cargo.toml
crates/lake-query/src/lib.rs
specs/issue-227-query-rest-iceberg.spec.md
verification/issue-227-query-rest-iceberg.md

### Forbidden
Lake registry or Metasrv changes
Iceberg production writes, catalog enumeration, or metadata mirroring
credentials, object URLs, or object bytes in Lake state or tickets
new external network or Docker dependencies
SQL semantics changes

## Completion Criteria

Rule: query-rest-iceberg-composition — Query consumes the configured REST
  catalog through the same external SQL catalog users address

Scenario: QueryEngine reads an external REST Iceberg table without Lake metadata
  Test:
    Package: lake-query
    Filter: query_engine_reads_external_rest_iceberg_catalog
  Given a temporary Iceberg table exposed through a configured loopback REST catalog
  When a QueryEngine with the connected catalog executes a fully-qualified external SQL scan
  Then it returns the external row and performs no Lake registry lookup

Scenario: Existing exact-snapshot Flight behavior remains guarded
  Test:
    Package: lake-query
    Filter: flight_ticket_keeps_iceberg_snapshot_after_catalog_advances
  Given a Flight ticket issued for an Iceberg table snapshot
  When the external catalog advances before DoGet
  Then DoGet reads the originally selected snapshot

Scenario: Mixed or unknown SQL retains the Lake catalog refresh
  Test:
    Package: lake-query
    Filter: only_external_iceberg_references_skip_lake_refresh
  Given direct SQL containing a Lake table, an unqualified reference, no table, or invalid SQL
  When QueryEngine chooses whether to bypass the Lake catalog refresh
  Then only a statement whose every physical table is fully-qualified Iceberg bypasses it

## Out of Scope

- A second Iceberg catalog, Iceberg writes, DDL/DML, catalog maintenance, or
  Docker-backed catalog products.
