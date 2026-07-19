spec: task
name: "iceberg-rest-auth"
inherits: project
tags: [iceberg, rest, oauth, security, query]
---

## Intent

Lake can federate a public Iceberg REST catalog, but a production catalog
normally requires authentication. Query must be able to authenticate its
deployment-local REST session without persisting a credential in Lake metadata or
placing one in SQL or Flight tickets.

## Decisions

- Add a validated, redacted REST-auth value object to `lake-iceberg`. It
  supports one static bearer token or OAuth client credentials, never both.
- Pass only vetted standard Iceberg REST properties to the official
  `iceberg-rust` client: `token`, `credential`, `oauth2-server-uri`, `scope`,
  `audience`, and `resource`.
- `lake query` reads optional auth values from process environment. OAuth
  parameters require a credential; malformed, contradictory, or partial auth
  configuration fails before Query binds.
- Secrets remain process-local: they are absent from Lake registry records,
  table descriptors, SQL, Flight tickets, errors, metrics, and `Debug` output.
- The connector remains read-only, bounded to configured namespaces, and
  avoids external namespace/table enumeration.

## Boundaries

### Allowed Changes
crates/lake-iceberg/**
crates/lake-cli/src/commands/serve.rs
Cargo.lock
docs/design/iceberg-federation.md
docs/guides/cli.md
README.md
specs/issue-199-iceberg-rest-auth.spec.md
verification/issue-199-iceberg-rest-auth.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-catalog/**
crates/lake-query/**
Lake registry schema changes
Lake metadata, statement-ticket, SQL-text, metrics-label, or log persistence of REST credentials
endpoint userinfo or credentials embedded in URLs
Iceberg write, DDL, DML, commit, or catalog mutation operations
unbounded Iceberg namespace or table enumeration

## Completion Criteria

Rule: iceberg-rest-auth — secured external REST catalogs remain process-local

Scenario: Static bearer authentication reaches a protected REST catalog
  Test:
    Package: lake-iceberg
    Filter: rest_catalog_static_bearer_auth_is_runtime_only
  Given a REST catalog that rejects requests without its required bearer token
  When Lake connects using a configured static process-local token
  Then its validated, redacted REST-auth value object reads the configured table and no test-observed configuration or error rendering contains the token

Scenario: OAuth client credentials obtain a bounded REST session token
  Test:
    Package: lake-iceberg
    Filter: rest_catalog_oauth_client_credentials_are_runtime_only
  Given a REST catalog whose token endpoint accepts one OAuth client credential
  When Lake connects with that credential and an explicit OAuth scope
  Then it exchanges the credential for a bearer token, reads the configured table, and exposes neither secret in `Debug`

Scenario: External REST failures cannot echo an outbound bearer token
  Test:
    Package: lake-iceberg
    Filter: rest_catalog_failures_redact_runtime_bearer_tokens
  Given a REST catalog that returns the received authorization header in a failing payload
  When Lake connects with a runtime bearer token
  Then the returned error and its `Debug` rendering contain no token material

Rule: query-auth-lifecycle — auth configuration fails before Query accepts traffic

Scenario: Auth configuration is explicit and internally consistent
  Test:
    Package: lake-cli
    Filter: iceberg_rest_auth_configuration_is_validated_before_listener_bind
  Given absent auth, a static token, OAuth credentials, and contradictory or partial auth settings
  When `lake query` constructs its Iceberg configuration
  Then valid forms are accepted while invalid forms fail before listener bind

## Out of Scope

- Credential persistence, credential discovery from Lake metadata, and secrets
  embedded in endpoint URLs.
- Automatic external secret rotation or a Lake-owned OAuth/token service.
- Iceberg writes, additional catalog types, or multiple external catalogs.
