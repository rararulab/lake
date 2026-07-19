spec: task
name: "iceberg-rest-tls"
inherits: project
tags: [iceberg, rest, tls, security, query]
---

## Intent

Protect Lake's deployment-only Iceberg bearer and OAuth credentials on the
external catalog control path. Today a Query deployment accepts a remote
`http://` REST catalog or overridden OAuth token endpoint. A deployment can
therefore configure a bearer token or client credential and send it over
plaintext transport before Query binds.

This advances the `goal.md` working signal that the stateless Query layer
absorbs read fan-out without making credentials part of its data/control
plane, while preserving the read-only external-catalog boundary. It does not
alter Lake's metadata authority, object-data path, or SQL semantics.

## Decisions

- An external Iceberg REST catalog endpoint and an overridden OAuth token
  endpoint require `https`. Plain `http` remains permitted only for a numeric
  IP loopback endpoint (`127.0.0.0/8` or `::1`) so local integration tests and
  explicit single-host development remain possible.
- Hostnames such as `localhost` do not qualify for the plaintext exception:
  Lake must not make a DNS-dependent security decision.
- The existing credential-free URL restrictions (no userinfo, query, or
  fragment), redacted error boundary, finite namespace allowlist, request
  timeout, and OAuth renewal behavior remain unchanged.

## Boundaries

### Allowed Changes
crates/lake-iceberg/src/lib.rs
crates/lake-iceberg/tests/config.rs
crates/lake-cli/src/commands/serve.rs
README.md
docs/design/iceberg-federation.md
docs/guides/cli.md
specs/issue-206-iceberg-rest-tls.spec.md

### Forbidden
crates/lake-meta/**
crates/lake-metasrv/**
crates/lake-catalog/**
crates/lake-query/**
crates/lake-flight/**
Lake registry schema changes
Iceberg write, DDL, DML, commit, or catalog mutation operations
new insecure-production escape hatches
custom CA, mTLS, proxy, or DNS-resolution behavior for Iceberg REST
credentials in metadata, SQL, Flight tickets, logs, metrics, or URLs
unbounded Iceberg namespace or table enumeration

## Completion Criteria

Rule: iceberg-rest-tls — external REST credentials never default to plaintext

Scenario: External REST URLs require TLS except numeric loopback development
  Test:
    Package: lake-iceberg
    Filter: external_rest_urls_require_tls_or_numeric_loopback
  Level: unit
  Given HTTPS external catalog and OAuth endpoints plus remote HTTP, hostname
    HTTP, IPv4 loopback HTTP, and IPv6 loopback HTTP variants
  When catalog and OAuth client-credential configurations are validated
  Then only HTTPS and numeric loopback HTTP variants are accepted

Scenario: Deployment configuration rejects plaintext credentials before Query binds
  Test:
    Package: lake-cli
    Filter: iceberg_rest_transport_is_validated_before_listener_bind
  Level: unit
  Given a complete Iceberg deployment configuration with either a remote HTTP
    catalog endpoint or a remote HTTP OAuth token endpoint
  When Query parses its environment configuration
  Then it rejects the setup before the Flight listener can bind, without
    disclosing the token or client secret

## Out of Scope

- Configuring external REST trust roots, client certificates, proxy routing, or
  DNS-based allowlists.
- Any external Iceberg write path or changes to the native Lake storage and
  commit protocol.
