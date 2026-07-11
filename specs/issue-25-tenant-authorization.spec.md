spec: task
name: "tenant-catalog-object-authorization"
inherits: project
tags: [security, tenant, authorization, flight, query, metasrv, objects]
---

## Intent

Turn Lake's authenticated Flight identity into an enforceable tenant boundary
for catalog reads, metadata mutations, and managed FILE stages without adding
reader-proportional metadata traffic or proxying large-object bytes.

## Decisions

- `PrincipalId` and `TenantId` are validated immutable value objects. A
  principal owns a finite namespace set and an explicit role; authorization
  never operates on an unvalidated free-form tenant string.
- A credential map loaded from a protected file maps opaque bearer tokens to
  redacted principals. Authentication installs the principal in tonic request
  extensions; handlers never re-parse or log raw credentials.
- Query authorizes referenced namespaces before DataFusion planning and filters
  discovery from its cached catalog snapshot. Policy evaluation is local and
  cannot add a per-query metastore lookup.
- Metasrv independently authorizes DDL, resolve, drop, and FILE append. A
  trusted Query service principal may carry a delegated end-user tenant header;
  ordinary principals may not forge delegation. Follower forwarding preserves
  the already-authorized identity.
- Managed-stage discovery derives an exact tenant child prefix from the
  configured base stage. The SDK continues to validate every DataLocation
  against that scoped prefix and never receives credentials from Lake.
- Production S3 workload credentials must be IAM-restricted to the same tenant
  prefix. Lake enforces its software boundary but does not issue AWS identity.
- Explicit anonymous loopback development maps to one named development
  principal. Non-loopback anonymous access remains forbidden.
- Denials return `PermissionDenied` without revealing whether another tenant's
  resource exists.

## Boundaries

### Allowed Changes
- `Cargo.toml`
- `Cargo.lock`
- `crates/lake-common/**`
- `crates/lake-flight/**`
- `crates/lake-catalog/**`
- `crates/lake-query/**`
- `crates/lake-metasrv/**`
- `crates/lake-sdk/**`
- `crates/lake-objects/**`
- `crates/lake-cli/**`
- `README.md`
- `docs/architecture.md`
- `docs/design/**`
- `docs/plans/**`
- `specs/**`
- `verification/**`
- `**/.github/**`
- `**/AGENT.md`
- `**/CLAUDE.md`
- `**/docs/guides/mise-ci.md`
- `**/docs/guides/workflow.md`
- `**/mise.toml`

The final six patterns account for shared-checkout history already present in
the repository workflow. This workspace does not edit those shared files.

### Forbidden
- Per-query authorization lookups against Metasrv or the metastore
- Treating authentication as authorization
- Trusting an end-user-supplied delegated-tenant header
- Returning bearer or AWS credentials in descriptors, diagnostics, or rows
- Proxying video/model bytes through Query or Metasrv
- Row/column-level policy or cross-tenant object sharing

## Completion Criteria

Scenario: bearer credentials resolve validated redacted tenant principals
  Test:
    Package: lake-flight
    Filter: bearer_principals_are_tenant_scoped_and_redacted
  Given two opaque credentials mapped to distinct principals and tenants
  When authentication accepts each credential and rejects malformed mappings
  Then request extensions contain the validated principal while Debug and
  errors contain neither credential value

Scenario: Query denies cross-tenant SQL before planning
  Test:
    Package: lake-query
    Filter: query_tenant_policy_denies_cross_namespace_before_execution
  Given cached tables for two tenants and a principal owning only one namespace
  When same-tenant and cross-tenant SQL are submitted
  Then same-tenant SQL executes, cross-tenant SQL is PermissionDenied before a
  table scan, and authorization causes no additional metastore request

Scenario: Query discovery exposes only authorized catalog resources
  Test:
    Package: lake-query
    Filter: query_discovery_filters_unauthorized_namespaces
  Given one cached catalog containing namespaces owned by two tenants
  When each principal performs Flight SQL catalog/schema/table discovery
  Then each response contains only that principal's authorized namespaces and
  tables without revealing hidden resource existence

Scenario: Metasrv independently rejects cross-tenant mutations
  Test:
    Package: lake-metasrv
    Filter: metasrv_rejects_cross_tenant_mutations
  Given direct user and trusted Query-service principals
  When create, resolve, drop, or FILE append targets another tenant
  Then direct cross-tenant requests and forged delegation are PermissionDenied
  while a trusted correctly delegated request is accepted

Scenario: managed FILE stages are tenant-prefix scoped
  Test:
    Package: lake-sdk
    Filter: managed_stage_discovery_is_tenant_scoped
  Given two authenticated tenants sharing one configured base stage
  When each SDK discovers its stage and opens a DataLocation
  Then descriptors contain distinct tenant child prefixes, each SDK refuses
  the other tenant's URI locally, and neither descriptor contains credentials

Scenario: anonymous development has an explicit bounded identity
  Test:
    Package: lake-flight
    Filter: insecure_loopback_uses_explicit_development_principal
  Given explicit loopback development security
  When an anonymous RPC is intercepted
  Then it receives only the configured development principal and production
  exposure validation still rejects anonymous non-loopback serving

## Out of Scope

- JWT/OIDC discovery and key rotation
- Row/column-level policy
- Cross-tenant table or object sharing
- Browser presigning
- AWS credential issuance
- Billing, quotas, and audit-log export
