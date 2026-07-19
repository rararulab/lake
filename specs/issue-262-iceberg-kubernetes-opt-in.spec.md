spec: task
name: "iceberg-kubernetes-opt-in"
inherits: project
tags: [iceberg, kubernetes, deployment, security]
---

## Intent

Make the existing read-only Iceberg REST federation deployable from Lake's
Kubernetes reference without making it mandatory or letting a catalog
credential leak into the shared runtime ConfigMap, Metasrv, or repository.

Reproducer: the current reference has no Iceberg configuration source. An
operator must hand-edit the Query pod to enable the documented environment
variables, which makes it easy to add a partial configuration that prevents
Query from starting or to place a REST token in a broadly shared ConfigMap.

## Decisions

- Iceberg remains disabled when no opt-in resource is present. The base
  ConfigMap must not define empty or placeholder `LAKE_ICEBERG_*` values because
  Query rejects partial configuration before it binds.
- The Query pod binds the known optional `lake-iceberg-runtime` ConfigMap keys
  individually: complete non-secret endpoint, warehouse, namespace, and
  optional timeout or OAuth metadata configuration. It must not `envFrom` an
  arbitrary ConfigMap key.
- The Query pod binds only `LAKE_ICEBERG_REST_TOKEN` or
  `LAKE_ICEBERG_REST_CREDENTIAL` from the optional
  `lake-iceberg-runtime` Secret. It is not mounted or imported by Metasrv and
  Lake does not commit a Secret resource.
- The runbook shows explicit creation, all-or-nothing configuration, auth-mode
  selection, credential rotation/removal, and Query workload identity access
  to Iceberg table files.

## Boundaries

### Allowed Changes
deploy/kubernetes/lake.yaml
docs/guides/kubernetes.md
crates/lake-cli/tests/kubernetes_manifests.rs
specs/issue-262-iceberg-kubernetes-opt-in.spec.md

### Forbidden
crates/lake-iceberg/**
crates/lake-query/**
crates/lake-metasrv/**
Iceberg SQL, snapshot, catalog, or auth behavior
Lake registry or Metasrv wiring
an in-repository Kubernetes Secret resource or static credential
changes to another workspace

## Completion Criteria

Scenario: Kubernetes federation configuration is absent by default and isolated when enabled
  Test:
    Package: lake-cli
    Filter: kubernetes_reference_iceberg_federation_is_opt_in_and_query_only
  Given the checked-in Kubernetes reference manifest
  When its Query and Metasrv pod environments are inspected
  Then only Query has optional individually scoped ConfigMap and Secret key references named
  lake-iceberg-runtime, no arbitrary Iceberg envFrom, base Iceberg variable, or Secret resource
  exists, and the Kubernetes runbook documents complete creation, one-auth-mode setup, and
  activation restart

## Out of Scope

- Iceberg write federation, catalog enumeration, or a Lake-owned Iceberg commit path.
- Adding a controller, Helm chart, Kustomize overlay, credential discovery, or retry loop.
