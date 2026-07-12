spec: task
name: "kubernetes-reference-deployment"
inherits: project
tags: [deployment, kubernetes, container, security, operations]
---

## Intent

Lake's runtime now has TLS/bearer security, authenticated gRPC Health,
loopback-only Prometheus metrics, finite shutdown, and process resource
budgets. Provide a production reference deployment that preserves those
contracts instead of requiring operators to reconstruct them in YAML.

## Decisions

- Query is a stateless Deployment with at least two replicas. Metasrv is a
  three-replica StatefulSet with stable identities and a disruption budget.
- Flight is TLS-only and uses mounted bearer/principal files. Health probes
  invoke the standard gRPC Health API with the same CA, server name, and
  bearer metadata; anonymous Kubernetes gRPC probes are forbidden.
- Prometheus binds only to pod loopback and is never exposed by a Service.
- Pods run non-root with immutable root filesystems, dropped capabilities,
  explicit CPU/memory requests and limits, topology spreading, and a
  termination grace longer than Lake's configured drain budget.
- Production metadata and table state use DynamoDB and S3. No workload pod
  uses a persistent volume as authoritative Lake state.
- A multi-stage container builds the Rust binary and runs it as a numeric
  non-root user with the authenticated gRPC health probe installed.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
mise.toml
crates/lake-cli/Cargo.toml
crates/lake-cli/tests/kubernetes_manifests.rs
Dockerfile
.dockerignore
deploy/**
docs/guides/kubernetes.md
docs/architecture.md
README.md
docs/plans/2026-07-12-kubernetes-reference-deployment.md
specs/issue-71-kubernetes-reference.spec.md
verification/issue-71-kubernetes-reference.md

### Forbidden
crates/lake-common/**
crates/lake-flight/**
crates/lake-meta/**
crates/lake-metasrv/src/**
crates/lake-query/src/**
public Flight wire changes
anonymous health probes
non-loopback metrics services
authoritative local pod storage
cloud-specific ingress or IAM policy
Helm charts or operators

## Completion Criteria

Scenario: Reference deployment preserves the runtime security and topology contracts
  Test:
    Package: lake-cli
    Filter: kubernetes_reference_is_secure_and_matches_runtime_contract
  Given the checked-in container and Kubernetes reference files
  When their rendered resource graph and pod specifications are inspected
  Then Query is stateless and scalable, Metasrv is bounded and disruption-safe, probes authenticate, metrics stay private, and every pod has finite resources and shutdown

## Out of Scope

- Cluster provisioning, ingress, service mesh, cert issuance, or IAM creation.
- Autoscaler policy and workload-specific resource sizing.
- Distributed tracing and backup/restore automation.
- A Helm chart, Kubernetes operator, or cloud-specific overlay.
