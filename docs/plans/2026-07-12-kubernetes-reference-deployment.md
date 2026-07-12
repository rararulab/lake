# Kubernetes reference deployment implementation plan

**Goal:** Package Lake and provide a scheduler reference that preserves its
security, availability, observability, and bounded-resource contracts.

## Task 1: Lock the deployment contract

1. Add a failing resource-graph test for Query/Metasrv topology.
2. Assert authenticated TLS gRPC health probes and loopback-only metrics.
3. Assert pod hardening, resources, topology spread, and shutdown budgets.

## Task 2: Package the process

1. Add a reproducible multi-stage Rust container build.
2. Install a pinned gRPC health probe and runtime CA material.
3. Run as a numeric non-root user with a writable spill-only directory.

## Task 3: Add Kubernetes references

1. Add namespace, configuration, services, Query Deployment, Metasrv
   StatefulSet, and disruption budgets.
2. Keep credentials as required Secret references, never example values.
3. Keep metadata and table authority in DynamoDB/S3, not pod volumes.

## Task 4: Verify and ship

1. Document prerequisites, secret shapes, rollout, probes, and tuning.
2. Run spec lifecycle, strict validation, gate, independent review, and
   independent verification.
3. Merge one reviewed PR and verify main.
