# Local Deploy Standards

Local deploy exists to support integration tests and agent development. It must
not assume one developer, one checkout, or one fixed port.

## Rules

- No fixed host ports. Do not reserve `localhost:4566`, `8080`, `3000`, or any
  other global port in checked-in manifests.
- No fixed global names. Cluster names, namespaces, containers, and temp files
  must be derived from the checkout path or an explicit caller-provided name.
- Every `up` command must emit a machine-readable endpoint file under `.lake/`.
  Humans can read stdout; agents should read the file.
- Every `down` command must tear down only the resources created by the same
  checkout. Never delete a shared cluster or a hard-coded namespace.
- Kubernetes manifests stay portless by default: `ClusterIP` services, no
  `NodePort`, no kind `extraPortMappings`.
- If the host needs to call a service, create a dynamic `kubectl port-forward`
  with an omitted local port such as `:4566`, then record the allocated port.

## Mise Layering

- `mise install` installs only the base development tools needed for normal
  Rust work.
- Local deploy tools (`kind`, `kubectl`, cloud emulators, load-test tools) are
  task-scoped tools on deploy tasks. They must not appear in top-level
  `[tools]` unless the whole repo needs them for normal edit/test cycles.
- CI should not run local deploy unless the job explicitly owns the environment.

## Current Contract

`mise run test-env-up` creates a checkout-scoped kind cluster and deploys
LocalStack for DynamoDB. It writes `.lake/test-env.env` with:

```bash
LAKE_TEST_CLUSTER=<cluster-name>
LAKE_DYNAMODB_ENDPOINT=http://127.0.0.1:<dynamic-port>
```

`mise run test-env-down` kills that checkout's port-forward and deletes that
checkout's cluster.
