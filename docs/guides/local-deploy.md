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
- Bind to an ephemeral host port (Docker `-p 4566`, no fixed left-hand side)
  and read the allocated port back, rather than reserving a global one.
- Prefer the lightest thing that works: a single container needs Docker, not a
  Kubernetes cluster. Do not stand up kind/k8s for one emulator.

## Mise Layering

- `mise install` installs only the base development tools needed for normal
  Rust work. Docker is assumed present (not a mise tool).
- Any local-deploy-only tools (cloud emulators, load-test tools) are
  task-scoped `tools` on the deploy tasks, never top-level `[tools]`.
- CI should not run local deploy unless the job explicitly owns the environment.

## Current Contract

`mise run test-env-up` runs a checkout-scoped LocalStack container (DynamoDB +
S3) directly in Docker — no kind/k8s. The container is named per checkout
(path hash) and bound to an ephemeral port, both discovered dynamically. It
writes `.lake/test-env.env` with:

```bash
LAKE_DYNAMODB_ENDPOINT=http://127.0.0.1:<dynamic-port>
```

(the same endpoint serves S3, since LocalStack multiplexes all services on one
port). `mise run test-env-down` removes that checkout's container.

The community image `localstack/localstack:3` is pinned deliberately —
`:latest` now requires a `LOCALSTACK_AUTH_TOKEN` and exits without one.
