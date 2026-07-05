# deploy/

Manifests for local integration-test infrastructure. Read
`docs/guides/local-deploy.md` before editing this directory.

Hard rule: local deploy is portless. Do not add fixed host ports, kind
`extraPortMappings`, or Kubernetes `NodePort` services. Multiple agents run
parallel checkouts, so endpoints are discovered dynamically and written under
`.lake/` by `scripts/test-env.ts`.

- `kind-config.yaml` — portless kind cluster config; the cluster name is passed
  by `scripts/test-env.ts`, not hard-coded here.
- `localstack.yaml` — localstack Deployment + ClusterIP Service, DynamoDB only
  (integration target for the future DynamoDB `MetaStore`).
