# Verification: issue #71 Kubernetes reference deployment

- verdict: **PASS**
- score_authority: `verifier`
- base_sha: `1d89475448f96860c1d6bb35e5af063604b7b8f4`
- head_sha: `142602049aa47ead6b07a73f30f258d2dc416058`
- implementer_evidence: not used as acceptance evidence; commands and runtime probes below were executed independently

## Revision and boundary

- `git merge-base 142602049aa47ead6b07a73f30f258d2dc416058 1d894754` returned the exact base SHA.
- The workspace was clean and `@-` was exactly the candidate before verification.
- Every changed path is allowed by the latest spec: root Cargo files, `mise.toml`, CLI manifest/test, Docker inputs, deployment/docs/plan/spec. No forbidden domain implementation, Flight wire, Helm/operator, ingress, or IAM-policy path changed.
- Candidate subject is conventional: `feat(deploy): add hardened Kubernetes reference (#71)`.

## Bound selector and schema validation

- `mise run spec-lifecycle specs/issue-71-kubernetes-reference.spec.md`: PASS; the guarded runner reported the single scenario PASS and confirmed a real test executed.
- `cargo test -p lake-cli kubernetes_reference_is_secure_and_matches_runtime_contract -- --nocapture`: PASS (`1 passed`).
- `mise exec -- kubeconform -strict -summary -kubernetes-version 1.32.0 deploy/kubernetes/lake.yaml`: PASS; 11 resources found, 11 valid, 0 invalid/errors/skipped.
- `kubectl apply --dry-run=client --validate=false -f deploy/kubernetes/lake.yaml -o name`: PASS; deterministically rendered Namespace, two ServiceAccounts, ConfigMap, three Services, two PDBs, Deployment, and StatefulSet.

## Acceptance matrix

- **Topology:** Query is a three-replica stateless Deployment with rolling `maxUnavailable: 0`, topology spread, and PDB. Metasrv is exactly a three-replica StatefulSet with stable headless service, pod-IP advertisement, topology spread, and `minAvailable: 2` PDB.
- **Authenticated health:** startup/liveness use the empty standard Health service and readiness uses `arrow.flight.protocol.FlightService`. Every probe loads the bearer token, supplies TLS CA and server-name override, and uses `grpc_health_probe`. Query binds wildcard and probes loopback; Metasrv binds `${POD_IP}:50052` and all three probes use the same `${POD_IP}:50052` interface. The selector locks this address relationship.
- **Private metrics:** both processes bind `LAKE_METRICS_ADDR=127.0.0.1:9090`; no Service exposes 9090. Documentation requires a same-pod sidecar or capable node agent.
- **Pod hardening:** service-account token automount is disabled; pod user/group are numeric 65532 with RuntimeDefault seccomp; Lake and init containers deny privilege escalation, use read-only roots, drop all capabilities, and declare CPU/memory requests and limits.
- **Finite lifecycle:** Lake drain is 30 seconds and pod termination grace is 45 seconds. Query has bounded memory/spill configuration and a disposable capped spill `emptyDir`.
- **Durable authority:** cloud ConfigMap wires DynamoDB/S3 identities and prefixes. No workload contains PVC or hostPath authoritative storage; only memory-backed prepared secrets and disposable Query spill use `emptyDir`.
- **Fail closed:** no Secret object or credential is checked in. Both workloads reference required external Secrets; the non-root init container copies them to memory-backed storage with mode 0600. Missing or malformed security material prevents init/startup or Lake security validation.
- **Documentation:** the Kubernetes guide covers image digest pinning, cloud/workload identity customization, secret creation and certificate names, authenticated probes, loopback metrics, shutdown/resource tuning, rollout, and the non-turnkey threat boundary.

## Container contract

Final Dockerfile inputs were hashed during verification:

- `Dockerfile`: `26d541245a446afad0f10584d42ba4515c14ecea26097667922da745d0e72c02`
- `.dockerignore`: `09d1e71de475cbce03d13e66c7010cc513945e3402219e996159078a209436f4`
- `Cargo.lock`: `3dac6efb3507e5c4fd03dcf27e47d0ab6c4c40cd4fe412c6667fbc9f8c64f835`

`docker image inspect/history lake:issue-71` showed image ID `sha256:0b053565285f44390c45beb8e4ce7a79e5f4ef435b976d7fd1d43bb8d8effe96`, `linux/arm64`, `User=65532:65532`, and entrypoint `/usr/local/bin/lake`. History matches the final pinned frontend/base/probe stages, dated Debian snapshot installs, binary copies, numeric user, and entrypoint.

Independent container runs proved:

- `id`: `uid=65532(lake) gid=65532(lake)`.
- `/usr/local/bin/lake --version`: `lake 0.0.1`; default entrypoint `--help` exposed the expected Lake CLI commands.
- `/usr/local/bin/grpc_health_probe -version`: `0.4.53` at commit `64350b746e427bb84b0f1bf3572dabe03a73fe0b`.
- Both binaries are executable. SHA-256: Lake `710aa7967f56b6fb3413aace204da390fef24cd8042d1cafa839c977ec54ebfc`; probe `39c7e985ee1e719f5d8aa0eab3f18eea95230fbc2e8149ba6e0b67f440c0670e`.
- `/tmp/lake-spill` is owned by UID/GID 65532.

A cached-build probe was started, but daemon recovery had discarded BuildKit cache and the command began a second full cold build. It was intentionally cancelled before compilation, per instruction not to repeat the already completed immutable-snapshot cold build; this is not counted as a passing or failing build result. The existing final image was independently inspected and executed as above.

## Repository gates

- `mise run doctor`: PASS.
- Fresh `mise run gate`: PASS in 34.31s on final head, including workspace/all-target tests, CLI selftest, hooks, and site checks.
- `mise run test-integration`: initial attempt found a dead daemon-recovery LocalStack container with no published port. After inspecting and removing that stale test-only container, the clean rerun passed 14/14 tests in 18.42s and removed its container.

## Verdict

**PASS.** Final candidate `142602049aa47ead6b07a73f30f258d2dc416058` satisfies every spec acceptance criterion. Kubernetes resources validate and render locally, the corrected Metasrv probes target the bound interface, the runtime image is non-root and contains working Lake/probe binaries, and final gate plus integration verification pass. No release blocker remains.
