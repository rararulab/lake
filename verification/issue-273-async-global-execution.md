# Issue #273 verification

Candidate base: `a6b527017cf35fa2eb7c63ce38362fb702d21056`

## Delivered contract

- Query replicas optionally coordinate running async execution through one
  compact, CAS-managed lease index in their already-dedicated async state
  store. The index has a fixed maximum of 64 entries, bounded JSON size, eight
  CAS attempts, exact opaque tokens, and domain-separated tenant digests rather
  than raw tenant identities.
- A worker reserves shared execution capacity before it claims the durable job
  record. Global or per-tenant saturation maps to retryable scheduler pressure:
  the record remains queued, is not terminally failed, and is reconsidered by a
  later bounded page scan.
- Running workers renew the exact execution token with the existing job lease.
  Completion, failure, deadline, and failed claim paths release only that exact
  token. A crash is recovered by expiry; a stale renewal or release cannot
  mutate a successor token.
- The existing local tenant-fair scheduler remains process-local and is still
  responsible for bounded page selection. There is no global queue, leader,
  foreground metadata lookup, or data-plane change.
- The optional global limit pair is parsed before listener bind. Kubernetes
  enables it with four global executions and one per tenant, and the runbook
  requires a full drain-and-recreate rollout for changes because mixed old/new
  workers cannot enforce one shared lease contract.
- Documentation now separates cluster execution capacity from strict global
  dispatch fairness and includes the replica-to-lease lifecycle diagram.

## Red/green evidence

- The new lease tests first lacked the shared execution-lease APIs. They now
  exercise three independent store handles against one Rocks authority,
  same-tenant capacity, cross-tenant admission, expiry reclamation, stale-token
  fencing, and saturation that leaves a real durable record queued.
- A synchronized 32-owner CAS burst succeeds under the finite retry budget,
  proving the compact index remains live under one scheduling burst rather than
  relying on a process-local counter.
- CLI validation covers absent, partial, zero, excessive, contradictory,
  malformed, and valid global environment pairs. The metric test proves fixed
  outcomes do not export representative tenant, query, worker, or token data.

## Verification

- `mise run doctor` — PASS in the #273 jj workspace.
- `mise run spec-lint specs/issue-273-async-global-execution.spec.md` — PASS,
  quality 100%.
- Focused shared-lease, CLI, telemetry, and Kubernetes selectors — PASS.
- `mise run spec-lifecycle specs/issue-273-async-global-execution.spec.md` —
  PASS; every scenario selector executed at least one test.
- `mise run gate` — PASS: hooks, workspace tests, local selftest, upstream
  ADBC interoperability, and site build/output checks.
- Pre-push correctness, concurrency, security, and scope review — APPROVE; no
  P0/P1 findings. Residual operational requirement is intentional: replicas
  sharing leases must use synchronized wall clocks and drain/recreate for any
  global-limit rollout.
