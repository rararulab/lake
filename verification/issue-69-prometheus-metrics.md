# Verification: issue #69 Prometheus runtime metrics

- verdict: **PASS**
- score_authority: `verifier`
- base_sha: `6a2d94046a8f0d9ed24f53a3bd9c46e1689b8df5`
- head_sha: `f1ff39dfb62a22be520e39cc6334eb01049e2005`
- implementer_evidence: not consulted; commands were rerun independently from a clean workspace

## Revision and boundary

- `jj st` was clean before incremental verification and `@-` was exactly candidate `f1ff39df`.
- The candidate chain contains the conventional feature commit plus `fix(observability): harden metrics lifecycle coverage (#69)`.
- The incremental `e7cc17be..f1ff39df` diff changes only CLI/Query/Metasrv implementation and tests, the CLI guide, issue spec, and this verification record. The final diff remains entirely inside the spec allowlist; no forbidden common/flight/meta/sdk, wire-format, Kubernetes, or OTLP path changed.

## Selector transition and lifecycle

| Selector | Base | Head | Independent focused result |
|---|---:|---:|---|
| `metrics_endpoint_is_loopback_only_and_owned_by_shutdown` | 0 | 1 | PASS: 1 passed |
| `query_metrics_cover_admission_and_catalog_refresh` | 0 | 1 | PASS: 1 passed |
| `metasrv_metrics_cover_append_leadership_and_maintenance` | 0 | 1 | PASS: 1 passed |

`mise run spec-lifecycle specs/issue-69-prometheus-metrics.spec.md` passed under the guarded runner: all three scenarios passed and every selector executed at least one real test.

## Targeted review

- Metrics remain opt-in: no recorder or HTTP listener is created when `LAKE_METRICS_ADDR` is absent.
- Address parsing requires a literal IP socket and `ip().is_loopback()`. Hostnames, wildcard addresses, and non-loopback IPs fail before the Flight server future is constructed or polled.
- The Axum router now dispatches all methods explicitly and serves exposition only when `method == GET` on `/metrics`. The selector proves GET returns 200, HEAD and POST return 405, and an unrelated GET path returns 404. Exposition uses Prometheus text content type.
- The scrape listener and 30-second exporter upkeep loop are no longer placed in a detached spawned task. The metrics future is pinned inside and owned directly by the outer server future. The selector aborts that outer future and proves the listener becomes immediately re-bindable, covering cancellation-by-drop as well as ordinary joined shutdown.
- Query metrics are now exercised through the production `serve_with_config_and_shutdown` path for initial refresh/readiness and shutdown withdrawal, plus the production catalog refresh loop for a background failure. Metasrv metrics are exercised through production `campaign_once`, bounded `sweep`, and the health-readiness monitor rather than direct telemetry helper calls.
- Query labels are limited to static admission, rejection, refresh phase, and outcome states. Metasrv labels are limited to static admission, campaign, maintenance stage, and outcome states. Call-site review found no SQL, tenant, namespace, table, URI, credential, operation ID, or arbitrary path value used as a label. Global labels are fixed service identity and build version.
- README and CLI/architecture docs cover the opt-in loopback address, localhost sidecar/node-agent model, endpoint, exported series and semantics, forbidden labels, and owned lifecycle.

## Commands and results

- `mise run doctor`: PASS.
- Three focused `cargo test -p <package> <selector> -- --nocapture` commands: PASS, exactly one matching unit test each.
- `cargo test -p lake-cli`: PASS (22 unit + 4 logging integration).
- `cargo test -p lake-query`: PASS (34 unit + 1 integration).
- `cargo test -p lake-metasrv`: PASS (61 unit passed, 1 explicit LocalStack-only ignored; 5 integration passed).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS.
- Latest-head `mise run gate`: PASS in 32.12s after removing `data/`.
- Earlier full `mise run ci` evidence on the feature candidate passed in 37.39s, including rustdoc with warnings denied, spec self-test, site checks, CLI e2e, workspace tests, and 14/14 LocalStack integration tests. Per incremental-verification direction it was not repeated after the test/lifecycle-only hardening commit; the latest full workspace gate was repeated and passed.
- `mise run check-commits '6a2d9404..e7cc17be'`: PASS for the feature commit; the incremental fix subject is also conventional by inspection.
- `mise run ship` was not invoked because its final command is `jj git push`, which verifier instructions forbid. All non-mutating ship components (`ci` and candidate-range commit validation) were run independently and passed.

## Verdict

**PASS.** Candidate `f1ff39df` satisfies the three bound scenarios, stays within the declared boundary, strictly limits exposition to `GET /metrics`, releases the listener when the owning future is aborted, exercises Query and Metasrv production transitions, exports bounded labels, documents the operational contract, and passes the latest focused, strict lint, guarded lifecycle, and full workspace gate verification.
