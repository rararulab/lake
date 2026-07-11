# Verification: issue #69 Prometheus runtime metrics

- verdict: **PASS**
- score_authority: `verifier`
- base_sha: `6a2d94046a8f0d9ed24f53a3bd9c46e1689b8df5`
- head_sha: `e7cc17bebd9b67cc0ddafae9fcb056803bf28998`
- implementer_evidence: not consulted; commands were rerun independently from a clean workspace

## Revision and boundary

- `jj st` was clean before verification and `@-` was exactly candidate `e7cc17be`.
- The candidate contains one conventional commit: `feat(observability): expose bounded Prometheus metrics (#69)`.
- The 21 changed paths are all inside the spec allowlist: root Cargo files, README, Query/Metasrv/CLI crates, the two allowed docs, the issue plan, and the issue spec. No forbidden common/flight/meta/sdk, wire-format, Kubernetes, or OTLP path changed.

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
- The Axum router registers only `GET /metrics`; other paths and methods receive non-success routing responses. Exposition uses Prometheus text content type.
- The scrape listener and 30-second exporter upkeep loop run in one owned task. CLI cancellation drives both Flight and metrics shutdown; every server, shutdown, and metrics-failure branch cancels and joins the metrics task before return. Listener release is asserted by rebinding the socket.
- Query labels are limited to static admission, rejection, refresh phase, and outcome states. Metasrv labels are limited to static admission, campaign, maintenance stage, and outcome states. Call-site review found no SQL, tenant, namespace, table, URI, credential, operation ID, or arbitrary path value used as a label. Global labels are fixed service identity and build version.
- README and CLI/architecture docs cover the opt-in loopback address, localhost sidecar/node-agent model, endpoint, exported series and semantics, forbidden labels, and owned lifecycle.

## Commands and results

- `mise run doctor`: PASS.
- Three focused `cargo test -p <package> <selector> -- --nocapture` commands: PASS, exactly one matching unit test each.
- `cargo test -p lake-cli`: PASS (22 unit + 4 logging integration).
- `cargo test -p lake-query`: PASS (34 unit + 1 integration).
- `cargo test -p lake-metasrv`: PASS (61 unit passed, 1 explicit LocalStack-only ignored; 5 integration passed).
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`: PASS.
- Cold-state `mise run gate`: PASS in 27.47s.
- `mise run ci`: PASS in 37.39s, including rustdoc with warnings denied, spec self-test, site checks, CLI e2e, workspace tests, and 14/14 LocalStack integration tests.
- `mise run check-commits '6a2d9404..e7cc17be'`: PASS.
- `mise run ship` was not invoked because its final command is `jj git push`, which verifier instructions forbid. All non-mutating ship components (`ci` and candidate-range commit validation) were run independently and passed.

## Verdict

**PASS.** Candidate `e7cc17be` satisfies the three bound scenarios, stays within the declared boundary, keeps the scrape surface loopback-only and lifecycle-owned, exports bounded labels, documents the operational contract, and passes focused, package, strict lint, guarded lifecycle, gate, and full local CI verification.
