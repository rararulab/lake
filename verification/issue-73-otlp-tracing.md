# Verification: issue #73 bounded OTLP distributed tracing

Candidate base: `fd323e557f5820b20a28a66cc48dff5efadd0f38`

## Functional contract

- `mise run spec-lifecycle specs/issue-73-otlp-tracing.spec.md` — PASS; all
  four selectors executed at least one test.
- Default configuration starts no exporter. Invalid endpoint configuration
  fails before the command future is polled. An unavailable collector does not
  stop the command, and both normal completion and cancellation own exporter
  shutdown within the configured bound.
- W3C propagation is restricted to `traceparent` and `tracestate`.
- The integration tests prove client → Query → Metasrv and
  client → follower → leader trace continuity. A real Flight SQL
  `GetFlightInfo` + `DoGet` round trip is included.
- Every exported production server span inspected by the tests has exactly
  `rpc.system`, `rpc.service`, `rpc.method`, and `rpc.outcome`. No SQL, tenant,
  principal, namespace, table, URI/path, credential, media type, action body,
  or operation ID is exported.
- Response spans remain alive until stream completion and record late errors
  or caller cancellation instead of reporting early success.

## Regression and operations

- `cargo test -p lake-cli -p lake-flight -p lake-query -p lake-metasrv` — PASS
  (one pre-existing LocalStack-only ignored test).
- `cargo clippy -p lake-cli -p lake-flight -p lake-query -p lake-metasrv --all-targets -- -D warnings` — PASS.
- `mise run k8s-validate` — PASS, 11/11 resources valid under strict Kubernetes
  1.32 schemas.
- `mise run gate` — PASS before review fixes: workspace all-target tests, local
  end-to-end self-check, hooks, and site checks. The affected packages and spec
  lifecycle were rerun after review fixes as listed above.

The macOS linker emits its existing large `__eh_frame` warning for the very
large debug test binaries; it is non-fatal and unrelated to this change.

## Independent review

The first correctness review requested changes for incomplete public SQL span
coverage, a missing client-to-Query assertion, helper-only CLI lifecycle tests,
and response spans ending before relayed streams. The candidate now addresses
all four: all supported Query Flight operations are traced, SQL and append
chains include the client trace, the outer process lifecycle is exercised, and
the shared response wrapper owns span outcome through completion/error/drop.

