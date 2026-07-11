spec: task
name: "structured-server-logging"
inherits: project
tags: [cli, observability, logging, operations]
---

## Intent

Server leadership, maintenance, shutdown, and refresh paths already emit
`tracing` events, but the `lake` binary never installs a subscriber. In a
deployed process those operational events disappear, leaving failures visible
only through RPC symptoms. The application boundary must initialize a
machine-readable stderr log sink before parsing commands or opening storage.

This closes one concrete part of `docs/architecture.md`'s remaining production
observability work without adding a metrics service or changing tier APIs.

## Decisions

- Initialize `tracing-subscriber` once at the start of the CLI process, before
  clap dispatch and `Context::open`.
- Default to newline-delimited JSON on stderr with ANSI disabled. Accept
  `LAKE_LOG_FORMAT=json|pretty`; reject any other value before storage or
  network setup.
- Use `RUST_LOG` when present and valid. Otherwise default to INFO for lake's
  binary, query, metasrv, and catalog targets without enabling noisy dependency
  logs. An invalid explicit filter fails startup.
- Emit one credential-free startup event containing only the package version.
  Never log argv, SQL, paths, environment values, bearer tokens, or credentials.
- Preserve stdout as the deterministic CLI result channel.

## Boundaries

### Allowed Changes
Cargo.toml
Cargo.lock
crates/lake-cli/**
docs/architecture.md
docs/guides/cli.md
docs/plans/2026-07-12-structured-server-logging.md
specs/issue-63-structured-logging.spec.md
verification/issue-63-structured-logging.md

### Forbidden
crates/lake-query/**
crates/lake-metasrv/**
crates/lake-meta/**
Flight or SQL protocols
HTTP health or metrics endpoints
OpenTelemetry exporters
logging request payloads, SQL, argv, credentials, or tokens

## Completion Criteria

Scenario: Server binary emits structured startup logs to stderr
  Test:
    Package: lake-cli
    Filter: binary_emits_json_startup_log_before_command_dispatch
  Given JSON logging and an INFO filter for the lake target
  When the binary starts with an invalid command
  Then its first stderr line is valid JSON containing the startup event and version while stdout stays empty

Scenario: Invalid logging configuration fails before storage setup
  Test:
    Package: lake-cli
    Filter: invalid_log_configuration_fails_before_storage_setup
  Given an invalid log format and a data directory that does not exist
  When the binary starts
  Then it reports the logging configuration error and does not create the data directory

Scenario: Logging defaults are production structured
  Test:
    Package: lake-cli
    Filter: binary_defaults_to_json_logging
  Given neither logging environment variable is set
  When the binary starts
  Then its startup event is JSON on stderr and stdout stays empty

Scenario: Pretty logging is plain stderr
  Test:
    Package: lake-cli
    Filter: pretty_logging_stays_on_plain_stderr
  Given pretty logging is explicitly selected
  When the binary starts
  Then the startup event uses stderr without ANSI and stdout stays empty

Scenario: Default filter exposes Lake operations without dependency noise
  Test:
    Package: lake-cli
    Filter: default_filter_enables_lake_targets_only
  Given INFO events from all four Lake targets and an external dependency
  When the built-in filter is applied
  Then every Lake event is emitted and the dependency event is suppressed

## Out of Scope

- Prometheus metrics, distributed traces, or an OTLP exporter.
- HTTP readiness/liveness endpoints.
- Per-request spans or changes to existing event fields.
- Runtime log-filter reload.
