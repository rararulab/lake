# ADBC and Flight interoperability plan

## Goal

Prove Lake's public SQL protocol with an independently implemented upstream
client. Keep the server fixture and long-running Flight coverage in Rust, and
use the official pinned ADBC Flight SQL Python wheel only as a black-box client.

## Decisions

- `adbc-driver-flightsql` and `pyarrow` are exact direct dependencies in a
  dedicated uv project with a committed lockfile. They do not enter Lake's
  runtime or Rust dependency graph.
- ADBC covers interactive Flight SQL execution, typed multi-batch Arrow
  results, bearer propagation, and client-visible read-only errors.
- Standard Rust Arrow Flight covers `PollFlightInfo`, endpoint redemption, and
  `CancelFlightInfo`. Those are Flight RPCs; the ordinary ADBC DB-API query
  surface does not expose the full lifecycle.
- The conformance process is loopback-only, uses ephemeral state and ports,
  has fixed timeouts, and is part of `mise run gate`.

## Sequence

1. Add failing black-box tests around a real Query listener.
2. Pin and lock the upstream client runner.
3. Correct any Flight SQL compatibility gaps exposed by the upstream driver.
4. Add the bounded task to the gate and document the verified matrix.
5. Run spec lifecycle, clippy, and the full gate.

