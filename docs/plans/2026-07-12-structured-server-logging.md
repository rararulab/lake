# Structured server logging implementation plan

**Goal:** Make existing operational tracing events observable as structured
stderr logs from every `lake` process.

**Architecture:** Subscriber ownership belongs at the binary application
boundary. Domain crates continue emitting `tracing` events without depending
on output format or deployment configuration.

## Task 1: Prove the process-level logging contract

1. Add integration tests that launch the compiled binary as a subprocess.
2. Assert JSON mode emits a parseable startup record before clap rejects an
   invalid command and never writes logs to stdout.
3. Assert an invalid format fails before a requested data directory is
   created.
4. Run both tests and confirm RED because no subscriber/configuration exists.

## Task 2: Install the subscriber at startup

1. Add `tracing-subscriber` with `env-filter`, `fmt`, and `json` features.
2. Parse the two environment boundaries with explicit defaults and errors.
3. Install JSON or pretty stderr output before command parsing.
4. Emit one version-only startup event and confirm both tests GREEN.

## Task 3: Document and verify

1. Document defaults, environment overrides, stderr/stdout separation, and
   the no-sensitive-fields boundary.
2. Run spec lifecycle, strict clippy, and the full local gate.
3. Obtain independent verifier and reviewer approval, then ship and merge.
