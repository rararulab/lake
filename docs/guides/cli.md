# CLI Standards

`lake` is the all-in-one operator and agent interface for the project. It is
not just the e2e self-check binary.

## Shape

- Use `clap` derive (`Parser`, `Subcommand`, `Args`, `ValueEnum`) for all
  command parsing.
- The binary owns subcommands. Expected top-level direction:
  - `lake self-check` — local ingest -> commit -> SQL smoke test.
  - `lake serve ...` — run lake services, including future Flight SQL /
    control-plane services.
  - `lake client ...` — client/operator commands that talk to a running server.
  - `lake admin ...` — explicit maintenance/debug operations.
- New user-visible behavior lands as a subcommand or subcommand group; do not
  grow one huge flag bag on the root command.

## Agent-Friendly Output

- Errors and progress go to stderr. Data goes to stdout.
- Commands that produce data must support a machine-readable mode before they
  are considered agent-friendly. Prefer `--format json` for structured output.
- JSON output must be stable enough for scripts: no decorative text, no tables,
  no ANSI color.
- Human table output is fine, but it is a presentation mode, not the contract.
- Exit code `0` means success; non-zero means the requested operation did not
  complete. Do not hide partial failures behind printed warnings.

## Configuration

- Precedence is CLI args > env vars > config file > defaults.
- Config defaults live at the application boundary. Domain crates do not hide
  operational defaults.
- All path/endpoint flags use explicit names such as `--data-dir`,
  `--catalog-url`, or `--endpoint`; avoid ambiguous positional arguments for
  operational commands.

## Async Runtime

- The CLI is async-first. Use `#[tokio::main]` at the binary boundary and keep
  I/O operations async through command handlers.
- Blocking local filesystem or CPU-heavy work must be isolated; do not hold
  async locks across `.await`.
- DataFusion APIs are async at the query boundary (`SessionContext::sql`,
  `DataFrame::collect`), so CLI query paths should remain async instead of
  wrapping them in sync helper APIs.

## References

- clap derive subcommands: <https://docs.rs/clap/latest/clap/_derive/_tutorial/index.html>
- Tokio runtime: <https://docs.rs/tokio>
- DataFusion SQL API: <https://datafusion.apache.org/library-user-guide/using-the-sql-api.html>
