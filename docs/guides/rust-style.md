# Rust Code Style

## Style Direction

Write Rust in the style region defined by BurntSushi, dtolnay, and Niko Matsakis. This means: functional-first, iterator chains over imperative loops,
combinators on Option/Result for simple transforms, `match` for complex branching, immutable by
default, early returns with `?` to keep the happy path flat.

If you're unsure whether a pattern fits, ask: "Would BurntSushi write it this way in `ripgrep`?"
If yes, it's probably right. If it feels like enterprise Java, it's probably wrong.

## Toolchain Constraints

These are zero-ambiguity rules — not style preferences, but mechanical requirements.

### Error Handling
- `snafu` exclusively in domain code — never `thiserror` or manual `impl Error`
- `anyhow` allowed only at the application boundary (`crates/lake-cli`)
- Error enum pattern: `#[derive(Debug, Snafu)]` + `#[snafu(visibility(pub))]`
- Naming: per-crate `{CrateName}Error` enums (`MetaError` in `lake-meta`, `ManifestError` in
  `lake-manifest`), variants use `#[snafu(display("..."))]`
- Propagation: `.context(XxxSnafu)?` or `.whatever_context("msg")?`
- Per-crate alias: each crate defines `pub type Result<T> = std::result::Result<T, {CrateName}Error>`

### Struct Construction — `bon::Builder`
- 3+ fields: `#[derive(bon::Builder)]` — no manual `fn new()` constructors
- Config structs: pair with `Deserialize`, never `#[derive(Default)]` — defaults come from
  explicit config, not code
- Cross-module: `Foo::builder().field(val).build()`, not struct literals
- Within the defining module: struct literals are fine
- `Option<T>` fields auto-default to `None` in bon — no need for `#[builder(default)]`
- 1-2 field structs: direct construction, no builder needed

### Type Patterns
- Trait objects: `pub type XRef = Arc<dyn X>` alias (e.g. `MetaStoreRef`)
- No hardcoded config defaults sprinkled through domain code — configuration is explicit
  and auditable at the application boundary

### Async
- Lake is async-first at I/O and query boundaries. DataFusion query APIs are
  async; metastore/network/object-store operations stay async instead of being
  hidden behind sync wrappers.
- `#[async_trait]` + `Send + Sync` bound on async trait definitions
- Blocking CPU/filesystem work is isolated from async control flow; do not hold
  locks across `.await`.
- Logging: `tracing` macros + `#[instrument(skip_all)]`

### Rustdoc
- Crate and module roots use `//!` to describe purpose, invariants, and the
  smallest useful example.
- New or modified public items require `///` docs that explain contract,
  invariants, errors, and async/blocking behavior when relevant.
- Public API docs should include examples when the example clarifies why the
  API exists. Keep examples doctestable unless they require external services;
  mark external-service examples `no_run` and explain the requirement.
- `cargo +nightly doc --workspace --no-deps --document-private-items` runs with
  `RUSTDOCFLAGS="-D warnings"` in the `mise run doc`/CI gate.

### Code Organization
- `mod.rs` only for re-exports + `//!` module docs — when a module grows into a directory,
  split logic into sub-files
- Imports: `std` → external crates → internal (`crate::` / `super::`)
- No wildcard imports (`use foo::*`)
- `.expect("context")` over `unwrap()` in non-test code
- Apache-2.0 license header on every source file
