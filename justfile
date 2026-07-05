# ========================================================================================
# Default Recipe & Help
# ========================================================================================

[group("📒 Help")]
[private]
default:
    @just --list --list-heading '🌊 lake justfile manual page:\n'

[doc("show help")]
[group("📒 Help")]
help: default

# ========================================================================================
# Session Lifecycle — see init.sh
# ========================================================================================

[doc("session-start health check (toolchain, hooks, cargo check, gh)")]
[group("🩺 Lifecycle")]
doctor:
    @./init.sh

[doc("list open agent:claude issues from GitHub")]
[group("🩺 Lifecycle")]
agenda:
    @gh issue list --label agent:claude --state open --limit 30

# ========================================================================================
# Code Quality
# ========================================================================================

[doc("format Rust code (nightly rustfmt)")]
[group("👆 Code Quality")]
fmt:
    cargo +nightly fmt --all

[doc("check formatting without writing")]
[group("👆 Code Quality")]
fmt-check:
    cargo +nightly fmt --all -- --check

[doc("run cargo clippy with -D warnings")]
[group("👆 Code Quality")]
clippy:
    cargo clippy --all-targets --all-features --no-deps -- -D warnings

[doc("run all pre-commit hooks against all files")]
[group("👆 Code Quality")]
hooks:
    prek run --all-files

# ========================================================================================
# Build & Test
# ========================================================================================

[doc("run tests")]
[group("🧪 Test")]
test:
    cargo test --all-targets

[doc("end-to-end self-check: ingest -> commit -> SQL query")]
[group("🧪 Test")]
e2e:
    cargo run

[doc("full quality gate: hooks + test + e2e")]
[group("🧪 Test")]
gate: hooks test e2e
