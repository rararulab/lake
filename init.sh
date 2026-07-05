#!/usr/bin/env bash
# init.sh — lake session-start health check
#
# Run this at the top of every new agent (or human) session. It is the
# single source of truth for "is my environment ready to code on lake?" —
# `just doctor` is a thin wrapper around this file.
#
# Exit code: 0 if every fatal check passes, non-zero otherwise. `gh` auth
# is warn-only — plenty of tasks work without it.

set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [ -t 1 ]; then
    RED=$'\033[31m'; GREEN=$'\033[32m'; YELLOW=$'\033[33m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
else
    RED=""; GREEN=""; YELLOW=""; BOLD=""; RESET=""
fi

FATAL_FAILURES=0

ok()      { printf '  %s[ ok ]%s   %s\n' "$GREEN"  "$RESET" "$1"; }
warn()    { printf '  %s[warn]%s   %s\n' "$YELLOW" "$RESET" "$1"; }
fail()    { printf '  %s[fail]%s   %s\n' "$RED"    "$RESET" "$1"; FATAL_FAILURES=$((FATAL_FAILURES + 1)); }
section() { printf '\n%s%s%s\n' "$BOLD" "$1" "$RESET"; }

# ----------------------------------------------------------------------------
# Toolchain — fatal
# ----------------------------------------------------------------------------
section "Toolchain"

if cargo --version >/dev/null 2>&1; then
    ok "cargo: $(cargo --version)"
else
    fail "cargo not found — install via https://rustup.rs"
fi

if cargo +nightly fmt --version >/dev/null 2>&1; then
    ok "nightly rustfmt: $(cargo +nightly fmt --version)"
else
    fail "nightly rustfmt missing — rustup toolchain install nightly --component rustfmt"
fi

if prek --version >/dev/null 2>&1; then
    ok "prek: $(prek --version)"
else
    fail "prek not found — brew install prek"
fi

# ----------------------------------------------------------------------------
# Git hooks — fatal
# ----------------------------------------------------------------------------
section "Git hooks"

for hook in pre-commit commit-msg; do
    if [ -f ".git/hooks/$hook" ]; then
        ok "$hook hook installed"
    else
        fail "$hook hook missing — run: prek install --hook-type pre-commit --hook-type commit-msg"
    fi
done

# ----------------------------------------------------------------------------
# Build — fatal
# ----------------------------------------------------------------------------
section "Build"

if cargo check --all-targets --quiet 2>/dev/null; then
    ok "cargo check passes"
else
    fail "cargo check fails — fix before starting new work"
fi

# ----------------------------------------------------------------------------
# GitHub — warn-only
# ----------------------------------------------------------------------------
section "GitHub"

if gh auth status >/dev/null 2>&1; then
    ok "gh authenticated"
    if git remote get-url origin >/dev/null 2>&1; then
        OPEN_ISSUES=$(gh issue list --label agent:claude --state open --json number --jq 'length' 2>/dev/null || echo "?")
        ok "open agent:claude issues: $OPEN_ISSUES"
    else
        warn "no git remote 'origin' — issue/PR flow unavailable until one is added"
    fi
else
    warn "gh not authenticated — issue/PR flow unavailable (gh auth login)"
fi

# ----------------------------------------------------------------------------
section "Result"
if [ "$FATAL_FAILURES" -eq 0 ]; then
    ok "environment ready"
    exit 0
else
    fail "$FATAL_FAILURES fatal check(s) failed"
    exit 1
fi
