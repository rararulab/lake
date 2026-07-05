#!/usr/bin/env bun
// doctor.ts — session-start health check.
//
// Run via `mise run doctor`. Single source of truth for "is my environment
// ready to code on lake?". Exit 0 if every fatal check passes.

import { $ } from "bun";

let failures = 0;

const ok = (msg: string) => console.log(`[ ok ] ${msg}`);
const warn = (msg: string) => console.log(`[warn] ${msg}`);
const fail = (msg: string) => {
  console.log(`[fail] ${msg}`);
  failures += 1;
};

async function passes(cmd: string[]): Promise<boolean> {
  const proc = Bun.spawn(cmd, { stdout: "ignore", stderr: "ignore" });
  return (await proc.exited) === 0;
}

if (await passes(["mise", "install", "--quiet"])) ok("mise tools installed");
else fail("mise install");

if (await passes(["cargo", "+nightly", "fmt", "--version"])) ok("nightly rustfmt");
else fail("nightly rustfmt — rustup toolchain install nightly --component rustfmt");

if (await passes(["cargo", "check", "--workspace", "--all-targets", "--quiet"])) ok("cargo check");
else fail("cargo check — fix before starting new work");

if (await passes(["jj", "root"])) {
  const root = (await $`jj root`.quiet().text()).trim();
  ok(`jj repo: ${root}`);
} else {
  fail("not a jj repo — run: jj git init --colocate");
}

if (await passes(["gh", "auth", "status"])) ok("gh authenticated");
else warn("gh not authenticated — issue/PR flow unavailable");

process.exit(failures === 0 ? 0 : 1);
