#!/usr/bin/env bun
// test-integration.ts — run the `#[ignore]` LocalStack integration tests
// locally: bring up the emulator, run the ignored tests against it, tear down.
//
//   mise run test-integration
//
// CI provisions LocalStack as a service container and runs the same tests
// directly (see .github/workflows/ci.yml); this is the local equivalent.

import { $ } from "bun";
import { readFile } from "node:fs/promises";

async function endpoint(): Promise<string> {
  const env = await readFile(".lake/test-env.env", "utf8");
  const match = env.match(/^LAKE_DYNAMODB_ENDPOINT=(.+)$/m);
  if (!match) throw new Error("LAKE_DYNAMODB_ENDPOINT not found in .lake/test-env.env");
  return match[1]!.trim();
}

await $`bun scripts/test-env.ts up`;
try {
  const ep = await endpoint();
  console.log(`running ignored integration tests against ${ep}`);
  const proc = Bun.spawn(
    [
      "cargo",
      "nextest",
      "run",
      "-p",
      "lake-meta",
      "-p",
      "lake-engine-lance",
      "--run-ignored",
      "ignored-only",
    ],
    {
      stdout: "inherit",
      stderr: "inherit",
      env: {
        ...process.env,
        LAKE_DYNAMODB_ENDPOINT: ep,
        LAKE_S3_ENDPOINT: ep,
        AWS_ACCESS_KEY_ID: "test",
        AWS_SECRET_ACCESS_KEY: "test",
        AWS_REGION: "us-east-1",
        // Bypass an ambient corporate/system proxy for the loopback endpoint
        // (no-op on machines/CI without one) — see commands/mod.rs.
        LAKE_S3_PROXY_EXCLUDES: "localhost,127.0.0.1,::1",
        NO_PROXY: "localhost,127.0.0.1,::1",
        no_proxy: "localhost,127.0.0.1,::1",
      },
    },
  );
  const code = await proc.exited;
  if (code !== 0) process.exitCode = code;
} finally {
  await $`bun scripts/test-env.ts down`;
}
