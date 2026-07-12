#!/usr/bin/env bun
// test-integration.ts — run the `#[ignore]` LocalStack integration tests
// locally: bring up the emulator, run the ignored tests against it, tear down.
// In CI, `--external` consumes an already-provisioned LocalStack service.
//
//   mise run test-integration
//
// CI provisions LocalStack as a service container and runs the same tests
// directly (see .github/workflows/ci.yml); this is the local equivalent.

import { $ } from "bun";
import { readFile } from "node:fs/promises";

import { integrationEnvironment, redactedEndpoint } from "./test-integration-env";

async function endpoint(): Promise<string> {
  const env = await readFile(".lake/test-env.env", "utf8");
  const match = env.match(/^LAKE_DYNAMODB_ENDPOINT=(.+)$/m);
  if (!match) throw new Error("LAKE_DYNAMODB_ENDPOINT not found in .lake/test-env.env");
  return match[1]!.trim();
}

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} must be set in --external mode`);
  return value;
}

async function runIntegration(
  dynamoEndpoint: string,
  s3Endpoint: string,
  profile?: string,
): Promise<number> {
  console.log(`running ignored integration tests against ${redactedEndpoint(s3Endpoint)}`);
  const profileArgs = profile ? ["--profile", profile] : [];
  const proc = Bun.spawn(
    [
      "cargo",
      "nextest",
      "run",
      "-p",
      "lake-objects",
      "-p",
      "lake-sdk",
      "-p",
      "lake-meta",
      "-p",
      "lake-engine-lance",
      ...profileArgs,
      "--run-ignored",
      "ignored-only",
    ],
    {
      stdout: "inherit",
      stderr: "inherit",
      env: integrationEnvironment(process.env, dynamoEndpoint, s3Endpoint),
    },
  );
  return proc.exited;
}

if (process.argv.slice(2).includes("--external")) {
  process.exitCode = await runIntegration(
    requiredEnvironment("LAKE_DYNAMODB_ENDPOINT"),
    requiredEnvironment("LAKE_S3_ENDPOINT"),
    "ci",
  );
} else {
  await $`bun scripts/test-env.ts up`;
  try {
    const ep = await endpoint();
    process.exitCode = await runIntegration(ep, ep);
  } finally {
    await $`bun scripts/test-env.ts down`;
  }
}
