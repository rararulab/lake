#!/usr/bin/env bun
// test-iceberg-integration.ts — run Apache REST + MinIO Iceberg coverage.
//
// Local mode owns a checkout-scoped Docker fixture. `--external` consumes
// caller-supplied endpoints for environments that already manage the fixture.

import { $ } from "bun";
import { readFile } from "node:fs/promises";

import { icebergIntegrationEnvironment, redactedEndpoint } from "./test-iceberg-integration-env";

type FixtureEndpoints = {
  restEndpoint: string;
  warehouse: string;
  s3Endpoint: string;
};

function requiredEnvironment(name: string): string {
  const value = process.env[name]?.trim();
  if (!value) throw new Error(`${name} must be set in --external mode`);
  return value;
}

function fromEnvironment(): FixtureEndpoints {
  return {
    restEndpoint: requiredEnvironment("LAKE_ICEBERG_TEST_REST_ENDPOINT"),
    warehouse: requiredEnvironment("LAKE_ICEBERG_TEST_WAREHOUSE"),
    s3Endpoint: requiredEnvironment("LAKE_ICEBERG_TEST_S3_ENDPOINT"),
  };
}

async function fromStateFile(): Promise<FixtureEndpoints> {
  const state = await readFile(".lake/test-iceberg-env.env", "utf8");
  const value = (name: string) => {
    const match = state.match(new RegExp(`^${name}=(.+)$`, "m"));
    if (!match) throw new Error(`${name} not found in .lake/test-iceberg-env.env`);
    return match[1]!.trim();
  };
  return {
    restEndpoint: value("LAKE_ICEBERG_TEST_REST_ENDPOINT"),
    warehouse: value("LAKE_ICEBERG_TEST_WAREHOUSE"),
    s3Endpoint: value("LAKE_ICEBERG_TEST_S3_ENDPOINT"),
  };
}

async function runIntegration(endpoints: FixtureEndpoints, profile?: string): Promise<number> {
  console.log(
    `running Apache REST Iceberg integration against ${redactedEndpoint(endpoints.restEndpoint)} and ${redactedEndpoint(endpoints.s3Endpoint)}`,
  );
  const profileArgs = profile ? ["--profile", profile] : [];
  const proc = Bun.spawn(
    [
      "cargo",
      "nextest",
      "run",
      "-p",
      "lake-query",
      ...profileArgs,
      "--run-ignored",
      "ignored-only",
      "apache_rest_catalog_with_minio_is_queryable",
    ],
    {
      stdout: "inherit",
      stderr: "inherit",
      env: icebergIntegrationEnvironment(
        process.env,
        endpoints.restEndpoint,
        endpoints.warehouse,
        endpoints.s3Endpoint,
      ),
    },
  );
  return proc.exited;
}

if (process.argv.slice(2).includes("--external")) {
  process.exitCode = await runIntegration(fromEnvironment(), "ci");
} else {
  try {
    await $`bun scripts/test-iceberg-env.ts up`;
    process.exitCode = await runIntegration(await fromStateFile());
  } finally {
    await $`bun scripts/test-iceberg-env.ts down`;
  }
}
