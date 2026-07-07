#!/usr/bin/env bun
// test-env.ts — checkout-scoped, portless local integration dependencies.
//
// Runs localstack (DynamoDB + S3) directly in Docker — no kind/k8s. A single
// container does not warrant a Kubernetes cluster, and it starts in seconds.
// Portless + parallel-safe: the container is named per checkout (path hash)
// and bound to an ephemeral host port, both discovered dynamically and written
// to `.lake/test-env.env` so multiple worktrees never collide.
//
//   bun scripts/test-env.ts up     start localstack, write the endpoint
//   bun scripts/test-env.ts down   stop and remove this checkout's container
//
// Community image `:3` is pinned deliberately: `:latest` now requires a
// LOCALSTACK_AUTH_TOKEN and exits without one.

import { $ } from "bun";
import { createHash } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const IMAGE = "localstack/localstack:3";
const SLUG = createHash("sha256").update(resolve(".")).digest("hex").slice(0, 10);
const CONTAINER = `lake-localstack-${SLUG}`;
const STATE_DIR = ".lake";
const STATE_ENV = `${STATE_DIR}/test-env.env`;

async function containerId(): Promise<string> {
  const out = await $`docker ps -aq --filter name=^/${CONTAINER}$`.quiet().nothrow().text();
  return out.trim();
}

/** The host port Docker mapped to localstack's 4566. */
async function hostPort(): Promise<string> {
  const mapping = (await $`docker port ${CONTAINER} 4566/tcp`.quiet().text()).trim();
  // e.g. "0.0.0.0:53142" (possibly multiple lines for v4/v6) — take the first.
  const port = mapping.split("\n")[0]?.split(":").pop();
  if (!port) throw new Error(`could not read mapped port for ${CONTAINER}: ${mapping}`);
  return port;
}

async function waitHealthy(endpoint: string): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const res = await fetch(`${endpoint}/_localstack/health`).catch(() => null);
    if (res?.ok) {
      const health = (await res.json()) as { services?: Record<string, string> };
      const s = health.services ?? {};
      const ready = (v?: string) => v === "available" || v === "running";
      if (ready(s.dynamodb) && ready(s.s3)) return;
    }
    await Bun.sleep(500);
  }
  throw new Error(`timed out waiting for localstack health at ${endpoint}`);
}

async function up(): Promise<void> {
  await mkdir(STATE_DIR, { recursive: true });
  if (!(await containerId())) {
    await $`docker run -d --name ${CONTAINER} -p 4566 -e SERVICES=dynamodb,s3 ${IMAGE}`.quiet();
  } else {
    console.log(`container '${CONTAINER}' already exists`);
  }
  const endpoint = `http://127.0.0.1:${await hostPort()}`;
  await waitHealthy(endpoint);
  await writeFile(STATE_ENV, `LAKE_DYNAMODB_ENDPOINT=${endpoint}\n`);
  console.log(`test env ready — DynamoDB + S3 (localstack) at ${endpoint}`);
  console.log(`endpoint written to ${STATE_ENV}`);
}

async function down(): Promise<void> {
  if (await containerId()) {
    await $`docker rm -f ${CONTAINER}`.quiet();
    console.log(`removed container '${CONTAINER}'`);
  } else {
    console.log(`container '${CONTAINER}' does not exist — nothing to do`);
  }
  await rm(STATE_DIR, { recursive: true, force: true });
}

switch (process.argv[2]) {
  case "up":
    await up();
    break;
  case "down":
    await down();
    break;
  default:
    console.error("usage: test-env.ts <up|down>");
    process.exit(2);
}
