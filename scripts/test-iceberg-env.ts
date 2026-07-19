#!/usr/bin/env bun
// test-iceberg-env.ts — checkout-scoped Apache Iceberg REST + MinIO fixture.
//
// This proves Lake's external Iceberg path against Apache's own REST fixture,
// not against Lake's in-process protocol fake. Containers bind ephemeral host
// ports and are named from the checkout path, so parallel worktrees do not
// collide.
//
//   bun scripts/test-iceberg-env.ts up
//   bun scripts/test-iceberg-env.ts down

import { $ } from "bun";
import { createHash } from "node:crypto";
import { mkdir, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const REST_IMAGE = "apache/iceberg-rest-fixture:1.10.1";
const MINIO_IMAGE = "minio/minio:RELEASE.2025-05-24T17-08-30Z";
const MC_IMAGE = "minio/mc:RELEASE.2025-05-21T01-59-54Z";
const SLUG = createHash("sha256").update(resolve(".")).digest("hex").slice(0, 10);
const NETWORK = `lake-iceberg-${SLUG}`;
const MINIO = `lake-iceberg-minio-${SLUG}`;
const REST = `lake-iceberg-rest-${SLUG}`;
const STATE_DIR = ".lake";
const STATE_ENV = `${STATE_DIR}/test-iceberg-env.env`;
const WAREHOUSE = "s3://icebergdata/demo";
const MINIO_REGION = "us-east-1";
const MINIO_USER = "admin";
const MINIO_PASSWORD = "password";
const MC_ALIAS = "lake";
const MC_HOST = `http://${MINIO_USER}:${MINIO_PASSWORD}@minio:9000`;

async function containerId(container: string): Promise<string> {
  const out = await $`docker ps -aq --filter name=^/${container}$`.quiet().nothrow().text();
  return out.trim();
}

async function networkExists(): Promise<boolean> {
  const result = await $`docker network inspect ${NETWORK}`.quiet().nothrow();
  return result.exitCode === 0;
}

async function hostPort(container: string, port: number): Promise<string> {
  const mapping = (await $`docker port ${container} ${port}/tcp`.quiet().text()).trim();
  const hostPort = mapping.split("\n")[0]?.split(":").pop();
  if (!hostPort) throw new Error(`could not read mapped port for ${container}: ${mapping}`);
  return hostPort;
}

async function waitFor(endpoint: string, path: string, name: string): Promise<void> {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    const response = await fetch(`${endpoint}${path}`).catch(() => null);
    if (response?.ok) return;
    await Bun.sleep(500);
  }
  throw new Error(`timed out waiting for ${name} at ${endpoint}`);
}

async function ensureNetwork(): Promise<void> {
  if (!(await networkExists())) await $`docker network create ${NETWORK}`.quiet();
}

async function ensureMinio(): Promise<string> {
  if (!(await containerId(MINIO))) {
    await $`docker run -d --name ${MINIO} --network ${NETWORK} --network-alias minio -p 9000 -e MINIO_ROOT_USER=${MINIO_USER} -e MINIO_ROOT_PASSWORD=${MINIO_PASSWORD} ${MINIO_IMAGE} server /data`.quiet();
  }
  const endpoint = `http://127.0.0.1:${await hostPort(MINIO, 9000)}`;
  await waitFor(endpoint, "/minio/health/live", "MinIO");
  return endpoint;
}

async function makeBucketPublic(): Promise<void> {
  await $`docker run --rm --network ${NETWORK} -e MC_HOST_lake=${MC_HOST} ${MC_IMAGE} mb --ignore-existing ${MC_ALIAS}/icebergdata`.quiet();
  await $`docker run --rm --network ${NETWORK} -e MC_HOST_lake=${MC_HOST} ${MC_IMAGE} anonymous set download ${MC_ALIAS}/icebergdata`.quiet();
}

async function ensureRest(minioEndpoint: string): Promise<string> {
  if (!(await containerId(REST))) {
    const catalogS3Endpoint = minioEndpoint.replace("127.0.0.1", "gateway.localhost");
    await $`docker run -d --name ${REST} --network ${NETWORK} --add-host gateway.localhost:host-gateway -p 8181 -e AWS_ACCESS_KEY_ID=${MINIO_USER} -e AWS_SECRET_ACCESS_KEY=${MINIO_PASSWORD} -e AWS_REGION=${MINIO_REGION} -e CATALOG_CATALOG__IMPL=org.apache.iceberg.jdbc.JdbcCatalog -e CATALOG_URI=jdbc:sqlite:file:/tmp/iceberg_rest_mode=memory -e CATALOG_WAREHOUSE=${WAREHOUSE} -e CATALOG_IO__IMPL=org.apache.iceberg.aws.s3.S3FileIO -e CATALOG_S3_ENDPOINT=${catalogS3Endpoint} -e CATALOG_S3_PATH__STYLE__ACCESS=true ${REST_IMAGE}`.quiet();
  }
  const endpoint = `http://127.0.0.1:${await hostPort(REST, 8181)}`;
  await waitFor(endpoint, "/v1/config", "Apache Iceberg REST catalog");
  return endpoint;
}

async function up(): Promise<void> {
  await mkdir(STATE_DIR, { recursive: true });
  await ensureNetwork();
  const minioEndpoint = await ensureMinio();
  await makeBucketPublic();
  const restEndpoint = await ensureRest(minioEndpoint);
  await writeFile(
    STATE_ENV,
    [
      `LAKE_ICEBERG_TEST_REST_ENDPOINT=${restEndpoint}`,
      `LAKE_ICEBERG_TEST_WAREHOUSE=${WAREHOUSE}`,
      `LAKE_ICEBERG_TEST_S3_ENDPOINT=${minioEndpoint}`,
      "",
    ].join("\n"),
  );
  console.log(`test env ready — Apache Iceberg REST catalog at ${restEndpoint}`);
  console.log(`test env ready — public MinIO warehouse at ${minioEndpoint}`);
  console.log(`endpoints written to ${STATE_ENV}`);
}

async function removeContainer(container: string): Promise<void> {
  if (await containerId(container)) await $`docker rm -f ${container}`.quiet();
}

async function down(): Promise<void> {
  await removeContainer(REST);
  await removeContainer(MINIO);
  if (await networkExists()) await $`docker network rm ${NETWORK}`.quiet();
  await rm(STATE_ENV, { force: true });
  console.log("removed Apache Iceberg REST integration environment");
}

switch (process.argv[2]) {
  case "up":
    await up();
    break;
  case "down":
    await down();
    break;
  default:
    console.error("usage: test-iceberg-env.ts <up|down>");
    process.exit(2);
}
