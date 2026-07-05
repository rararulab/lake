#!/usr/bin/env bun
// test-env.ts — checkout-scoped, portless local integration-test deps.
//
//   bun scripts/test-env.ts up     create kind cluster, deploy localstack, start dynamic port-forward
//   bun scripts/test-env.ts down   stop port-forward and delete this checkout's cluster

import { $ } from "bun";
import { createHash } from "node:crypto";
import { closeSync, existsSync, openSync } from "node:fs";
import { mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

type State = {
  cluster: string;
  context: string;
  dynamodbEndpoint: string;
  portForwardPid: number;
};

const ROOT = resolve(".");
const SLUG = createHash("sha256").update(ROOT).digest("hex").slice(0, 10);
const CLUSTER = `lake-${SLUG}`;
const CONTEXT = `kind-${CLUSTER}`;
const STATE_DIR = ".lake";
const STATE_JSON = `${STATE_DIR}/test-env.json`;
const STATE_ENV = `${STATE_DIR}/test-env.env`;
const PORT_FORWARD_LOG = `${STATE_DIR}/localstack-port-forward.log`;

async function clusterExists(): Promise<boolean> {
  const out = await $`kind get clusters`.quiet().nothrow().text();
  return out.split("\n").includes(CLUSTER);
}

function processExists(pid: number): boolean {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
}

async function readState(): Promise<State | null> {
  if (!existsSync(STATE_JSON)) {
    return null;
  }
  return JSON.parse(await readFile(STATE_JSON, "utf8")) as State;
}

async function stopPortForward(): Promise<void> {
  const state = await readState();
  if (state?.portForwardPid && processExists(state.portForwardPid)) {
    process.kill(state.portForwardPid);
  }
}

async function waitForEndpoint(pid: number): Promise<string> {
  const deadline = Date.now() + 15_000;
  const pattern = /Forwarding from 127\.0\.0\.1:(\d+) -> 4566/;

  while (Date.now() < deadline) {
    const log = existsSync(PORT_FORWARD_LOG) ? await readFile(PORT_FORWARD_LOG, "utf8") : "";
    const match = pattern.exec(log);
    if (match) {
      return `http://127.0.0.1:${match[1]}`;
    }
    if (!processExists(pid)) {
      throw new Error(`kubectl port-forward exited before publishing an endpoint:\n${log}`);
    }
    await Bun.sleep(250);
  }

  throw new Error(`timed out waiting for kubectl port-forward endpoint; see ${PORT_FORWARD_LOG}`);
}

async function startPortForward(): Promise<State> {
  await stopPortForward();
  await writeFile(PORT_FORWARD_LOG, "");
  const logFd = openSync(PORT_FORWARD_LOG, "a");
  const proc = Bun.spawn(
    [
      "kubectl",
      "--context",
      CONTEXT,
      "port-forward",
      "service/localstack",
      ":4566",
    ],
    {
      stdin: "ignore",
      stdout: logFd,
      stderr: logFd,
    },
  );
  closeSync(logFd);
  proc.unref();

  const dynamodbEndpoint = await waitForEndpoint(proc.pid);
  const state = {
    cluster: CLUSTER,
    context: CONTEXT,
    dynamodbEndpoint,
    portForwardPid: proc.pid,
  };
  await writeFile(STATE_JSON, `${JSON.stringify(state, null, 2)}\n`);
  await writeFile(
    STATE_ENV,
    `LAKE_TEST_CLUSTER=${CLUSTER}\nLAKE_DYNAMODB_ENDPOINT=${dynamodbEndpoint}\n`,
  );
  return state;
}

async function up(): Promise<void> {
  await mkdir(STATE_DIR, { recursive: true });
  if (await clusterExists()) {
    console.log(`kind cluster '${CLUSTER}' already exists`);
  } else {
    await $`kind create cluster --name ${CLUSTER} --config deploy/kind-config.yaml`;
  }
  await $`kubectl --context ${CONTEXT} apply -f deploy/localstack.yaml`;
  await $`kubectl --context ${CONTEXT} rollout status deployment/localstack --timeout=180s`;
  const state = await startPortForward();
  console.log(`test env ready — DynamoDB (localstack) at ${state.dynamodbEndpoint}`);
  console.log(`endpoint env written to ${STATE_ENV}`);
}

async function down(): Promise<void> {
  await stopPortForward();
  if (await clusterExists()) {
    await $`kind delete cluster --name ${CLUSTER}`;
  } else {
    console.log(`kind cluster '${CLUSTER}' does not exist — nothing to do`);
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
