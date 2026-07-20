import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

const repository = process.env.GITHUB_REPOSITORY ?? "rararulab/lake";
const [, name = "lake"] = repository.split("/");
const base = process.env.GITHUB_ACTIONS === "true" ? `/${name}` : "";

const expectedFiles = [
  "dist/index.html",
  "dist/docs/index.html",
  "dist/docs/architecture/index.html",
  "dist/docs/guides/workflow/index.html",
  "dist/assets/architecture-overview.html",
  "dist/assets/iceberg-federation.html",
  "dist/search/index.html",
  "dist/pagefind/pagefind.js",
];

const missing = expectedFiles.filter(path => !existsSync(resolve(process.cwd(), path)));
if (missing.length > 0) {
  throw new Error(`Missing generated site output:\n${missing.join("\n")}`);
}

const home = readFileSync(resolve(process.cwd(), "dist/index.html"), "utf8");
const architecture = readFileSync(
  resolve(process.cwd(), "dist/docs/architecture/index.html"),
  "utf8"
);
const icebergFederation = readFileSync(
  resolve(process.cwd(), "dist/docs/design/iceberg-federation/index.html"),
  "utf8"
);

for (const text of [
  "An open-source lakehouse for embodied-AI data",
  "robot-training direction",
  "Documentation",
]) {
  if (!home.includes(text)) throw new Error(`Landing page is missing: ${text}`);
}

if (!architecture.includes('<h1 id="architecture">Architecture</h1>')) {
  throw new Error("The canonical architecture document was not rendered.");
}

for (const [document, asset] of [
  [architecture, "architecture-overview.html"],
  [architecture, "iceberg-federation.html"],
  [icebergFederation, "iceberg-federation.html"],
] as const) {
  const href = `${base}/assets/${asset}`;
  if (!document.includes(`href="${href}"`)) {
    throw new Error(`The published diagram link is missing: ${href}`);
  }
}

process.stdout.write(`Checked ${expectedFiles.length} generated site artifacts.\n`);
