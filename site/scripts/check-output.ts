import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

const expectedFiles = [
  "dist/index.html",
  "dist/docs/index.html",
  "dist/docs/architecture/index.html",
  "dist/docs/guides/workflow/index.html",
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

for (const text of [
  "The lakehouse for embodied AI",
  "Design targets",
  "Documentation",
]) {
  if (!home.includes(text)) throw new Error(`Landing page is missing: ${text}`);
}

if (!architecture.includes('<h1 id="architecture">Architecture</h1>')) {
  throw new Error("The canonical architecture document was not rendered.");
}

process.stdout.write(`Checked ${expectedFiles.length} generated site artifacts.\n`);
