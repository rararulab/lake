#!/usr/bin/env bun
// check-conventional-commit.ts — validate commit messages against
// Conventional Commits (https://www.conventionalcommits.org/).
//
// Two modes:
//   bun scripts/check-conventional-commit.ts <commit-msg-file>
//     prek commit-msg hook: validate the message being written.
//   bun scripts/check-conventional-commit.ts --range <rev-range>
//     CI: validate the subject of every commit in the git range.

import { $ } from "bun";

// type(scope): description | type: description | type(scope)!: description
const PATTERN =
  /^(feat|fix|refactor|docs|test|chore|ci|perf|style|build|revert)(\([a-z0-9_-]+\))?!?: .+/;
const EXEMPT = /^(Merge |Revert )/;

function explain(got: string): void {
  console.error("❌ Commit message does not follow Conventional Commits format.");
  console.error("");
  console.error("  Expected: <type>(<scope>): <description>");
  console.error(`  Got:      ${got}`);
  console.error("");
  console.error(
    "  Allowed types: feat, fix, refactor, docs, test, chore, ci, perf, style, build, revert",
  );
  console.error("  Examples:");
  console.error("    feat(catalog): add manifest provider cache");
  console.error("    fix(meta): make CAS reject missing keys");
  console.error("    docs: update AGENT.md catalog");
}

const valid = (subject: string) => EXEMPT.test(subject) || PATTERN.test(subject);

const args = process.argv.slice(2);

if (args[0] === "--range") {
  const range = args[1];
  if (!range) {
    console.error("usage: check-conventional-commit.ts --range <rev-range>");
    process.exit(2);
  }
  const shas = (await $`git rev-list ${range}`.text()).trim().split("\n").filter(Boolean);
  let bad = 0;
  for (const sha of shas) {
    const subject = (await $`git log -1 --format=%s ${sha}`.text()).trim();
    if (!valid(subject)) {
      console.error(`::error::commit ${sha} does not follow Conventional Commits: ${subject}`);
      bad += 1;
    }
  }
  process.exit(bad === 0 ? 0 : 1);
}

const msgFile = args[0];
if (!msgFile) {
  console.error("usage: check-conventional-commit.ts <commit-msg-file> | --range <rev-range>");
  process.exit(2);
}
const firstLine = (await Bun.file(msgFile).text()).split("\n", 1)[0] ?? "";
if (!valid(firstLine)) {
  explain(firstLine);
  process.exit(1);
}
