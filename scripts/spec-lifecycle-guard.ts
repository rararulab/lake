#!/usr/bin/env bun
// spec-lifecycle-guard.ts — zero-match guard around `agent-spec lifecycle`.
//
// agent-spec (<= 0.3.0) reports a scenario as passed even when the cargo
// test filter matches ZERO tests ("running 0 tests" / "0 passed; N filtered
// out"). That false-green converts "unverified" into "verified" — upstream:
// https://github.com/ZhangHanDong/agent-spec/issues/4
//
// This wrapper runs the lifecycle with JSON output, then inspects the raw
// runner output captured in each scenario's test evidence. A scenario whose
// test runs executed zero tests (0 passed AND 0 failed across every
// `test result:` line) is a FAIL, regardless of agent-spec's own verdict.
// The guard fails CLOSED: a report it cannot parse, or one carrying no
// scenario results at all, is never treated as verified.
//
// Exit-code contract:
//   0 — lifecycle passed AND every Test selector executed >= 1 test
//   1 — verification failure (lifecycle itself failed, or a selector
//       matched zero tests)
//   2 — infra/usage error (bad args; agent-spec missing; report is
//       malformed JSON or missing .verification.results — schema drift)
//
// `mise run spec-lifecycle` routes through this script; `mise run
// spec-selftest` asserts it rejects specs/fixtures/zero-match.spec.md.

interface Evidence {
  type?: string;
  stdout?: string;
}

interface ScenarioResult {
  scenario_name?: string;
  verdict?: string;
  evidence?: Evidence[];
}

interface Report {
  stage?: string;
  passed?: boolean;
  verification?: { spec_name?: string; results?: ScenarioResult[] };
}

const spec = process.argv[2];
if (!spec || process.argv.length !== 3) {
  console.error("usage: spec-lifecycle-guard.ts <spec-file>");
  process.exit(2);
}

const proc = Bun.spawn(
  ["agent-spec", "lifecycle", spec, "--code", ".", "--change-scope", "worktree", "--format", "json"],
  { stdout: "pipe", stderr: "inherit" },
);
const raw = await new Response(proc.stdout).text();
const lifecycleExit = await proc.exited;

let report: Report;
try {
  report = JSON.parse(raw) as Report;
} catch {
  console.error(
    "spec-lifecycle-guard: report is malformed JSON — refusing to treat it as verified",
  );
  console.error(raw);
  process.exit(2);
}

// Human-readable summary (the JSON run carries the runner output the guard
// needs, so we run once and render it ourselves).
console.log("=== Lifecycle Report (guarded) ===");
console.log(
  `Spec: ${report.verification?.spec_name ?? "unknown"}  stage: ${report.stage}  passed: ${report.passed}`,
);
const results = report.verification?.results;
for (const r of results ?? []) {
  console.log(`  [${(r.verdict ?? "?").toUpperCase()}] ${r.scenario_name}`);
}

if (lifecycleExit !== 0) {
  console.error(`spec-lifecycle-guard: FAIL — agent-spec lifecycle exited ${lifecycleExit}`);
  process.exit(1);
}

// Fail closed on schema drift: a green lifecycle whose report carries no
// scenario results verified nothing.
if (!Array.isArray(results) || results.length === 0) {
  console.error(
    "spec-lifecycle-guard: report has no scenario results (.verification.results) — refusing to treat it as verified",
  );
  process.exit(2);
}

// Zero-match detection: for every scenario with test evidence, sum executed
// tests (passed + failed) across all `test result:` lines in the captured
// runner stdout. Zero executed tests means the selector resolved to nothing.
// Scenarios WITHOUT test_output evidence are skipped on purpose: boundary
// checks carry none.
const zeroMatch = results
  .filter((r) => (r.evidence ?? []).some((e) => e.type === "test_output"))
  .filter((r) => {
    const executed = (r.evidence ?? [])
      .filter((e) => e.type === "test_output")
      .flatMap((e) => [...(e.stdout ?? "").matchAll(/(\d+) passed; (\d+) failed/g)])
      .reduce((sum, m) => sum + Number(m[1]) + Number(m[2]), 0);
    return executed === 0;
  })
  .map((r) => r.scenario_name ?? "<unnamed scenario>");

if (zeroMatch.length > 0) {
  console.error("");
  console.error(
    "spec-lifecycle-guard: FAIL — Test selector(s) matched ZERO tests (0 passed; filtered out):",
  );
  for (const name of zeroMatch) console.error(`  - ${name}`);
  console.error(
    "Every lane-1 Test: selector must resolve to >=1 real test function — see specs/README.md.",
  );
  process.exit(1);
}

console.log("spec-lifecycle-guard: OK — every Test selector executed >=1 test");
