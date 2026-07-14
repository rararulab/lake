#!/usr/bin/env bun
// spec-selftest.ts — regression lock for the zero-match false-green.
//
// The lifecycle gate must REJECT specs/fixtures/zero-match.spec.md with
// exit 1. Exit 0 means the false-green is back; exit 2 means the gate could
// not even run (agent-spec missing, malformed report) — both are failures.

const scopeTest = Bun.spawn(["bun", "test", "scripts/spec-lifecycle-guard.test.ts"], {
  stdout: "inherit",
  stderr: "inherit",
});
if ((await scopeTest.exited) !== 0) {
  console.log("spec-selftest: FAIL — lifecycle guard Jujutsu scope regression test failed");
  process.exit(1);
}

const proc = Bun.spawn(
  ["bun", "scripts/spec-lifecycle-guard.ts", "specs/fixtures/zero-match.spec.md"],
  { stdout: "inherit", stderr: "inherit" },
);
const rc = await proc.exited;

switch (rc) {
  case 1:
    console.log("spec-selftest: OK — lifecycle gate rejected the zero-match fixture (exit 1)");
    break;
  case 0:
    console.log(
      "spec-selftest: FAIL — zero-match fixture passed the lifecycle gate (false-green is back)",
    );
    process.exit(1);
  default:
    console.log(`spec-selftest: FAIL — guard could not run the gate (infra error, exit ${rc})`);
    process.exit(1);
}
