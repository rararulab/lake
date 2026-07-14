import { expect, test } from "bun:test";
import { chmod, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

test("lifecycle guard asks agent-spec for the current Jujutsu change set", async () => {
  const directory = await mkdtemp(join(tmpdir(), "lake-spec-lifecycle-"));
  const argumentsPath = join(directory, "arguments");
  const agentSpecPath = join(directory, "agent-spec");

  try {
    await writeFile(
      agentSpecPath,
      `#!/bin/sh
printf '%s\\n' "$@" > "$LAKE_GUARD_ARGUMENTS"
printf '%s\\n' '{"stage":"complete","passed":true,"verification":{"spec_name":"mock","results":[{"scenario_name":"mock","verdict":"pass","evidence":[]}]}}'
`,
    );
    await chmod(agentSpecPath, 0o755);

    const proc = Bun.spawn(
      ["bun", "scripts/spec-lifecycle-guard.ts", "specs/fixtures/zero-match.spec.md"],
      {
        cwd: process.cwd(),
        env: {
          ...process.env,
          LAKE_GUARD_ARGUMENTS: argumentsPath,
          PATH: `${directory}:${process.env.PATH}`,
        },
        stdout: "pipe",
        stderr: "pipe",
      },
    );

    expect(await proc.exited).toBe(0);
    expect((await readFile(argumentsPath, "utf8")).trim().split("\n")).toEqual([
      "lifecycle",
      "specs/fixtures/zero-match.spec.md",
      "--code",
      ".",
      "--change-scope",
      "jj",
      "--format",
      "json",
    ]);
  } finally {
    await rm(directory, { force: true, recursive: true });
  }
});
