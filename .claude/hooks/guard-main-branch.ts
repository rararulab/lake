#!/usr/bin/env bun
// Guard: prevent branch switching on the main checkout.
// Claude must use `jj workspace add` for branch work, never mutate the
// main checkout. PreToolUse hook for Bash — reads the tool input JSON on
// stdin, exit 2 blocks the command.

const input = await Bun.stdin.text();

let command = "";
try {
  command = (JSON.parse(input)?.tool_input?.command as string) ?? "";
} catch {
  process.exit(0); // unparseable payload — don't block
}

// Inside a workspace/worktree — allow everything.
const cwd = process.cwd();
if (cwd.includes("/.worktrees/") || cwd.includes("/.claude/worktrees/")) {
  process.exit(0);
}

const block = (msg: string): never => {
  console.log(`BLOCKED: ${msg}`);
  console.log("Example: jj workspace add .worktrees/issue-N-slug");
  process.exit(2);
};

// git branch creation / switching on the main checkout.
if (/git (checkout -[bB]|switch -c)\b/.test(command)) {
  block("Do not create branches on the main checkout. Use 'jj workspace add' instead.");
}
if (/git (checkout|switch) /.test(command)) {
  const allowed =
    /git (checkout|switch) (main|origin\/|--|-- |-$)/.test(command) ||
    /git checkout --/.test(command) ||
    /git switch -$/.test(command);
  if (!allowed) {
    block("Do not switch branches on the main checkout. Use 'jj workspace add' for branch work.");
  }
}

// jj: editing non-@ revisions or moving main from the main checkout.
if (/jj (edit|new) main\b/.test(command) && !/jj new main -/.test(command)) {
  // `jj new main` inside a workspace is the normal start; on the main
  // checkout it abandons the working copy position — block.
  block("Do not reposition the main checkout. Use 'jj workspace add' for branch work.");
}
if (/jj bookmark (set|move) main\b/.test(command)) {
  block("Do not move the main bookmark locally. Merges go through GitHub PRs.");
}

process.exit(0);
