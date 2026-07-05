# docs/

Project documentation. Progressive disclosure: root `AGENT.md`/`CLAUDE.md`
are catalogs; the substance is here.

- `architecture.md` — system design: read path, commit protocol,
  invariants, crate map, deliberate simplifications (`ponytail:` markers)
- `design/meta-server.md` — meta-server direction: what lake adopts/adapts/rejects from GreptimeDB metasrv, and phasing
- `guides/workflow.md` — issue -> jj workspace -> PR -> merge, end to end
- `guides/mise-ci.md` — mise, Bun Shell scripts, hooks, and GitHub CI standards
- `guides/local-deploy.md` — portless local deploy and task-scoped deploy tooling
- `guides/cli.md` — clap-based, agent-friendly all-in-one CLI standards
- `guides/rust-style.md` — style direction + mechanical toolchain rules
- `guides/commit-style.md` — Conventional Commits, scopes
- `guides/code-comments.md` — comment discipline
- `guides/anti-patterns.md` — known failure modes; check before review
- `guides/agent-md.md` — the per-folder AGENT.md convention itself
