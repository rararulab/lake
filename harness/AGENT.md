# harness/

Engine-neutral AI-harness contracts. `.claude/agents/*.md` are thin
wrappers over these files — the substance lives here, so any agent engine
(Claude, Codex, ...) reads the same contracts.

- `roles/spec-author.md` — request -> lane triage -> Task Contract / issue
- `roles/implementer.md` — one issue end-to-end inside a jj workspace
- `roles/reviewer.md` — diff + spec review, P0–P3 verdict contract
- `roles/verifier.md` — fresh-context re-verification from clean state
- `stages.toml` — S0–S7 pipeline stage protocol (inputs/outputs/checks)
