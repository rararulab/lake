# Commit Style — Conventional Commits (MANDATORY)

Every commit message MUST follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description> (#N)

<optional body>

Closes #N
```

- **Allowed types**: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`, `ci`, `perf`, `style`, `build`, `revert`
- **Scope** matches the area of the codebase: `meta`, `manifest`, `catalog`, `ci`, `docs`, `harness` —
  e.g. `feat(catalog):`, `fix(meta):`, `chore(ci):`
- **Breaking changes** use `!`: `feat(manifest)!: change manifest schema version field`
- Include `(#N)` issue reference in commit subject
- Include `Closes #N` in commit body
- A local `commit-msg` hook (`scripts/check-conventional-commit.sh`) enforces this — do NOT bypass it
- Do NOT use free-form commit messages like `"update code"` or `"fix stuff"` — they will be rejected
