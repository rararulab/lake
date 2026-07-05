# What NOT To Do

Every entry has a **why** — the reasoning generalizes better than the rule alone.

## Code & Architecture

- Do NOT use manual `impl Display` + `impl Error` — **why:** `snafu` generates consistent, composable error types; hand-rolled impls drift in style and miss context propagation
- Do NOT rewrite or mutate a manifest file once written — **why:** manifest immutability is what makes reader-side caching safe and unbounded (CLAUDE.md invariant 2); a rewritten manifest silently poisons every cached reader
- Do NOT store anything in the KV metastore beyond the tiny version pointers — **why:** the metastore is the only mutable, contended piece; anything else stored there becomes a hot central store and defeats the read-scaling design (CLAUDE.md invariant 1)
- Do NOT CAS the version pointer before the manifest file is durably written — **why:** the commit protocol is manifest-first, pointer-second (CLAUDE.md invariant 3); reversing the order lets readers resolve a pointer to a manifest that doesn't exist yet
- Do NOT let backend types (RocksDB / DynamoDB) leak outside `src/meta.rs` — **why:** the `MetaStore` trait is the seam that keeps dev and prod backends swappable (CLAUDE.md invariant 4); a leaked backend type couples callers to one deployment
- Do NOT use noop/hollow trait implementations (silently return `Ok(())` / `Ok(None)` / `vec![]`) — **why:** silent success hides integration bugs; if nothing tests or calls a method's return value, the method shouldn't exist
- Do NOT write manual `fn new()` for 3+ field structs — **why:** `bon::Builder` provides consistent, IDE-friendly construction; manual constructors create positional-argument bugs
- Do NOT hardcode connection strings or config defaults deep in domain code — **why:** config must be explicit and auditable at the application boundary; hidden defaults cause "works on my machine" failures
- Do NOT expose mechanism-tuning constants as required config — **why:** cache sizes, retry backoffs, sweeper intervals, and similar internal knobs have no deployment-relevant "right" value; they belong as `const` next to the mechanism they tune. Test: "would a deploy operator have a real reason to pick a different value?" If no → `const`.
- Do NOT write code comments in any language other than English — **why:** non-English comments fragment search and break tooling for international contributors

## Workflow

- Do NOT work directly on `main` — **why:** direct commits bypass CI, review, and issue tracking; even one-line changes need the safety net
- Do NOT merge locally — **why:** local merges skip CI checks and lose the PR audit trail
- Do NOT edit files in the main checkout for 'quick fixes' — **why:** this is the same rule as above, stated explicitly because "just this once" is the most common failure mode
- Do NOT create issues/PRs without proper labels (agent + type + component) — **why:** unlabeled items break automated dashboards and make triage impossible
- Do NOT leave stale worktrees — **why:** stale worktrees accumulate disk usage and cause branch confusion
- Do NOT report PR as complete before CI is green — **why:** user acts on "done" signal; reporting prematurely wastes their time when CI fails
- Do NOT grow a module into a directory without an `AGENT.md` — **why:** without agent guidelines, the next agent working in that area will repeat the same mistakes (see [agent-md.md](agent-md.md))
- Do NOT use stacked PRs (sub-PR → feature branch → main) — **why:** every layer in the stack waits on its own CI run, compounding turnaround time. One issue = one PR targeting `main`. If a plan spans multiple unrelated concerns, split it into independent issues upfront — each shipped on its own.
