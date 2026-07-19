# #249 Pages mise bootstrap serialization

## Failure

GitHub Pages run
[`29689600901`](https://github.com/rararulab/lake/actions/runs/29689600901/job/88199867932)
failed inside `jdx/mise-action` before `mise run site-check`. Concurrent
`cargo:*` tool installs raced while rustup updated the shared stable toolchain,
leaving missing rename targets and partial component rollbacks.

## Revisions

- Base: `bdc9ea6d4bc17fde8d2cde4130905245b9a8ecf3`
- Candidate: `39b13093af9eb8a71ea5c89a73050c285a3b4b9e`
- Authority: author self-check; no independent verifier verdict is claimed.

## Fix

- Added `install_args: --jobs=1` to the Pages `jdx/mise-action` step, matching
  the serialization invariant already used by direct mise jobs in CI.
- Expanded `direct_mise_actions_serialize_tool_installation` to inspect every
  YAML workflow and reject any direct mise-action tool install without the
  serialization argument.

## Verification

- RED: the expanded regression test failed against the merged Pages workflow
  with `left: None`, `right: Some("--jobs=1")`, identifying `pages.yml` as the
  missing serialization guard.
- GREEN: `cargo test -p lake-cli --test ci_bootstrap -- --nocapture` passed
  both bootstrap tests after the one-line workflow fix.
- `mise run site-check` passed with zero Astro diagnostics, 88 static pages,
  86 Pagefind-indexed pages, and all generated-output smoke checks.
- Final `mise run gate` passed after the test refactor, including all workspace
  targets, e2e self-check, ADBC interoperability, hooks, and site checks.
- `cargo +nightly fmt --all -- --check` and `git diff --check` passed.

## Result

Author self-check PASS. The Pages job no longer permits the concurrent mise
bootstrap mode that caused the observed rustup failure.
