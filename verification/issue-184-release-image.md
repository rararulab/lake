# Verification report — issue #184 P1 repair

- `base_sha`: `b7f7d0b74959057848be686d7cff21a706ce35e4`
- `head_sha`: `85bf3e17d90bc3cc79703e84b1d4596a95496ec2`
- `score_authority`: verifier
- `implementer_evidence`: self_check_only

## Scope

The candidate adds the release-image workflow and its contract test, then
fences manual publication to an already-published GitHub release's immutable
`target_commitish`. Manual dispatch now requires the release API's tag,
published state, target revision, checked-out tag revision, and `version.txt`
to agree before Buildx can run. The Kubernetes reference remains an invalid
digest placeholder until an operator replaces every occurrence with a real
manifest digest. No Lake application protocol or runtime source changed.

Jujutsu is authoritative in this colocated workspace. The candidate parent was
`85bf3e17` and the working copy was clean before this report was created.

## Commands

| Command | Result |
| --- | --- |
| `mise run doctor` | PASS — mise tools, nightly rustfmt, cargo check, jj repository, GitHub authentication, and origin remote all passed. |
| `actionlint .github/workflows/release-image.yml` | PASS — exited 0 with no diagnostics. |
| `mise run fmt-check` | PASS — invoked `cargo +nightly fmt --all -- --check` and exited 0. |
| `cargo test -p lake-cli --test release_artifacts` | PASS — `2 passed; 0 failed; 0 ignored`. |
| `mise run spec-lifecycle specs/issue-184-release-image.spec.md` | PASS — both scenarios passed and `spec-lifecycle-guard: OK — every Test selector executed >=1 test`. |
| `rm -rf data && mise run gate` | PASS — fresh e2e created `robots.episodes`, committed `v2`, and printed `self-check ok`; ADBC, site, and workspace tests passed; final line `Finished in 157.96s`. |

The macOS linker emitted its existing `__eh_frame` compact-unwind performance
warning. It was non-fatal for every command.

## Manual-dispatch probes

No image was pushed: GHCR publication requires the post-merge GitHub token and
is outside the verifier's authority. The workflow's pre-publication source
validation was exercised with GitHub's live release API and an isolated real
Git checkout of the published `v1.0.0` tag.

| Probe | Expected | Observed |
| --- | --- | --- |
| `workflow_dispatch`, `RELEASE_TAG=v1.0.0` | release API data, tag commit, checkout HEAD, and `version.txt` all agree | PASS — API returned `{"draft":false,"published_at":"2026-07-17T14:56:45Z","tag_name":"v1.0.0","target_commitish":"d1c622a5a50792a8c81e4805a0dc30a38d2e9b05"}`; source validation exported `VERSION=1.0.0` and `RELEASE_REVISION=d1c622a5a50792a8c81e4805a0dc30a38d2e9b05`. |
| `workflow_dispatch`, `RELEASE_TAG=v1.0.1` | unpublished/nonexistent release fails before checkout/build | PASS — `gh: Not Found (HTTP 404)`. |
| `workflow_dispatch`, `RELEASE_TAG=1.0.0` | malformed tag fails before release lookup/build | PASS — the `^v[0-9]+\\.[0-9]+\\.[0-9]+$` syntax gate rejected it. |

The candidate workflow explicitly resolves
`expected_revision` from `.target_commitish`, resolves
`refs/tags/$RELEASE_TAG^{commit}`, and rejects any disagreement with checkout
HEAD. These were observed in the candidate source at lines 53, 60, and 61–64.

## Transition matrix

| Transition | Observation | Result |
| --- | --- | --- |
| original feature fail-to-pass | At `b7f7d0b7`, `cargo test -p lake-cli --test release_artifacts --manifest-path <base>/Cargo.toml` failed: `no test target named release_artifacts`. At the candidate, the target ran and both tests passed. | observed |
| P1 fail-to-pass | `jj file show -r 92660e51 .github/workflows/release-image.yml` did not contain `.target_commitish`; the repaired head consumes the live API's immutable value and matches it against both tag and checkout revisions. | observed |
| pass-to-fail | actionlint, nightly fmt check, contract tests, spec lifecycle, manual valid/invalid probes, cold e2e, ADBC, site, and workspace tests all passed. | 0 observed |

## Verdict

**PASS.** The P1 repair makes the real `v1.0.0` manual backfill satisfiable
only when it is bound to GitHub's published immutable release revision, and it
passes the independent full local gate.
