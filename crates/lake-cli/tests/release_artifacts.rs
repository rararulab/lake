// Copyright 2026 Rararulab
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{
    fs,
    path::PathBuf,
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_yaml::Value;

fn root() -> PathBuf {
    let mut directory = std::env::current_dir().expect("release artifact invocation directory");
    loop {
        let manifest = directory.join("Cargo.toml");
        if manifest.is_file()
            && fs::read_to_string(&manifest).is_ok_and(|contents| contents.contains("[workspace]"))
        {
            return directory;
        }
        assert!(
            directory.pop(),
            "release artifact contracts must run from a Cargo workspace"
        );
    }
}

#[test]
fn release_artifact_contract_uses_invocation_workspace() {
    const PROBE: &str = "LAKE_RELEASE_ARTIFACT_ROOT_PROBE";

    if std::env::var_os(PROBE).is_some() {
        assert_eq!(
            root(),
            std::env::current_dir().expect("probe current directory"),
            "release artifact tests must resolve files from the invoking workspace"
        );
        return;
    }

    let fixture = std::env::temp_dir().join(format!(
        "lake-release-artifact-root-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after epoch")
            .as_nanos()
    ));
    fs::create_dir_all(&fixture).expect("create probe workspace");
    fs::write(fixture.join("Cargo.toml"), "[workspace]\n").expect("write probe manifest");

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("release_artifact_contract_uses_invocation_workspace")
        .env(PROBE, "1")
        .current_dir(&fixture)
        .status()
        .expect("run workspace-root probe");
    fs::remove_dir_all(&fixture).expect("remove probe workspace");
    assert!(status.success(), "release artifact probe must pass");
}

#[test]
fn mise_target_directory_is_workspace_isolated() {
    let target_dir = toml_string(&read("mise.toml"), "CARGO_TARGET_DIR")
        .expect("mise must configure CARGO_TARGET_DIR");
    assert_eq!(
        target_dir, "{{xdg_cache_home}}/lake/target/{{config_root | hash}}",
        "the shared Cargo cache must use an immutable workspace-specific target directory"
    );
}

fn read(path: &str) -> String {
    fs::read_to_string(root().join(path)).unwrap_or_else(|error| panic!("read {path}: {error}"))
}

fn toml_string(section: &str, key: &str) -> Option<String> {
    section.lines().find_map(|line| {
        line.strip_prefix(key)
            .and_then(|value| value.strip_prefix(" = \""))
            .and_then(|value| value.strip_suffix('"'))
            .map(str::to_owned)
    })
}

fn workspace_version() -> String {
    let manifest = read("Cargo.toml");
    let package = manifest
        .split_once("[workspace.package]\n")
        .expect("workspace package section")
        .1
        .split_once("\n[")
        .expect("workspace package section terminator")
        .0;
    toml_string(package, "version").expect("workspace package version")
}

fn locked_lake_packages() -> Vec<(String, String)> {
    read("Cargo.lock")
        .split("\n[[package]]\n")
        .filter_map(|package| {
            let name = toml_string(package, "name")?;
            name.starts_with("lake-").then(|| {
                let version = toml_string(package, "version")
                    .unwrap_or_else(|| panic!("{name} lockfile version"));
                (name, version)
            })
        })
        .collect()
}

fn release_lockfile_selectors() -> Vec<String> {
    serde_json::from_str::<serde_json::Value>(&read("release-please-config.json"))
        .expect("release-please config must be valid JSON")["extra-files"]
        .as_array()
        .expect("release-please extra files")
        .iter()
        .filter(|file| file["path"].as_str() == Some("Cargo.lock"))
        .filter_map(|file| file["jsonpath"].as_str().map(str::to_owned))
        .collect()
}

fn workflow() -> Value {
    serde_yaml::from_str(&read(".github/workflows/release-image.yml"))
        .expect("release-image workflow must be valid YAML")
}

fn ci_workflow() -> Value {
    serde_yaml::from_str(&read(".github/workflows/ci.yml")).expect("CI workflow must be valid YAML")
}

fn release_please_workflow() -> Value {
    serde_yaml::from_str(&read(".github/workflows/release-please.yml"))
        .expect("release-please workflow must be valid YAML")
}

fn steps(workflow: &Value) -> &[Value] {
    workflow["jobs"]["publish-image"]["steps"]
        .as_sequence()
        .expect("publish-image steps")
}

fn step_using<'a>(steps: &'a [Value], action: &str) -> &'a Value {
    steps
        .iter()
        .find(|step| {
            step["uses"]
                .as_str()
                .is_some_and(|uses| uses.starts_with(action))
        })
        .unwrap_or_else(|| panic!("missing {action} step"))
}

fn assert_pinned_action(step: &Value) {
    let uses = step["uses"].as_str().expect("action reference");
    let (_, revision) = uses.split_once('@').expect("action revision");
    let revision = revision.split_whitespace().next().expect("action revision");
    assert!(
        revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
        "action must be pinned by immutable SHA: {uses}"
    );
}

#[test]
fn release_image_workflow_is_tag_pinned_and_multiarch() {
    let workflow = workflow();
    let triggers = &workflow["on"];
    assert!(
        triggers["release"]["types"]
            .as_sequence()
            .is_some_and(|types| types
                .iter()
                .any(|value| value.as_str() == Some("published")))
    );
    assert_eq!(
        triggers["workflow_dispatch"]["inputs"]["tag"]["required"].as_bool(),
        Some(true)
    );
    assert_eq!(workflow["permissions"]["contents"].as_str(), Some("read"));
    assert_eq!(workflow["permissions"]["packages"].as_str(), Some("write"));

    let job = &workflow["jobs"]["publish-image"];
    assert_eq!(job["runs-on"].as_str(), Some("ubuntu-24.04"));
    assert_eq!(
        job["env"]["IMAGE"].as_str(),
        Some("ghcr.io/${{ github.repository }}")
    );
    assert_eq!(
        job["env"]["RELEASE_TAG"].as_str(),
        Some("${{ github.event.release.tag_name || inputs.tag }}")
    );
    assert_eq!(job["env"]["GH_TOKEN"].as_str(), Some("${{ github.token }}"));

    let steps = steps(&workflow);
    for action in [
        "actions/checkout@",
        "docker/setup-qemu-action@",
        "docker/setup-buildx-action@",
        "docker/login-action@",
        "docker/build-push-action@",
    ] {
        assert_pinned_action(step_using(steps, action));
    }
    let checkout = steps
        .iter()
        .find(|step| step["name"].as_str() == Some("Check out release source"))
        .expect("release source checkout");
    assert_eq!(
        checkout["with"]["ref"].as_str(),
        Some("${{ env.RELEASE_TAG }}")
    );
    assert_eq!(checkout["with"]["path"].as_str(), Some("release-source"));
    assert_eq!(checkout["with"]["fetch-depth"].as_u64(), Some(0));

    let build = step_using(steps, "docker/build-push-action@");
    assert_eq!(
        build["with"]["platforms"].as_str(),
        Some("linux/amd64,linux/arm64")
    );
    assert_eq!(build["with"]["push"].as_bool(), Some(true));
    let tags = build["with"]["tags"].as_str().expect("release tags");
    assert!(tags.contains("${{ env.IMAGE }}:${{ env.RELEASE_TAG }}"));
    assert!(tags.contains("${{ env.IMAGE }}:${{ env.VERSION }}"));
    assert!(!tags.contains("latest"));
    let labels = build["with"]["labels"].as_str().expect("OCI labels");
    for label in [
        "org.opencontainers.image.source=",
        "org.opencontainers.image.revision=",
        "org.opencontainers.image.version=",
    ] {
        assert!(labels.contains(label), "missing OCI label {label}");
    }
    assert!(
        steps.iter().any(|step| {
            step["run"]
                .as_str()
                .is_some_and(|run| run.contains("steps.build.outputs.digest"))
        }),
        "published manifest digest must be exposed in the run summary"
    );
}

#[test]
fn release_image_workflow_separates_source_and_recipe_for_backfills() {
    let workflow = workflow();
    let steps = steps(&workflow);
    assert_eq!(
        steps
            .iter()
            .filter(|step| {
                step["uses"]
                    .as_str()
                    .is_some_and(|uses| uses.starts_with("actions/checkout@"))
            })
            .count(),
        2,
        "release recovery must use exactly one immutable source checkout and one immutable recipe \
         checkout"
    );

    let recipe = steps
        .iter()
        .find(|step| step["name"].as_str() == Some("Check out build recipe"))
        .expect("workflow revision checkout");
    assert_pinned_action(recipe);
    assert_eq!(recipe["with"]["ref"].as_str(), Some("${{ github.sha }}"));
    assert_eq!(recipe["with"]["path"].as_str(), Some("build-recipe"));

    let source = steps
        .iter()
        .find(|step| step["name"].as_str() == Some("Check out release source"))
        .expect("release source checkout");
    assert_pinned_action(source);
    assert_eq!(
        source["with"]["ref"].as_str(),
        Some("${{ env.RELEASE_TAG }}")
    );
    assert_eq!(source["with"]["path"].as_str(), Some("release-source"));
    assert_eq!(source["with"]["fetch-depth"].as_u64(), Some(0));

    let validation = steps
        .iter()
        .find(|step| step["name"].as_str() == Some("Verify release source"))
        .expect("release source validation step");
    assert_eq!(
        validation["working-directory"].as_str(),
        Some("release-source")
    );
    let validation_script = validation["run"]
        .as_str()
        .expect("release source validation script");
    assert!(
        validation_script.contains("git -C \"$GITHUB_WORKSPACE/build-recipe\" rev-parse HEAD"),
        "the workflow must read the recipe revision from its immutable checkout"
    );
    assert!(
        validation_script.contains(
            "printf 'BUILD_RECIPE_REVISION=%s\\n' \"$build_recipe_revision\" >> \"$GITHUB_ENV\""
        ),
        "the workflow must export the recipe revision for OCI labeling"
    );

    let build = step_using(steps, "docker/build-push-action@");
    assert_eq!(build["with"]["context"].as_str(), Some("release-source"));
    assert_eq!(
        build["with"]["file"].as_str(),
        Some("build-recipe/Dockerfile")
    );
    assert!(
        build["with"]["labels"].as_str().is_some_and(|labels| {
            labels.contains("org.opencontainers.image.revision=${{ env.RELEASE_REVISION }}")
                && labels.contains(
                    "io.rararulab.lake.build-recipe.revision=${{ env.BUILD_RECIPE_REVISION }}",
                )
        }),
        "historical recovery must publish the immutable build-recipe revision separately from the \
         release source"
    );
}

#[test]
fn release_image_workflow_reuses_scoped_build_cache() {
    let workflow = workflow();
    let build = step_using(steps(&workflow), "docker/build-push-action@");
    assert_eq!(
        build["with"]["cache-from"].as_str(),
        Some("type=gha,scope=lake-release-image")
    );
    assert_eq!(
        build["with"]["cache-to"].as_str(),
        Some("type=gha,scope=lake-release-image,mode=max")
    );
}

#[test]
fn release_image_caches_rust_dependencies_before_copying_application_sources() {
    let dockerfile = read("Dockerfile");
    let install = dockerfile
        .find("cargo install cargo-chef --locked --version")
        .expect("Docker build must pin cargo-chef");
    let prepare = dockerfile
        .find("cargo chef prepare --recipe-path recipe.json")
        .expect("Docker build must prepare a dependency recipe");
    let cook = dockerfile
        .find("cargo chef cook --release --recipe-path recipe.json")
        .expect("Docker build must cook the dependency recipe");
    let application = dockerfile
        .rfind("COPY . .")
        .expect("Docker build must copy application sources after cooking dependencies");
    let build = dockerfile
        .rfind("cargo build --locked --release --package lake-cli --bin lake")
        .expect("Docker build must compile lake after copying application sources");

    assert!(install < prepare && prepare < cook && cook < application && application < build);
    assert!(
        !dockerfile[cook..application].contains("--mount=type=cache,target=/src/target"),
        "the cooked target must be part of an exportable BuildKit layer"
    );
}

#[test]
fn release_image_hydrates_path_dependencies_before_cargo_chef_cook() {
    let dockerfile = read("Dockerfile");
    let recipe = dockerfile
        .find("COPY --from=planner /src/recipe.json recipe.json")
        .expect("Docker build must transfer the generated dependency recipe");
    let path_dependency = dockerfile
        .find(
            "COPY --from=planner /src/third_party/datafusion-execution third_party/datafusion-execution",
        )
        .expect("Cargo-chef cook must receive the local path dependency from the planner");
    let cook = dockerfile
        .find("cargo chef cook --release --recipe-path recipe.json")
        .expect("Docker build must cook the dependency recipe");
    let application = dockerfile
        .rfind("COPY . .")
        .expect("Docker build must copy application sources after cooking dependencies");

    assert!(
        recipe < path_dependency && path_dependency < cook && cook < application,
        "the path dependency must be a cache input after the recipe and before cooking, not part \
         of the later application-source copy"
    );
}

#[test]
fn release_workflows_have_explicit_execution_budgets() {
    let ci = ci_workflow();
    let jobs = ci["jobs"].as_mapping().expect("CI jobs");
    for (name, job) in jobs {
        if job["runs-on"].is_string() {
            assert!(
                job["timeout-minutes"]
                    .as_u64()
                    .is_some_and(|timeout| timeout > 0),
                "hosted CI job {name:?} must declare a positive timeout"
            );
        }
    }
    assert_eq!(
        ci["jobs"]["iceberg-integration"]["timeout-minutes"].as_u64(),
        Some(30)
    );

    let release = workflow();
    assert_eq!(
        release["jobs"]["publish-image"]["timeout-minutes"].as_u64(),
        Some(180)
    );
}

#[test]
fn release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning() {
    let workflow = workflow();
    let validation = steps(&workflow)
        .iter()
        .find_map(|step| step["run"].as_str())
        .filter(|run| run.contains("version.txt"))
        .expect("release source validation step");
    assert!(validation.contains("GITHUB_EVENT_NAME"));
    assert!(validation.contains("GITHUB_SHA"));
    assert!(validation.contains("gh api"));
    assert!(validation.contains("releases/tags/$RELEASE_TAG"));
    assert!(validation.contains(".published_at"));
    assert!(validation.contains(".target_commitish"));
    assert!(validation.contains("refs/tags/$RELEASE_TAG^{commit}"));
    assert!(validation.contains("version.txt"));
    assert!(validation.contains("exit 1"));

    let deployment = read("deploy/kubernetes/lake.yaml");
    let images = deployment
        .lines()
        .filter_map(|line| line.trim().strip_prefix("image: "))
        .collect::<Vec<_>>();
    assert!(
        !images.is_empty(),
        "Kubernetes template must name its images"
    );
    assert!(images.iter().all(|image| {
        *image == "ghcr.io/rararulab/lake@sha256:REPLACE_WITH_RELEASE_MANIFEST_DIGEST"
    }));
    let guide = read("docs/guides/kubernetes.md");
    assert!(guide.contains("Do not deploy a mutable tag in production."));
    assert!(guide.contains("@sha256:<digest>"));
    assert!(guide.contains("REPLACE_WITH_RELEASE_MANIFEST_DIGEST"));
}

#[test]
fn release_please_dispatches_image_publication_for_root_release() {
    let workflow = release_please_workflow();
    assert_eq!(
        workflow["jobs"]["release"]["permissions"]["actions"].as_str(),
        Some("write")
    );

    let steps = workflow["jobs"]["release"]["steps"]
        .as_sequence()
        .expect("Release Please steps");
    let dispatch = steps
        .iter()
        .find(|step| {
            step["run"]
                .as_str()
                .is_some_and(|run| run.contains("gh api --method POST"))
        })
        .expect("Release Please must dispatch image publication after a release");
    assert_eq!(
        dispatch["if"].as_str(),
        Some("${{ steps.release.outputs.release_created == 'true' }}")
    );
    assert_eq!(
        dispatch["env"]["GH_TOKEN"].as_str(),
        Some("${{ github.token }}")
    );
    assert_eq!(
        dispatch["env"]["RELEASE_TAG"].as_str(),
        Some("${{ steps.release.outputs.tag_name }}")
    );
    let command = dispatch["run"].as_str().expect("image publication command");
    assert!(
        command.contains("repos/$GITHUB_REPOSITORY/actions/workflows/release-image.yml/dispatches")
    );
    assert!(command.contains("-f ref=main"));
    assert!(command.contains("inputs[tag]=$RELEASE_TAG"));
    assert!(!command.contains("gh workflow run"));

    let guide = read("docs/guides/mise-ci.md");
    let normalized_guide = guide.split_whitespace().collect::<Vec<_>>().join(" ");
    assert!(normalized_guide.contains("automatically dispatches the existing image workflow"));
    assert!(normalized_guide.contains("manual image backfill"));
}

#[test]
fn release_please_has_automatic_recovery_triggers() {
    let workflow = release_please_workflow();
    let triggers = &workflow["on"];
    let trigger_map = triggers.as_mapping().expect("Release Please triggers");
    assert!(
        triggers["push"]["branches"]
            .as_sequence()
            .is_some_and(|branches| branches
                .iter()
                .any(|branch| branch.as_str() == Some("main")))
    );
    assert!(trigger_map.contains_key(Value::String("workflow_dispatch".into())));
    assert!(
        triggers["schedule"]
            .as_sequence()
            .is_some_and(|schedules| schedules
                .iter()
                .any(|schedule| schedule["cron"].as_str() == Some("17 * * * *"))),
        "Release Please must retry its existing authority hourly after a transient platform \
         failure"
    );

    let steps = workflow["jobs"]["release"]["steps"]
        .as_sequence()
        .expect("Release Please steps");
    assert_eq!(
        steps
            .iter()
            .filter(|step| {
                step["uses"]
                    .as_str()
                    .is_some_and(|uses| uses.starts_with("googleapis/release-please-action@"))
            })
            .count(),
        1,
        "scheduled recovery must reuse the existing Release Please authority"
    );
}

#[test]
fn release_please_covers_every_workspace_lockfile_package() {
    let workspace_version = workspace_version();
    let lockfile_selectors = release_lockfile_selectors();
    let locked_packages = locked_lake_packages();
    assert!(
        !locked_packages.is_empty(),
        "Cargo.lock must contain workspace lake packages"
    );

    for (name, locked_version) in locked_packages {
        assert_eq!(
            locked_version, workspace_version,
            "{name} lockfile version must match the workspace version"
        );
        let selector = format!("$.package[?(@.name.value == \"{name}\")].version");
        assert_eq!(
            lockfile_selectors
                .iter()
                .filter(|candidate| *candidate == &selector)
                .count(),
            1,
            "{name} must have exactly one Cargo.lock release-please selector"
        );
    }
}
