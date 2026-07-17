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

use std::{fs, path::PathBuf};

use serde_yaml::Value;

fn root() -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..") }

fn read(path: &str) -> String {
    fs::read_to_string(root().join(path)).unwrap_or_else(|error| panic!("read {path}: {error}"))
}

fn workflow() -> Value {
    serde_yaml::from_str(&read(".github/workflows/release-image.yml"))
        .expect("release-image workflow must be valid YAML")
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
    let checkout = step_using(steps, "actions/checkout@");
    assert_eq!(
        checkout["with"]["ref"].as_str(),
        Some("${{ env.RELEASE_TAG }}")
    );
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
fn release_image_workflow_rejects_mismatched_tags_and_preserves_digest_pinning() {
    let workflow = workflow();
    let validation = steps(&workflow)
        .iter()
        .find_map(|step| step["run"].as_str())
        .filter(|run| run.contains("version.txt"))
        .expect("release source validation step");
    assert!(validation.contains("git describe --exact-match --tags HEAD"));
    assert!(validation.contains("version.txt"));
    assert!(validation.contains("exit 1"));

    let deployment = read("deploy/kubernetes/lake.yaml");
    assert!(
        !deployment.contains(":latest"),
        "the checked-in deployment reference must not introduce latest"
    );
    let guide = read("docs/guides/kubernetes.md");
    assert!(guide.contains("Do not deploy a mutable tag in production."));
    assert!(guide.contains("@sha256:<digest>"));
}
