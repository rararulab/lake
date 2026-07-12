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

use std::{fs, path::PathBuf, process::Command};

use serde::Deserialize as _;
use serde_yaml::Value;

fn root() -> PathBuf { PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..") }

fn load_documents(path: &str) -> Vec<Value> {
    let source = fs::read_to_string(root().join(path))
        .unwrap_or_else(|error| panic!("read {path}: {error}"));
    serde_yaml::Deserializer::from_str(&source)
        .map(|document| Value::deserialize(document).expect("valid Kubernetes YAML"))
        .collect()
}

fn find<'a>(documents: &'a [Value], kind: &str, name: &str) -> &'a Value {
    documents
        .iter()
        .find(|document| {
            document["kind"].as_str() == Some(kind)
                && document["metadata"]["name"].as_str() == Some(name)
        })
        .unwrap_or_else(|| panic!("missing {kind}/{name}"))
}

fn lake_container(workload: &Value) -> &Value {
    workload["spec"]["template"]["spec"]["containers"]
        .as_sequence()
        .expect("containers")
        .iter()
        .find(|container| container["name"].as_str() == Some("lake"))
        .expect("lake container")
}

fn has_named_volume_mount(container: &Value, name: &str, path: &str, read_only: bool) -> bool {
    container["volumeMounts"]
        .as_sequence()
        .is_some_and(|mounts| {
            mounts.iter().any(|mount| {
                mount["name"].as_str() == Some(name)
                    && mount["mountPath"].as_str() == Some(path)
                    && mount["readOnly"].as_bool().unwrap_or(false) == read_only
            })
        })
}

fn assert_pod_contract(
    workload: &Value,
    command: &str,
    listen: &str,
    probe_addr: &str,
    server_name: &str,
    secret_name: &str,
) {
    let pod = &workload["spec"]["template"]["spec"];
    assert_eq!(pod["terminationGracePeriodSeconds"].as_u64(), Some(45));
    assert_eq!(pod["automountServiceAccountToken"].as_bool(), Some(false));
    assert_eq!(pod["securityContext"]["runAsNonRoot"].as_bool(), Some(true));
    assert_eq!(pod["securityContext"]["runAsUser"].as_u64(), Some(65532));
    assert_eq!(pod["securityContext"]["runAsGroup"].as_u64(), Some(65532));
    assert_eq!(
        pod["securityContext"]["seccompProfile"]["type"].as_str(),
        Some("RuntimeDefault")
    );
    let topology = pod["topologySpreadConstraints"]
        .as_sequence()
        .expect("topology spread constraints");
    assert_eq!(topology.len(), 2);
    for key in ["kubernetes.io/hostname", "topology.kubernetes.io/zone"] {
        assert!(
            topology
                .iter()
                .any(|constraint| constraint["topologyKey"].as_str() == Some(key)),
            "missing topology spread for {key}"
        );
    }
    let volumes = pod["volumes"].as_sequence().expect("volumes");
    assert!(
        volumes.iter().all(|volume| {
            volume.get("persistentVolumeClaim").is_none() && volume.get("hostPath").is_none()
        }),
        "pod-local volumes cannot be authoritative Lake state"
    );
    let raw_secrets = volumes
        .iter()
        .find(|volume| volume["name"].as_str() == Some("raw-secrets"))
        .expect("raw secret volume");
    assert_eq!(
        raw_secrets["secret"]["secretName"].as_str(),
        Some(secret_name)
    );
    assert_eq!(raw_secrets["secret"]["defaultMode"].as_str(), Some("0444"));
    let runtime_secrets = volumes
        .iter()
        .find(|volume| volume["name"].as_str() == Some("runtime-secrets"))
        .expect("prepared secret volume");
    assert_eq!(
        runtime_secrets["emptyDir"]["medium"].as_str(),
        Some("Memory")
    );

    let init = pod["initContainers"]
        .as_sequence()
        .and_then(|containers| {
            containers
                .iter()
                .find(|container| container["name"].as_str() == Some("prepare-secrets"))
        })
        .expect("secret preparation init container");
    let init_args = init["args"]
        .as_sequence()
        .expect("secret preparation args")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(init_args.contains("cp /raw/* /prepared/"));
    assert!(init_args.contains("chmod 0600 /prepared/*"));
    assert!(has_named_volume_mount(init, "raw-secrets", "/raw", true));
    assert!(has_named_volume_mount(
        init,
        "runtime-secrets",
        "/prepared",
        false
    ));

    let lake = lake_container(workload);
    let args = lake["args"].as_sequence().expect("args");
    let command_line = args
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>()
        .join(" ");
    assert!(command_line.contains(command));
    assert!(command_line.contains(listen));
    assert_eq!(
        lake["securityContext"]["readOnlyRootFilesystem"].as_bool(),
        Some(true)
    );
    assert_eq!(
        lake["securityContext"]["allowPrivilegeEscalation"].as_bool(),
        Some(false)
    );
    assert!(
        lake["securityContext"]["capabilities"]["drop"]
            .as_sequence()
            .is_some_and(|drop| drop.iter().any(|value| value.as_str() == Some("ALL")))
    );
    for class in ["requests", "limits"] {
        assert!(lake["resources"][class]["cpu"].is_string());
        assert!(lake["resources"][class]["memory"].is_string());
    }

    let env = lake["env"].as_sequence().expect("env");
    assert!(env.iter().any(|entry| {
        entry["name"].as_str() == Some("LAKE_METRICS_ADDR")
            && entry["value"].as_str() == Some("127.0.0.1:9090")
    }));
    assert!(env.iter().any(|entry| {
        entry["name"].as_str() == Some("LAKE_SHUTDOWN_GRACE_MS")
            && entry["value"].as_str() == Some("30000")
    }));
    assert!(has_named_volume_mount(
        lake,
        "runtime-secrets",
        "/var/run/lake-secrets",
        true
    ));

    for (probe, service) in [
        ("startupProbe", "''"),
        ("livenessProbe", "''"),
        ("readinessProbe", "arrow.flight.protocol.FlightService"),
    ] {
        let shell = lake[probe]["exec"]["command"]
            .as_sequence()
            .and_then(|command| command.last())
            .and_then(Value::as_str)
            .expect("authenticated exec probe");
        assert!(shell.contains("grpc_health_probe"));
        assert!(shell.contains(probe_addr));
        assert!(shell.contains("-tls"));
        assert!(shell.contains("-tls-ca-cert=/var/run/lake-secrets/ca.crt"));
        assert!(shell.contains(&format!("-tls-server-name={server_name}")));
        assert!(shell.contains("authorization: Bearer"));
        assert!(shell.contains("/var/run/lake-secrets/health-token"));
        assert!(shell.contains(service));
    }
}

#[test]
fn kubernetes_reference_is_secure_and_matches_runtime_contract() {
    let schema = Command::new("mise")
        .current_dir(root())
        .args([
            "exec",
            "--",
            "kubeconform",
            "-strict",
            "-summary",
            "-kubernetes-version",
            "1.32.0",
            "deploy/kubernetes/lake.yaml",
        ])
        .output()
        .expect("mise must provide kubeconform 0.7.0");
    assert!(
        schema.status.success(),
        "strict Kubernetes schema validation failed:\n{}\n{}",
        String::from_utf8_lossy(&schema.stdout),
        String::from_utf8_lossy(&schema.stderr)
    );

    let documents = load_documents("deploy/kubernetes/lake.yaml");
    assert!(
        documents
            .iter()
            .all(|document| document["kind"].as_str() != Some("Secret"))
    );
    for service_account in ["lake-query", "lake-metasrv"] {
        assert_eq!(
            find(&documents, "ServiceAccount", service_account)["automountServiceAccountToken"]
                .as_bool(),
            Some(false)
        );
    }
    let config = find(&documents, "ConfigMap", "lake-runtime");
    for key in [
        "AWS_REGION",
        "LAKE_S3_BUCKET",
        "LAKE_TABLE_PREFIX",
        "LAKE_MANAGED_OBJECT_PREFIX",
        "LAKE_DYNAMODB_TABLE",
        "LAKE_ASYNC_QUERIES",
        "LAKE_ASYNC_DYNAMODB_TABLE",
        "LAKE_ASYNC_RESULT_PREFIX",
    ] {
        assert!(
            config["data"][key].is_string(),
            "missing cloud authority {key}"
        );
    }

    let query = find(&documents, "Deployment", "lake-query");
    assert!(
        query["spec"]["replicas"]
            .as_u64()
            .is_some_and(|replicas| replicas >= 2)
    );
    assert_pod_contract(
        query,
        "query",
        "0.0.0.0:50051",
        "-addr=127.0.0.1:50051",
        "lake-query.lake-system.svc.cluster.local",
        "lake-query-runtime",
    );
    assert_eq!(
        query["spec"]["template"]["spec"]["serviceAccountName"].as_str(),
        Some("lake-query")
    );
    let query_env = lake_container(query)["env"]
        .as_sequence()
        .expect("query env");
    assert!(query_env.iter().any(|entry| {
        entry["name"].as_str() == Some("LAKE_QUERY_TICKET_KEYS_FILE")
            && entry["value"].as_str() == Some("/var/run/lake-secrets/ticket-keys.json")
    }));
    assert!(query_env.iter().any(|entry| {
        entry["name"].as_str() == Some("LAKE_QUERY_TICKET_TTL_SECS")
            && entry["value"].as_str() == Some("300")
    }));
    let runbook = fs::read_to_string(root().join("docs/guides/kubernetes.md"))
        .expect("Kubernetes runbook");
    assert!(runbook.contains("--from-file=ticket-keys.json=query/ticket-keys.json"));
    for step in ["preload", "activate", "retire"] {
        assert!(runbook.contains(step), "missing ticket key rotation step {step}");
    }

    let metasrv = find(&documents, "StatefulSet", "lake-metasrv");
    assert_eq!(metasrv["spec"]["replicas"].as_u64(), Some(3));
    assert_eq!(
        metasrv["spec"]["serviceName"].as_str(),
        Some("lake-metasrv-headless")
    );
    assert_pod_contract(
        metasrv,
        "meta",
        "${POD_IP}:50052",
        "-addr=${POD_IP}:50052",
        "lake-metasrv.lake-system.svc.cluster.local",
        "lake-metasrv-runtime",
    );
    for probe in ["startupProbe", "livenessProbe", "readinessProbe"] {
        let shell = lake_container(metasrv)[probe]["exec"]["command"]
            .as_sequence()
            .and_then(|command| command.last())
            .and_then(Value::as_str)
            .expect("metasrv exec probe");
        assert!(
            shell.contains("-addr=${POD_IP}:50052"),
            "metasrv binds POD_IP, so {probe} must probe the same interface"
        );
    }
    assert_eq!(
        metasrv["spec"]["template"]["spec"]["serviceAccountName"].as_str(),
        Some("lake-metasrv")
    );
    assert!(
        lake_container(metasrv)["env"]
            .as_sequence()
            .expect("metasrv env")
            .iter()
            .any(|entry| {
                entry["name"].as_str() == Some("POD_IP")
                    && entry["valueFrom"]["fieldRef"]["fieldPath"].as_str() == Some("status.podIP")
            })
    );

    assert_eq!(
        find(&documents, "PodDisruptionBudget", "lake-metasrv")["spec"]["minAvailable"].as_u64(),
        Some(2)
    );
    let services = documents
        .iter()
        .filter(|document| document["kind"].as_str() == Some("Service"))
        .collect::<Vec<_>>();
    assert_eq!(services.len(), 3, "metrics must not gain a Service");
    for service in services {
        let name = service["metadata"]["name"].as_str().expect("service name");
        assert!(
            ["lake-query", "lake-metasrv", "lake-metasrv-headless"].contains(&name),
            "unexpected Service/{name}"
        );
        for port in service["spec"]["ports"]
            .as_sequence()
            .expect("service ports")
        {
            assert!(matches!(port["port"].as_u64(), Some(50051 | 50052)));
            assert_eq!(port["targetPort"].as_str(), Some("flight"));
        }
    }

    let dockerfile = fs::read_to_string(root().join("Dockerfile")).expect("production Dockerfile");
    assert!(dockerfile.starts_with("# syntax=docker/dockerfile:1.7@sha256:"));
    assert!(dockerfile.contains("FROM rust:") && dockerfile.contains("@sha256:"));
    assert!(dockerfile.contains("snapshot.debian.org/archive/debian/"));
    assert!(dockerfile.contains("[check-valid-until=no]"));
    assert!(!dockerfile.contains("deb.debian.org"));
    assert!(dockerfile.contains("cargo build --locked --release"));
    assert!(dockerfile.contains("COPY --from=health-probe"));
    assert!(dockerfile.contains("USER 65532:65532"));
    assert!(dockerfile.contains("ENTRYPOINT [\"/usr/local/bin/lake\"]"));
}
