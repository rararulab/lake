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

use std::process::Command;

#[test]
fn binary_emits_json_startup_log_before_command_dispatch() {
    let output = Command::new(env!("CARGO_BIN_EXE_lake"))
        .env("LAKE_LOG_FORMAT", "json")
        .env("RUST_LOG", "lake=info")
        .arg("definitely-not-a-command")
        .output()
        .expect("run lake binary");

    assert!(!output.status.success());
    assert!(output.stdout.is_empty(), "logs must never use stdout");
    let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
    let first = stderr.lines().next().expect("startup log line");
    let event: serde_json::Value = serde_json::from_str(first).expect("startup log is JSON");
    assert_eq!(event["level"], "INFO");
    assert_eq!(event["target"], "lake");
    assert_eq!(event["fields"]["message"], "lake process starting");
    assert_eq!(event["fields"]["version"], env!("CARGO_PKG_VERSION"));
    assert!(!first.contains("definitely-not-a-command"));
}

#[test]
fn invalid_log_configuration_fails_before_storage_setup() {
    let temp = tempfile::tempdir().expect("temporary directory");
    for (case, format, filter, expected) in [
        ("format", "yaml", "lake=info", "LAKE_LOG_FORMAT"),
        ("filter", "json", "[", "RUST_LOG"),
    ] {
        let data_dir = temp.path().join(format!("must-not-exist-{case}"));
        let output = Command::new(env!("CARGO_BIN_EXE_lake"))
            .env("LAKE_LOG_FORMAT", format)
            .env("RUST_LOG", filter)
            .args(["--data-dir", data_dir.to_str().expect("UTF-8 path")])
            .arg("definitely-not-a-command")
            .output()
            .expect("run lake binary");

        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        let stderr = String::from_utf8(output.stderr).expect("stderr is UTF-8");
        assert!(stderr.contains(expected), "stderr was: {stderr}");
        assert!(
            !data_dir.exists(),
            "logging errors must precede storage setup"
        );
    }
}
