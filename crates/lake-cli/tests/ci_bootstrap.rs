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

#[test]
fn mise_bootstrap_serializes_tool_installation() {
    let path = root().join(".github/actions/setup-lake-build/action.yml");
    let action: Value = serde_yaml::from_str(
        &fs::read_to_string(&path)
            .unwrap_or_else(|error| panic!("read {}: {error}", path.display())),
    )
    .expect("setup action must be valid YAML");
    let steps = action["runs"]["steps"]
        .as_sequence()
        .expect("setup action steps");
    let mise = steps
        .iter()
        .find(|step| {
            step["uses"]
                .as_str()
                .is_some_and(|uses| uses.starts_with("jdx/mise-action@"))
        })
        .expect("mise setup step");

    assert_eq!(mise["with"]["install"].as_bool(), Some(true));
    assert_eq!(mise["with"]["install_args"].as_str(), Some("--jobs=1"));
}
