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

//! Manifest data model and on-disk layout.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

/// One immutable snapshot of a table, stored at
/// `<table_root>/<table>/_manifests/v<N>.json`.
#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    /// Paths/URLs of parquet data files in this snapshot.
    pub files:   Vec<String>,
}

pub fn manifest_path(table_root: &Path, table: &str, version: u64) -> PathBuf {
    table_root
        .join(table)
        .join("_manifests")
        .join(format!("v{version}.json"))
}

pub(crate) fn ptr_key(table: &str) -> String { format!("ptr/{table}") }
