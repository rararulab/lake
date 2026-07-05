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

//! Immutable table snapshots. A manifest lists the data files of one table
//! version and is written once, never rewritten — so every reader node can
//! cache it forever. The KV metastore only holds the current-version
//! pointer.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};

use crate::{
    error::{
        CommitConflictSnafu, CorruptPointerSnafu, ManifestCodecSnafu, ManifestExistsSnafu,
        ManifestIoSnafu, Result,
    },
    meta::MetaStore,
};

/// One immutable snapshot of a table, stored at
/// `<table_root>/<table>/_manifests/v<N>.json`.
#[derive(Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub version: u64,
    /// Paths/URLs of parquet data files in this snapshot.
    pub files:   Vec<String>,
}

fn ptr_key(table: &str) -> String { format!("ptr/{table}") }

pub fn manifest_path(table_root: &Path, table: &str, version: u64) -> PathBuf {
    table_root
        .join(table)
        .join("_manifests")
        .join(format!("v{version}.json"))
}

pub fn current_version(meta: &dyn MetaStore, table: &str) -> Result<Option<u64>> {
    meta.get(&ptr_key(table))?
        .map(|bytes| {
            std::str::from_utf8(&bytes)
                .ok()
                .and_then(|s| s.parse().ok())
                .context(CorruptPointerSnafu { table })
        })
        .transpose()
}

pub fn load_current(
    meta: &dyn MetaStore,
    table_root: &Path,
    table: &str,
) -> Result<Option<Manifest>> {
    let Some(version) = current_version(meta, table)? else {
        return Ok(None);
    };
    let path = manifest_path(table_root, table, version);
    let bytes = std::fs::read(&path).context(ManifestIoSnafu { path: &path })?;
    let manifest = serde_json::from_slice(&bytes).context(ManifestCodecSnafu { path: &path })?;
    Ok(Some(manifest))
}

/// Commit a new snapshot: write the immutable manifest file first, then CAS
/// the version pointer in the KV. Losers of the race fail cleanly and
/// retry.
pub fn commit(
    meta: &dyn MetaStore,
    table_root: &Path,
    table: &str,
    files: Vec<String>,
) -> Result<u64> {
    let current = current_version(meta, table)?;
    let next = current.map_or(1, |v| v + 1);

    let path = manifest_path(table_root, table, next);
    let parent = path.parent().expect("manifest path always has a parent");
    std::fs::create_dir_all(parent).context(ManifestIoSnafu { path: parent })?;
    ensure!(
        !path.exists(),
        ManifestExistsSnafu {
            table,
            version: next
        }
    );
    let manifest = Manifest {
        version: next,
        files,
    };
    let bytes = serde_json::to_vec_pretty(&manifest).context(ManifestCodecSnafu { path: &path })?;
    std::fs::write(&path, bytes).context(ManifestIoSnafu { path: &path })?;

    let expected = current.map(|v| v.to_string());
    let swapped = meta.cas(
        &ptr_key(table),
        expected.as_deref().map(str::as_bytes),
        next.to_string().as_bytes(),
    )?;
    ensure!(swapped, CommitConflictSnafu { table });
    Ok(next)
}
