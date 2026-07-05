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

//! The commit protocol: write the immutable manifest file first, then CAS
//! the version pointer. Readers never observe a half-written snapshot.

use std::path::Path;

use lake_meta::MetaStore;
use snafu::{OptionExt, ResultExt, ensure};

use crate::{
    error::{
        CodecSnafu, CommitConflictSnafu, CorruptPointerSnafu, ExistsSnafu, IoSnafu, MetaSnafu,
        Result,
    },
    model::{Manifest, manifest_path, ptr_key},
};

pub async fn current_version(meta: &dyn MetaStore, table: &str) -> Result<Option<u64>> {
    meta.get(&ptr_key(table))
        .await
        .context(MetaSnafu)?
        .map(|bytes| {
            std::str::from_utf8(&bytes)
                .ok()
                .and_then(|s| s.parse().ok())
                .context(CorruptPointerSnafu { table })
        })
        .transpose()
}

pub async fn load_current(
    meta: &dyn MetaStore,
    table_root: &Path,
    table: &str,
) -> Result<Option<Manifest>> {
    let Some(version) = current_version(meta, table).await? else {
        return Ok(None);
    };
    let path = manifest_path(table_root, table, version);
    let bytes = tokio::fs::read(&path)
        .await
        .context(IoSnafu { path: &path })?;
    let manifest = serde_json::from_slice(&bytes).context(CodecSnafu { path: &path })?;
    Ok(Some(manifest))
}

/// Commit a new snapshot. Losers of the race fail cleanly and retry.
pub async fn commit(
    meta: &dyn MetaStore,
    table_root: &Path,
    table: &str,
    files: Vec<String>,
) -> Result<u64> {
    let current = current_version(meta, table).await?;
    let next = current.map_or(1, |v| v + 1);

    let path = manifest_path(table_root, table, next);
    let parent = path.parent().expect("manifest path always has a parent");
    tokio::fs::create_dir_all(parent)
        .await
        .context(IoSnafu { path: parent })?;
    let exists = tokio::fs::try_exists(&path)
        .await
        .context(IoSnafu { path: &path })?;
    ensure!(
        !exists,
        ExistsSnafu {
            table,
            version: next
        }
    );
    let manifest = Manifest {
        version: next,
        files,
    };
    let bytes = serde_json::to_vec_pretty(&manifest).context(CodecSnafu { path: &path })?;
    tokio::fs::write(&path, bytes)
        .await
        .context(IoSnafu { path: &path })?;

    let expected = current.map(|v| v.to_string());
    let swapped = meta
        .cas(
            &ptr_key(table),
            expected.as_deref().map(str::as_bytes),
            next.to_string().as_bytes(),
        )
        .await
        .context(MetaSnafu)?;
    ensure!(swapped, CommitConflictSnafu { table });
    Ok(next)
}

#[cfg(test)]
mod tests {
    use lake_meta::RocksMeta;

    use super::*;
    use crate::error::ManifestError;

    #[tokio::test]
    async fn commit_then_load_roundtrip_and_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let meta = RocksMeta::open(dir.path().join("meta")).unwrap();
        let root = dir.path().join("tables");

        let v1 = commit(&meta, &root, "t", vec!["a.parquet".into()])
            .await
            .unwrap();
        assert_eq!(v1, 1);
        let v2 = commit(
            &meta,
            &root,
            "t",
            vec!["a.parquet".into(), "b.parquet".into()],
        )
        .await
        .unwrap();
        assert_eq!(v2, 2);

        let m = load_current(&meta, &root, "t").await.unwrap().unwrap();
        assert_eq!(m.version, 2);
        assert_eq!(m.files.len(), 2);

        // A racing writer whose manifest file already exists fails cleanly.
        std::fs::write(manifest_path(&root, "t", 3), b"{}").unwrap();
        let err = commit(&meta, &root, "t", vec![]).await.unwrap_err();
        assert!(matches!(err, ManifestError::Exists { version: 3, .. }));
    }
}
