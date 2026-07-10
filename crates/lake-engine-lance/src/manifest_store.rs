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

//! A Lance [`ExternalManifestStore`] backed by lake's [`lake_meta::MetaStore`].
//!
//! On object stores without atomic put-if-not-exists (S3), Lance cannot race
//! concurrent writers safely with its default commit. Lance's answer is the
//! `ExternalManifestStore` trait: an external source of truth mapping
//! `(dataset base_uri, version) -> manifest_path`, on which it runs the commit
//! loop (write a staging manifest to the object store, atomically claim the
//! version in the external store, then finalize). This adapter routes that
//! pointer through our HA KV so lake's own compare-and-set provides the
//! atomicity S3 lacks.
//!
//! # Key layout
//!
//! One KV entry per `(base_uri, version)`:
//!
//! ```text
//! lance-manifest/<base_uri>/<version>  ->  {"path": "<manifest_path>"}   (JSON)
//! ```
//!
//! ponytail: the sketch in `architecture.md` keyed a single
//! `lance-manifest/<base_path>` holding `{version, path}`. Per-version keys are
//! what the trait actually needs — `get(base_uri, version)` must resolve any
//! historical version, and `put_if_not_exists(version)` maps exactly onto
//! `cas(key, None, ..)` (the version key must not yet exist). `get_latest_*`
//! then reduces to `list_prefix` + max, matching the DynamoDB reference store.
//! Only `path` is persisted; Lance re-derives manifest size/e_tag via a `head`
//! when it needs them. Caching size/e_tag here (and overriding
//! `get_latest_manifest_location`) is a later optimization.

use async_trait::async_trait;
use lake_meta::{MetaError, MetaStoreRef};
use lance::{Error, Result};
use lance_table::io::commit::external_manifest::ExternalManifestStore;
use serde::{Deserialize, Serialize};

/// KV key namespace for Lance manifest pointers.
const KEY_PREFIX: &str = "lance-manifest";

/// The JSON value persisted per `(base_uri, version)`.
#[derive(Serialize, Deserialize)]
struct ManifestPointer {
    path: String,
}

/// A [`ExternalManifestStore`] whose source of truth is a [`MetaStore`].
///
/// [`MetaStore`]: lake_meta::MetaStore
pub struct MetaManifestStore {
    meta: MetaStoreRef,
}

impl std::fmt::Debug for MetaManifestStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The KV backend behind `MetaStoreRef` is not `Debug`; the trait only
        // needs a name here.
        f.debug_struct("MetaManifestStore").finish_non_exhaustive()
    }
}

impl MetaManifestStore {
    #[must_use]
    pub fn new(meta: MetaStoreRef) -> Self { Self { meta } }

    fn version_key(base_uri: &str, version: u64) -> String {
        format!("{KEY_PREFIX}/{base_uri}/{version}")
    }

    fn base_prefix(base_uri: &str) -> String { format!("{KEY_PREFIX}/{base_uri}/") }
}

/// Lift a KV failure into Lance's error type so the commit loop can surface it.
fn store_err(source: MetaError) -> Error { Error::io_source(Box::new(source)) }

fn encode(path: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(&ManifestPointer {
        path: path.to_owned(),
    })
    .map_err(|e| Error::invalid_input(e.to_string()))
}

fn decode(bytes: &[u8]) -> Result<String> {
    let pointer: ManifestPointer =
        serde_json::from_slice(bytes).map_err(|e| Error::invalid_input(e.to_string()))?;
    Ok(pointer.path)
}

#[async_trait]
impl ExternalManifestStore for MetaManifestStore {
    async fn get(&self, base_uri: &str, version: u64) -> Result<String> {
        let key = Self::version_key(base_uri, version);
        match self.meta.get(&key).await.map_err(store_err)? {
            Some(bytes) => decode(&bytes),
            // Contract: a specific version that was never recorded is an error
            // (readers distinguish `NotFound` to fall back to the object store).
            None => Err(Error::not_found(format!("{base_uri}@{version}"))),
        }
    }

    async fn get_latest_version(&self, base_uri: &str) -> Result<Option<(u64, String)>> {
        let prefix = Self::base_prefix(base_uri);
        let versions = self.meta.list_prefix(&prefix).await.map_err(store_err)?;
        let Some(latest) = versions.iter().filter_map(|s| s.parse::<u64>().ok()).max() else {
            // No entries: dataset never committed through the external store.
            return Ok(None);
        };
        let key = Self::version_key(base_uri, latest);
        match self.meta.get(&key).await.map_err(store_err)? {
            Some(bytes) => Ok(Some((latest, decode(&bytes)?))),
            None => Ok(None),
        }
    }

    async fn put_if_not_exists(
        &self,
        base_uri: &str,
        version: u64,
        path: &str,
        _size: u64,
        _e_tag: Option<String>,
    ) -> Result<()> {
        let key = Self::version_key(base_uri, version);
        let value = encode(path)?;
        // `expected = None` => claim the version iff no writer beat us to it.
        if self.meta.cas(&key, None, &value).await.map_err(store_err)? {
            Ok(())
        } else {
            Err(Error::io(format!(
                "manifest version already claimed: {base_uri}@{version}"
            )))
        }
    }

    async fn put_if_exists(
        &self,
        base_uri: &str,
        version: u64,
        path: &str,
        _size: u64,
        _e_tag: Option<String>,
    ) -> Result<()> {
        let key = Self::version_key(base_uri, version);
        let Some(current) = self.meta.get(&key).await.map_err(store_err)? else {
            // Contract: finalizing a version that was never claimed is an error.
            return Err(Error::not_found(format!("{base_uri}@{version}")));
        };
        let value = encode(path)?;
        // Flip staging -> final for an already-claimed version. A concurrent
        // finalizer that already wrote the same final path leaves `current`
        // unchanged, so the retried commit converges.
        if self
            .meta
            .cas(&key, Some(&current), &value)
            .await
            .map_err(store_err)?
        {
            Ok(())
        } else {
            Err(Error::io(format!(
                "manifest finalize raced: {base_uri}@{version}"
            )))
        }
    }

    async fn delete(&self, base_uri: &str) -> Result<()> {
        let prefix = Self::base_prefix(base_uri);
        let versions = self.meta.list_prefix(&prefix).await.map_err(store_err)?;
        for version in versions {
            let key = format!("{prefix}{version}");
            let Some(value) = self.meta.get(&key).await.map_err(store_err)? else {
                continue;
            };
            if !self.meta.delete(&key, &value).await.map_err(store_err)? {
                return Err(Error::io(format!(
                    "manifest delete raced with a writer: {base_uri}@{version}"
                )));
            }
        }

        if self
            .meta
            .list_prefix(&prefix)
            .await
            .map_err(store_err)?
            .is_empty()
        {
            Ok(())
        } else {
            Err(Error::io(format!(
                "manifest history changed during delete: {base_uri}"
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use lake_meta::RocksMeta;
    use tempfile::tempdir;

    use super::*;

    fn store() -> (MetaManifestStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(dir.path()).unwrap());
        (MetaManifestStore::new(meta), dir)
    }

    #[tokio::test]
    async fn claim_get_and_advance() {
        let (s, _dir) = store();

        // Unknown dataset: no latest, and a specific version is an error.
        assert_eq!(s.get_latest_version("ds").await.unwrap(), None);
        assert!(s.get("ds", 1).await.is_err());

        // Claim v1, then a second claim of the same version must fail.
        s.put_if_not_exists("ds", 1, "v1.staging", 4, None)
            .await
            .unwrap();
        assert!(
            s.put_if_not_exists("ds", 1, "v1.other", 4, None)
                .await
                .is_err()
        );

        assert_eq!(s.get("ds", 1).await.unwrap(), "v1.staging");
        assert_eq!(
            s.get_latest_version("ds").await.unwrap(),
            Some((1, "v1.staging".to_owned()))
        );

        // Advance to v2; latest tracks the max version.
        s.put_if_not_exists("ds", 2, "v2.staging", 4, None)
            .await
            .unwrap();
        assert_eq!(
            s.get_latest_version("ds").await.unwrap(),
            Some((2, "v2.staging".to_owned()))
        );

        // Finalize v2 (staging -> final); get reflects the flip.
        s.put_if_exists("ds", 2, "v2.manifest", 4, None)
            .await
            .unwrap();
        assert_eq!(s.get("ds", 2).await.unwrap(), "v2.manifest");
        assert_eq!(
            s.get_latest_version("ds").await.unwrap(),
            Some((2, "v2.manifest".to_owned()))
        );
    }

    #[tokio::test]
    async fn finalize_missing_version_errors() {
        let (s, _dir) = store();
        assert!(
            s.put_if_exists("ds", 1, "v1.manifest", 4, None)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn delete_clears_history_for_recreate() {
        let (s, _dir) = store();
        s.put_if_not_exists("ds", 1, "v1.manifest", 4, None)
            .await
            .unwrap();
        s.put_if_not_exists("ds", 2, "v2.manifest", 4, None)
            .await
            .unwrap();

        s.delete("ds").await.unwrap();

        assert_eq!(s.get_latest_version("ds").await.unwrap(), None);
        s.put_if_not_exists("ds", 1, "new-v1.manifest", 4, None)
            .await
            .expect("a recreated dataset can claim version one");
    }
}
