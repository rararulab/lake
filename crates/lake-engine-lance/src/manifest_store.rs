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
//! One fixed current pointer plus immutable historical entries:
//!
//! ```text
//! lance-manifest-latest/<base_uri>     ->  {"version": 7, "path": "..."}
//! lance-manifest/<base_uri>/<version>  ->  {"path": "..."}
//! ```
//!
//! The fixed pointer is the atomic claim for the current version. Advancing it
//! first archives the prior exact pointer under its immutable version key,
//! then CASes latest from the exact old bytes to the new staging path. Lance
//! writes that staging manifest before calling this adapter, preserving
//! manifest-before-pointer. Existing per-version-only datasets scan once to
//! install the fixed pointer; every later latest lookup is one point read.

use std::cmp::Ordering;

use async_trait::async_trait;
use lake_meta::{MetaError, MetaStoreRef};
use lance::{Error, Result};
use lance_table::io::commit::external_manifest::ExternalManifestStore;
use serde::{Deserialize, Serialize};

/// KV key namespace for Lance manifest pointers.
const KEY_PREFIX: &str = "lance-manifest";
const LATEST_KEY_PREFIX: &str = "lance-manifest-latest";

/// The JSON value persisted per `(base_uri, version)`.
#[derive(Serialize, Deserialize)]
struct ManifestPointer {
    path: String,
}

/// The mutable current-version claim read on every dataset open.
#[derive(Serialize, Deserialize)]
struct LatestManifestPointer {
    version: u64,
    path:    String,
}

struct LatestState {
    bytes:   Vec<u8>,
    pointer: LatestManifestPointer,
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

    fn latest_key(base_uri: &str) -> String { format!("{LATEST_KEY_PREFIX}/{base_uri}") }

    async fn read_latest(&self, base_uri: &str) -> Result<Option<LatestState>> {
        let Some(bytes) = self
            .meta
            .get(&Self::latest_key(base_uri))
            .await
            .map_err(store_err)?
        else {
            return Ok(None);
        };
        let pointer = decode_latest(&bytes)?;
        Ok(Some(LatestState { bytes, pointer }))
    }

    async fn latest_or_migrate(&self, base_uri: &str) -> Result<Option<LatestState>> {
        if let Some(latest) = self.read_latest(base_uri).await? {
            return Ok(Some(latest));
        }

        let prefix = Self::base_prefix(base_uri);
        let versions = self.meta.list_prefix(&prefix).await.map_err(store_err)?;
        let Some(version) = versions
            .iter()
            .filter_map(|key| key.parse::<u64>().ok())
            .max()
        else {
            return Ok(None);
        };
        let Some(history) = self
            .meta
            .get(&Self::version_key(base_uri, version))
            .await
            .map_err(store_err)?
        else {
            return Ok(None);
        };
        let pointer = LatestManifestPointer {
            version,
            path: decode(&history)?,
        };
        let bytes = encode_latest(&pointer)?;
        let latest_key = Self::latest_key(base_uri);
        if self
            .meta
            .cas(&latest_key, None, &bytes)
            .await
            .map_err(store_err)?
        {
            return Ok(Some(LatestState { bytes, pointer }));
        }
        self.read_latest(base_uri)
            .await?
            .ok_or_else(|| Error::io(format!("manifest latest migration raced: {base_uri}")))
            .map(Some)
    }

    async fn archive_latest(&self, base_uri: &str, latest: &LatestState) -> Result<()> {
        let key = Self::version_key(base_uri, latest.pointer.version);
        let value = encode(&latest.pointer.path)?;
        if self.meta.cas(&key, None, &value).await.map_err(store_err)? {
            return Ok(());
        }
        match self.meta.get(&key).await.map_err(store_err)? {
            Some(current) if current == value => Ok(()),
            _ => Err(Error::io(format!(
                "manifest archive conflicts: {base_uri}@{}",
                latest.pointer.version
            ))),
        }
    }

    async fn finalize_latest_archive_if_present(
        &self,
        base_uri: &str,
        latest: &LatestState,
        final_path: &str,
    ) -> Result<()> {
        let key = Self::version_key(base_uri, latest.pointer.version);
        let Some(current) = self.meta.get(&key).await.map_err(store_err)? else {
            return Ok(());
        };
        let expected = encode(&latest.pointer.path)?;
        let value = encode(final_path)?;
        if current == value {
            return Ok(());
        }
        if current != expected {
            return Err(Error::io(format!(
                "manifest latest archive conflicts: {base_uri}@{}",
                latest.pointer.version
            )));
        }
        if self
            .meta
            .cas(&key, Some(&current), &value)
            .await
            .map_err(store_err)?
        {
            return Ok(());
        }
        match self.meta.get(&key).await.map_err(store_err)? {
            Some(installed) if installed == value => Ok(()),
            _ => Err(Error::io(format!(
                "manifest latest archive finalize raced: {base_uri}@{}",
                latest.pointer.version
            ))),
        }
    }
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

fn encode_latest(pointer: &LatestManifestPointer) -> Result<Vec<u8>> {
    serde_json::to_vec(pointer).map_err(|e| Error::invalid_input(e.to_string()))
}

fn decode_latest(bytes: &[u8]) -> Result<LatestManifestPointer> {
    serde_json::from_slice(bytes).map_err(|e| Error::invalid_input(e.to_string()))
}

#[async_trait]
impl ExternalManifestStore for MetaManifestStore {
    async fn get(&self, base_uri: &str, version: u64) -> Result<String> {
        let key = Self::version_key(base_uri, version);
        if let Some(bytes) = self.meta.get(&key).await.map_err(store_err)? {
            return decode(&bytes);
        }
        match self.latest_or_migrate(base_uri).await? {
            Some(latest) if latest.pointer.version == version => Ok(latest.pointer.path),
            _ => Err(Error::not_found(format!("{base_uri}@{version}"))),
        }
    }

    async fn get_latest_version(&self, base_uri: &str) -> Result<Option<(u64, String)>> {
        Ok(self
            .latest_or_migrate(base_uri)
            .await?
            .map(|latest| (latest.pointer.version, latest.pointer.path)))
    }

    async fn put_if_not_exists(
        &self,
        base_uri: &str,
        version: u64,
        path: &str,
        _size: u64,
        _e_tag: Option<String>,
    ) -> Result<()> {
        let next = LatestManifestPointer {
            version,
            path: path.to_owned(),
        };
        let next_bytes = encode_latest(&next)?;
        let latest_key = Self::latest_key(base_uri);
        match self.latest_or_migrate(base_uri).await? {
            None => {
                if self
                    .meta
                    .cas(&latest_key, None, &next_bytes)
                    .await
                    .map_err(store_err)?
                {
                    Ok(())
                } else {
                    Err(Error::io(format!(
                        "manifest version already claimed: {base_uri}@{version}"
                    )))
                }
            }
            Some(latest) if version < latest.pointer.version => {
                let key = Self::version_key(base_uri, version);
                let value = encode(path)?;
                if self.meta.cas(&key, None, &value).await.map_err(store_err)? {
                    Ok(())
                } else {
                    Err(Error::io(format!(
                        "manifest version already claimed: {base_uri}@{version}"
                    )))
                }
            }
            Some(latest) if version == latest.pointer.version => Err(Error::io(format!(
                "manifest version already claimed: {base_uri}@{version}"
            ))),
            Some(latest) => {
                let expected = latest.pointer.version.checked_add(1).ok_or_else(|| {
                    Error::invalid_input(format!("manifest version overflow: {base_uri}"))
                })?;
                if version != expected {
                    return Err(Error::invalid_input(format!(
                        "manifest version is not contiguous: {base_uri}@{version}, expected \
                         {expected}"
                    )));
                }
                self.archive_latest(base_uri, &latest).await?;
                if self
                    .meta
                    .cas(&latest_key, Some(&latest.bytes), &next_bytes)
                    .await
                    .map_err(store_err)?
                {
                    Ok(())
                } else {
                    Err(Error::io(format!(
                        "manifest version already claimed: {base_uri}@{version}"
                    )))
                }
            }
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
        let Some(latest) = self.latest_or_migrate(base_uri).await? else {
            return Err(Error::not_found(format!("{base_uri}@{version}")));
        };
        match version.cmp(&latest.pointer.version) {
            Ordering::Equal => {
                self.finalize_latest_archive_if_present(base_uri, &latest, path)
                    .await?;
                let value = encode_latest(&LatestManifestPointer {
                    version,
                    path: path.to_owned(),
                })?;
                if self
                    .meta
                    .cas(&Self::latest_key(base_uri), Some(&latest.bytes), &value)
                    .await
                    .map_err(store_err)?
                {
                    return Ok(());
                }
            }
            Ordering::Less => {
                let key = Self::version_key(base_uri, version);
                let Some(current) = self.meta.get(&key).await.map_err(store_err)? else {
                    return Err(Error::not_found(format!("{base_uri}@{version}")));
                };
                let value = encode(path)?;
                if self
                    .meta
                    .cas(&key, Some(&current), &value)
                    .await
                    .map_err(store_err)?
                {
                    return Ok(());
                }
            }
            Ordering::Greater => {
                return Err(Error::not_found(format!("{base_uri}@{version}")));
            }
        }
        Err(Error::io(format!(
            "manifest finalize raced: {base_uri}@{version}"
        )))
    }

    async fn delete(&self, base_uri: &str) -> Result<()> {
        let latest_key = Self::latest_key(base_uri);
        if let Some(latest) = self.read_latest(base_uri).await?
            && !self
                .meta
                .delete(&latest_key, &latest.bytes)
                .await
                .map_err(store_err)?
        {
            return Err(Error::io(format!(
                "manifest latest changed during delete: {base_uri}"
            )));
        }
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
            .get(&latest_key)
            .await
            .map_err(store_err)?
            .is_none()
            && self
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
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use lake_meta::{MetaStore, RocksMeta};
    use tempfile::tempdir;

    use super::*;

    fn store() -> (MetaManifestStore, tempfile::TempDir) {
        let dir = tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(dir.path()).unwrap());
        (MetaManifestStore::new(meta), dir)
    }

    struct CountingMeta {
        inner: RocksMeta,
        gets:  AtomicUsize,
        lists: AtomicUsize,
    }

    #[async_trait]
    impl MetaStore for CountingMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.gets.fetch_add(1, Ordering::SeqCst);
            self.inner.get(key).await
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            self.inner.cas(key, expected, new).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.lists.fetch_add(1, Ordering::SeqCst);
            self.inner.list_prefix(prefix).await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    fn counting_store() -> (MetaManifestStore, Arc<CountingMeta>, tempfile::TempDir) {
        let dir = tempdir().expect("manifest metadata directory");
        let meta = Arc::new(CountingMeta {
            inner: RocksMeta::open(dir.path()).expect("RocksMeta"),
            gets:  AtomicUsize::new(0),
            lists: AtomicUsize::new(0),
        });
        (MetaManifestStore::new(meta.clone()), meta, dir)
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
    async fn delete_clears_latest_and_history() {
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

    #[tokio::test]
    async fn latest_pointer_avoids_history_scan_after_publication() {
        let (store, meta, _dir) = counting_store();
        store
            .put_if_not_exists("point-ds", 1, "v1.staging", 4, None)
            .await
            .expect("claim first version");
        let scans_after_publish = meta.lists.load(Ordering::SeqCst);
        let gets_after_publish = meta.gets.load(Ordering::SeqCst);

        assert_eq!(
            store.get_latest_version("point-ds").await.expect("latest"),
            Some((1, "v1.staging".to_owned()))
        );
        assert_eq!(
            store.get_latest_version("point-ds").await.expect("latest"),
            Some((1, "v1.staging".to_owned()))
        );
        assert_eq!(meta.lists.load(Ordering::SeqCst), scans_after_publish);
        assert_eq!(
            meta.gets.load(Ordering::SeqCst),
            gets_after_publish + 2,
            "each latest resolution is exactly one point get"
        );
    }

    #[tokio::test]
    async fn concurrent_manifest_claims_never_regress_latest() {
        let (store, _dir) = store();
        let store = Arc::new(store);
        store
            .put_if_not_exists("race-ds", 1, "v1.staging", 4, None)
            .await
            .expect("claim v1");
        store
            .put_if_exists("race-ds", 1, "v1.manifest", 4, None)
            .await
            .expect("finalize v1");

        let left = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .put_if_not_exists("race-ds", 2, "v2-left.staging", 4, None)
                    .await
            })
        };
        let right = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .put_if_not_exists("race-ds", 2, "v2-right.staging", 4, None)
                    .await
            })
        };
        let results = [
            left.await.expect("left task"),
            right.await.expect("right task"),
        ];
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        let latest = store
            .get_latest_version("race-ds")
            .await
            .expect("latest after race")
            .expect("latest exists");
        assert_eq!(latest.0, 2);
        assert!(matches!(
            latest.1.as_str(),
            "v2-left.staging" | "v2-right.staging"
        ));
        assert_eq!(
            store.get("race-ds", 1).await.expect("archived v1"),
            "v1.manifest"
        );
    }

    #[tokio::test]
    async fn legacy_history_installs_latest_pointer_once() {
        let (store, meta, _dir) = counting_store();
        for (version, path) in [(1, "v1.manifest"), (2, "v2.manifest")] {
            let key = MetaManifestStore::version_key("legacy-ds", version);
            assert!(
                meta.cas(&key, None, &encode(path).expect("pointer"))
                    .await
                    .expect("legacy write")
            );
        }

        assert_eq!(
            store
                .get_latest_version("legacy-ds")
                .await
                .expect("migrate latest"),
            Some((2, "v2.manifest".to_owned()))
        );
        assert_eq!(meta.lists.load(Ordering::SeqCst), 1);
        assert_eq!(
            store
                .get_latest_version("legacy-ds")
                .await
                .expect("point latest"),
            Some((2, "v2.manifest".to_owned()))
        );
        assert_eq!(meta.lists.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn legacy_staging_finalize_can_advance() {
        let (store, meta, _dir) = counting_store();
        let key = MetaManifestStore::version_key("legacy-stage", 1);
        assert!(
            meta.cas(&key, None, &encode("v1.staging").expect("pointer"))
                .await
                .expect("legacy write")
        );

        assert_eq!(
            store
                .get_latest_version("legacy-stage")
                .await
                .expect("migrate staging"),
            Some((1, "v1.staging".to_owned()))
        );
        store
            .put_if_exists("legacy-stage", 1, "v1.manifest", 4, None)
            .await
            .expect("finalize migrated latest");
        store
            .put_if_not_exists("legacy-stage", 2, "v2.staging", 4, None)
            .await
            .expect("advance after migrated finalize");
        assert_eq!(
            store.get("legacy-stage", 1).await.expect("archived v1"),
            "v1.manifest"
        );
    }

    #[tokio::test]
    async fn current_and_historical_version_reads_survive_pointer_layout() {
        let (store, _dir) = store();
        store
            .put_if_not_exists("history-ds", 1, "v1.staging", 4, None)
            .await
            .expect("claim v1");
        store
            .put_if_exists("history-ds", 1, "v1.manifest", 4, None)
            .await
            .expect("finalize v1");
        store
            .put_if_not_exists("history-ds", 2, "v2.staging", 4, None)
            .await
            .expect("claim v2");
        store
            .put_if_exists("history-ds", 2, "v2.manifest", 4, None)
            .await
            .expect("finalize v2");

        assert_eq!(store.get("history-ds", 1).await.expect("v1"), "v1.manifest");
        assert_eq!(store.get("history-ds", 2).await.expect("v2"), "v2.manifest");
        store
            .put_if_not_exists("history-ds", 0, "v0.manifest", 4, None)
            .await
            .expect("backfill historical version");
        assert_eq!(store.get("history-ds", 0).await.expect("v0"), "v0.manifest");
        assert_eq!(
            store
                .get_latest_version("history-ds")
                .await
                .expect("latest"),
            Some((2, "v2.manifest".to_owned()))
        );
    }
}
