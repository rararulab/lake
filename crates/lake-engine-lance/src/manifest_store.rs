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
//! lance-manifest-latest/<base_uri>     ->  {"incarnation":"...","version":7,"path":"..."}
//! lance-manifest/<base_uri>/<version>  ->  {"path": "..."}
//! ```
//!
//! The fixed pointer is the atomic claim for the current version. Advancing it
//! first archives the prior exact pointer under its immutable version key,
//! then CASes latest from the exact old bytes to the new staging path. Lance
//! writes that staging manifest before calling this adapter, preserving
//! manifest-before-pointer. Existing per-version-only datasets scan once to
//! install the fixed pointer; every later latest lookup is one point read.
//! Drop changes the fixed value to an incarnation-bound `deleting` and then
//! durable `deleted` fence. Recreate generates a new incarnation, avoiding ABA
//! windows even if version and manifest path repeat at the same base URI.

use std::cmp::Ordering;

use async_trait::async_trait;
use lake_meta::{GuardedMutation, MetaError, MetaStoreRef};
use lance::{Error, Result, io::ObjectStore as LanceObjectStore};
use lance_table::io::commit::external_manifest::ExternalManifestStore;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// KV key namespace for Lance manifest pointers.
const KEY_PREFIX: &str = "lance-manifest";
const LATEST_KEY_PREFIX: &str = "lance-manifest-latest";
const CLEANUP_KEY_PREFIX: &str = "lance-manifest-cleanup";

/// The JSON value persisted per `(base_uri, version)`.
#[derive(Serialize, Deserialize)]
struct ManifestPointer {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    incarnation: Option<String>,
    path:        String,
}

/// The mutable current-version claim read on every dataset open.
#[derive(Serialize, Deserialize)]
struct LatestManifestPointer {
    incarnation: String,
    version:     u64,
    path:        String,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum DeleteState {
    Deleting,
    Deleted,
}

#[derive(Deserialize, Serialize)]
struct DeleteMarker {
    state:       DeleteState,
    incarnation: String,
}

#[derive(Deserialize, Serialize)]
struct CleanupCursor {
    incarnation:  String,
    continuation: Option<String>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct ManifestHistoryCleanupStats {
    pub visited:  usize,
    pub removed:  usize,
    pub retained: usize,
    pub has_more: bool,
}

#[async_trait]
pub(crate) trait ManifestExistence: Send + Sync {
    async fn exists(&self, path: &str) -> Result<bool>;
}

#[async_trait]
impl ManifestExistence for LanceObjectStore {
    async fn exists(&self, path: &str) -> Result<bool> {
        let path = Path::parse(path).map_err(|error| Error::invalid_input(error.to_string()))?;
        self.exists(&path).await
    }
}

struct LatestState {
    bytes:   Vec<u8>,
    pointer: LatestManifestPointer,
}

enum LatestRecord {
    Pointer(LatestState),
    Delete {
        bytes:       Vec<u8>,
        state:       DeleteState,
        incarnation: String,
    },
}

enum LatestResolution {
    Missing { expected: Option<Vec<u8>> },
    Present(LatestState),
}

/// A [`ExternalManifestStore`] whose source of truth is a [`MetaStore`].
///
/// [`MetaStore`]: lake_meta::MetaStore
#[derive(Clone)]
pub struct MetaManifestStore {
    meta:     MetaStoreRef,
    writable: bool,
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
    pub fn new(meta: MetaStoreRef) -> Self {
        Self {
            meta,
            writable: true,
        }
    }

    /// Build the manifest view used by stateless Query replicas.
    ///
    /// Reads never install a missing legacy latest pointer and every mutation
    /// method fails before touching the metastore, matching read-only IAM.
    #[must_use]
    pub fn new_read_only(meta: MetaStoreRef) -> Self {
        Self {
            meta,
            writable: false,
        }
    }

    fn require_writable(&self) -> Result<()> {
        if self.writable {
            Ok(())
        } else {
            Err(Error::io("read-only manifest store rejects mutation"))
        }
    }

    fn version_key(base_uri: &str, version: u64) -> String {
        format!("{KEY_PREFIX}/{base_uri}/{version}")
    }

    fn base_prefix(base_uri: &str) -> String { format!("{KEY_PREFIX}/{base_uri}/") }

    fn latest_key(base_uri: &str) -> String { format!("{LATEST_KEY_PREFIX}/{base_uri}") }

    fn cleanup_key(base_uri: &str) -> String { format!("{CLEANUP_KEY_PREFIX}/{base_uri}") }

    async fn read_latest(&self, base_uri: &str) -> Result<Option<LatestRecord>> {
        let Some(bytes) = self
            .meta
            .get(&Self::latest_key(base_uri))
            .await
            .map_err(store_err)?
        else {
            return Ok(None);
        };
        if let Ok(pointer) = decode_latest(&bytes) {
            return Ok(Some(LatestRecord::Pointer(LatestState { bytes, pointer })));
        }
        let marker = decode_delete_marker(&bytes)?;
        Ok(Some(LatestRecord::Delete {
            bytes,
            state: marker.state,
            incarnation: marker.incarnation,
        }))
    }

    fn resolve_record(base_uri: &str, record: Option<LatestRecord>) -> Result<LatestResolution> {
        match record {
            Some(LatestRecord::Pointer(latest)) => Ok(LatestResolution::Present(latest)),
            Some(LatestRecord::Delete {
                state: DeleteState::Deleted,
                bytes,
                ..
            }) => Ok(LatestResolution::Missing {
                expected: Some(bytes),
            }),
            Some(LatestRecord::Delete {
                state: DeleteState::Deleting,
                ..
            }) => Err(Error::io(format!(
                "manifest deletion in progress: {base_uri}"
            ))),
            None => Ok(LatestResolution::Missing { expected: None }),
        }
    }

    async fn latest_or_migrate(&self, base_uri: &str) -> Result<LatestResolution> {
        if let Some(record) = self.read_latest(base_uri).await? {
            return Self::resolve_record(base_uri, Some(record));
        }

        if !self.writable {
            return Err(Error::io(format!(
                "manifest latest pointer requires metadata migration: {base_uri}"
            )));
        }

        let prefix = Self::base_prefix(base_uri);
        let versions = self.meta.list_prefix(&prefix).await.map_err(store_err)?;
        let Some(version) = versions
            .iter()
            .filter_map(|key| key.parse::<u64>().ok())
            .max()
        else {
            return Ok(LatestResolution::Missing { expected: None });
        };
        let Some(history) = self
            .meta
            .get(&Self::version_key(base_uri, version))
            .await
            .map_err(store_err)?
        else {
            return Ok(LatestResolution::Missing { expected: None });
        };
        let pointer = LatestManifestPointer {
            incarnation: Uuid::now_v7().to_string(),
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
            return Ok(LatestResolution::Present(LatestState { bytes, pointer }));
        }
        Self::resolve_record(base_uri, self.read_latest(base_uri).await?)
    }

    async fn archive_latest(&self, base_uri: &str, latest: &LatestState) -> Result<()> {
        let key = Self::version_key(base_uri, latest.pointer.version);
        let value = encode(&latest.pointer.path, &latest.pointer.incarnation)?;
        let latest_key = Self::latest_key(base_uri);
        if self
            .meta
            .guarded_mutate(GuardedMutation::create(
                &latest_key,
                &latest.bytes,
                &key,
                &value,
            ))
            .await
            .map_err(store_err)?
        {
            return Ok(());
        }
        let Some(current) = self.meta.get(&key).await.map_err(store_err)? else {
            return Err(Error::io(format!(
                "manifest archive conflicts: {base_uri}@{}",
                latest.pointer.version
            )));
        };
        if current == value {
            return Ok(());
        }
        let installed = decode_manifest(&current)?;
        if installed.path != latest.pointer.path
            || installed
                .incarnation
                .as_deref()
                .is_some_and(|incarnation| incarnation != latest.pointer.incarnation)
        {
            return Err(Error::io(format!(
                "manifest archive conflicts: {base_uri}@{}",
                latest.pointer.version
            )));
        }
        if installed.incarnation.is_none()
            && self
                .meta
                .guarded_mutate(GuardedMutation::update(
                    &latest_key,
                    &latest.bytes,
                    &key,
                    &current,
                    &value,
                ))
                .await
                .map_err(store_err)?
        {
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
        let installed = decode_manifest(&current)?;
        let value = encode(final_path, &latest.pointer.incarnation)?;
        if installed.path == final_path
            && installed.incarnation.as_deref() == Some(&latest.pointer.incarnation)
        {
            return Ok(());
        }
        if installed.path != latest.pointer.path
            || installed
                .incarnation
                .as_deref()
                .is_some_and(|incarnation| incarnation != latest.pointer.incarnation)
        {
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
            Some(installed)
                if decode_manifest(&installed).is_ok_and(|pointer| {
                    pointer.path == final_path
                        && pointer.incarnation.as_deref() == Some(&latest.pointer.incarnation)
                }) =>
            {
                Ok(())
            }
            _ => Err(Error::io(format!(
                "manifest latest archive finalize raced: {base_uri}@{}",
                latest.pointer.version
            ))),
        }
    }

    pub(crate) async fn reclaim_removed_history(
        &self,
        base_uri: &str,
        objects: &dyn ManifestExistence,
        limit: usize,
    ) -> Result<ManifestHistoryCleanupStats> {
        self.require_writable()?;
        if limit == 0 {
            return Err(Error::invalid_input(
                "manifest history cleanup limit must be positive",
            ));
        }
        let Some(LatestRecord::Pointer(latest)) = self.read_latest(base_uri).await? else {
            return Ok(ManifestHistoryCleanupStats::default());
        };
        let cleanup_key = Self::cleanup_key(base_uri);
        let cursor_before = self.meta.get(&cleanup_key).await.map_err(store_err)?;
        let continuation = cursor_before
            .as_deref()
            .map(|bytes| {
                serde_json::from_slice::<CleanupCursor>(bytes)
                    .map_err(|error| Error::invalid_input(error.to_string()))
            })
            .transpose()?
            .filter(|cursor| cursor.incarnation == latest.pointer.incarnation)
            .and_then(|cursor| cursor.continuation);
        let prefix = Self::base_prefix(base_uri);
        let page = self
            .meta
            .scan_prefix_page(&prefix, continuation.as_deref(), limit)
            .await
            .map_err(store_err)?;
        let mut stats = ManifestHistoryCleanupStats {
            visited: page.entries().len(),
            has_more: page.continuation().is_some(),
            ..ManifestHistoryCleanupStats::default()
        };
        for (version, bytes) in page.entries() {
            let pointer = decode_manifest(bytes)?;
            if pointer
                .incarnation
                .as_deref()
                .is_some_and(|incarnation| incarnation != latest.pointer.incarnation)
            {
                return Err(Error::io(format!(
                    "manifest cleanup crossed incarnation: {base_uri}@{version}"
                )));
            }
            if objects.exists(&pointer.path).await? {
                stats.retained += 1;
                continue;
            }
            let key = format!("{prefix}{version}");
            if !self
                .meta
                .guarded_mutate(GuardedMutation::delete(
                    &Self::latest_key(base_uri),
                    &latest.bytes,
                    &key,
                    bytes,
                ))
                .await
                .map_err(store_err)?
            {
                return Err(Error::io(format!(
                    "manifest cleanup raced: {base_uri}@{version}"
                )));
            }
            stats.removed += 1;
        }
        let cursor_after = serde_json::to_vec(&CleanupCursor {
            incarnation:  latest.pointer.incarnation.clone(),
            continuation: page.continuation().map(str::to_owned),
        })
        .map_err(|error| Error::invalid_input(error.to_string()))?;
        let cursor_updated = match cursor_before.as_deref() {
            Some(expected) => {
                self.meta
                    .guarded_mutate(GuardedMutation::update(
                        &Self::latest_key(base_uri),
                        &latest.bytes,
                        &cleanup_key,
                        expected,
                        &cursor_after,
                    ))
                    .await
            }
            None => {
                self.meta
                    .guarded_mutate(GuardedMutation::create(
                        &Self::latest_key(base_uri),
                        &latest.bytes,
                        &cleanup_key,
                        &cursor_after,
                    ))
                    .await
            }
        }
        .map_err(store_err)?;
        if !cursor_updated {
            return Err(Error::io(format!(
                "manifest cleanup cursor raced: {base_uri}"
            )));
        }
        Ok(stats)
    }
}

/// Lift a KV failure into Lance's error type so the commit loop can surface it.
fn store_err(source: MetaError) -> Error { Error::io_source(Box::new(source)) }

fn encode(path: &str, incarnation: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(&ManifestPointer {
        incarnation: Some(incarnation.to_owned()),
        path:        path.to_owned(),
    })
    .map_err(|e| Error::invalid_input(e.to_string()))
}

fn encode_legacy(path: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(&ManifestPointer {
        incarnation: None,
        path:        path.to_owned(),
    })
    .map_err(|e| Error::invalid_input(e.to_string()))
}

fn decode_manifest(bytes: &[u8]) -> Result<ManifestPointer> {
    serde_json::from_slice(bytes).map_err(|e| Error::invalid_input(e.to_string()))
}

fn decode(bytes: &[u8]) -> Result<String> { Ok(decode_manifest(bytes)?.path) }

fn encode_latest(pointer: &LatestManifestPointer) -> Result<Vec<u8>> {
    serde_json::to_vec(pointer).map_err(|e| Error::invalid_input(e.to_string()))
}

fn decode_latest(bytes: &[u8]) -> Result<LatestManifestPointer> {
    serde_json::from_slice(bytes).map_err(|e| Error::invalid_input(e.to_string()))
}

fn encode_delete_marker(state: DeleteState, incarnation: &str) -> Result<Vec<u8>> {
    serde_json::to_vec(&DeleteMarker {
        state,
        incarnation: incarnation.to_owned(),
    })
    .map_err(|e| Error::invalid_input(e.to_string()))
}

fn decode_delete_marker(bytes: &[u8]) -> Result<DeleteMarker> {
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
            LatestResolution::Present(latest) if latest.pointer.version == version => {
                Ok(latest.pointer.path)
            }
            _ => Err(Error::not_found(format!("{base_uri}@{version}"))),
        }
    }

    async fn get_latest_version(&self, base_uri: &str) -> Result<Option<(u64, String)>> {
        match self.latest_or_migrate(base_uri).await? {
            LatestResolution::Missing { .. } => Ok(None),
            LatestResolution::Present(latest) => {
                Ok(Some((latest.pointer.version, latest.pointer.path)))
            }
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
        self.require_writable()?;
        let latest_key = Self::latest_key(base_uri);
        match self.latest_or_migrate(base_uri).await? {
            LatestResolution::Missing { expected } => {
                let next_bytes = encode_latest(&LatestManifestPointer {
                    incarnation: Uuid::now_v7().to_string(),
                    version,
                    path: path.to_owned(),
                })?;
                if self
                    .meta
                    .cas(&latest_key, expected.as_deref(), &next_bytes)
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
            LatestResolution::Present(latest) if version < latest.pointer.version => {
                let key = Self::version_key(base_uri, version);
                let value = encode(path, &latest.pointer.incarnation)?;
                if self
                    .meta
                    .guarded_mutate(GuardedMutation::create(
                        &latest_key,
                        &latest.bytes,
                        &key,
                        &value,
                    ))
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
            LatestResolution::Present(latest) if version == latest.pointer.version => {
                Err(Error::io(format!(
                    "manifest version already claimed: {base_uri}@{version}"
                )))
            }
            LatestResolution::Present(latest) => {
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
                let next_bytes = encode_latest(&LatestManifestPointer {
                    incarnation: latest.pointer.incarnation.clone(),
                    version,
                    path: path.to_owned(),
                })?;
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
        self.require_writable()?;
        let latest = match self.latest_or_migrate(base_uri).await? {
            LatestResolution::Present(latest) => latest,
            LatestResolution::Missing { .. } => {
                return Err(Error::not_found(format!("{base_uri}@{version}")));
            }
        };
        match version.cmp(&latest.pointer.version) {
            Ordering::Equal => {
                self.finalize_latest_archive_if_present(base_uri, &latest, path)
                    .await?;
                let value = encode_latest(&LatestManifestPointer {
                    incarnation: latest.pointer.incarnation.clone(),
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
                if let Some(LatestRecord::Pointer(installed)) = self.read_latest(base_uri).await?
                    && installed.pointer.incarnation == latest.pointer.incarnation
                    && installed.pointer.version == version
                    && installed.pointer.path == path
                {
                    return Ok(());
                }
            }
            Ordering::Less => {
                let key = Self::version_key(base_uri, version);
                let Some(current) = self.meta.get(&key).await.map_err(store_err)? else {
                    return Err(Error::not_found(format!("{base_uri}@{version}")));
                };
                let current_pointer = decode_manifest(&current)?;
                if current_pointer
                    .incarnation
                    .as_deref()
                    .is_some_and(|incarnation| incarnation != latest.pointer.incarnation)
                {
                    return Err(Error::io(format!(
                        "manifest finalize crossed incarnation: {base_uri}@{version}"
                    )));
                }
                let value = encode(path, &latest.pointer.incarnation)?;
                if self
                    .meta
                    .cas(&key, Some(&current), &value)
                    .await
                    .map_err(store_err)?
                {
                    return Ok(());
                }
                if let Some(installed) = self.meta.get(&key).await.map_err(store_err)?
                    && decode_manifest(&installed).is_ok_and(|pointer| {
                        pointer.path == path
                            && pointer.incarnation.as_deref() == Some(&latest.pointer.incarnation)
                    })
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
        self.require_writable()?;
        let latest_key = Self::latest_key(base_uri);
        let (deleting, incarnation) = match self.read_latest(base_uri).await? {
            Some(LatestRecord::Delete {
                bytes,
                state: DeleteState::Deleting,
                incarnation,
            }) => (bytes, incarnation),
            record => {
                let (expected, incarnation) = match record {
                    Some(LatestRecord::Pointer(latest)) => {
                        (Some(latest.bytes), latest.pointer.incarnation)
                    }
                    Some(LatestRecord::Delete {
                        bytes, incarnation, ..
                    }) => (Some(bytes), incarnation),
                    None => (None, Uuid::now_v7().to_string()),
                };
                let deleting = encode_delete_marker(DeleteState::Deleting, &incarnation)?;
                if !self
                    .meta
                    .cas(&latest_key, expected.as_deref(), &deleting)
                    .await
                    .map_err(store_err)?
                {
                    return Err(Error::io(format!(
                        "manifest latest changed during delete: {base_uri}"
                    )));
                }
                (deleting, incarnation)
            }
        };
        let deleted = encode_delete_marker(DeleteState::Deleted, &incarnation)?;
        let prefix = Self::base_prefix(base_uri);
        let versions = self.meta.list_prefix(&prefix).await.map_err(store_err)?;
        for version in versions {
            let key = format!("{prefix}{version}");
            let Some(value) = self.meta.get(&key).await.map_err(store_err)? else {
                continue;
            };
            if !self
                .meta
                .guarded_mutate(GuardedMutation::delete(
                    &latest_key,
                    &deleting,
                    &key,
                    &value,
                ))
                .await
                .map_err(store_err)?
            {
                return Err(Error::io(format!(
                    "manifest delete raced with a writer: {base_uri}@{version}"
                )));
            }
        }

        if !self
            .meta
            .list_prefix(&prefix)
            .await
            .map_err(store_err)?
            .is_empty()
        {
            return Err(Error::io(format!(
                "manifest history changed during delete: {base_uri}"
            )));
        }
        let cleanup_key = Self::cleanup_key(base_uri);
        if let Some(cursor) = self.meta.get(&cleanup_key).await.map_err(store_err)?
            && !self
                .meta
                .guarded_mutate(GuardedMutation::delete(
                    &latest_key,
                    &deleting,
                    &cleanup_key,
                    &cursor,
                ))
                .await
                .map_err(store_err)?
        {
            return Err(Error::io(format!(
                "manifest cleanup cursor changed during delete: {base_uri}"
            )));
        }
        if self
            .meta
            .cas(&latest_key, Some(&deleting), &deleted)
            .await
            .map_err(store_err)?
        {
            return Ok(());
        }
        match self.read_latest(base_uri).await? {
            Some(LatestRecord::Delete {
                state: DeleteState::Deleted,
                incarnation: installed,
                ..
            }) if installed == incarnation => Ok(()),
            _ => Err(Error::io(format!(
                "manifest deletion did not finalize: {base_uri}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
    };

    use lake_meta::{MetaStore, RocksMeta};
    use tempfile::tempdir;
    use tokio::sync::{Notify, Semaphore};

    use super::*;

    struct FakeManifestExistence {
        existing: Mutex<HashSet<String>>,
        calls:    AtomicUsize,
    }

    #[async_trait]
    impl ManifestExistence for FakeManifestExistence {
        async fn exists(&self, path: &str) -> Result<bool> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(self.existing.lock().expect("existing paths").contains(path))
        }
    }

    async fn history_fixture() -> (
        MetaManifestStore,
        Arc<RocksMeta>,
        tempfile::TempDir,
        FakeManifestExistence,
    ) {
        let dir = tempdir().expect("manifest metadata directory");
        let meta = Arc::new(RocksMeta::open(dir.path()).expect("RocksMeta"));
        let store = MetaManifestStore::new(meta.clone());
        for version in 1..=5 {
            store
                .put_if_not_exists(
                    "cleanup-ds",
                    version,
                    &format!("_versions/{version}.manifest"),
                    4,
                    None,
                )
                .await
                .expect("claim version");
        }
        let objects = FakeManifestExistence {
            existing: Mutex::new(HashSet::from([
                "_versions/2.manifest".to_owned(),
                "_versions/5.manifest".to_owned(),
            ])),
            calls:    AtomicUsize::new(0),
        };
        (store, meta, dir, objects)
    }

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

        async fn guarded_mutate(&self, mutation: GuardedMutation<'_>) -> lake_meta::Result<bool> {
            self.inner.guarded_mutate(mutation).await
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

    struct BlockingMeta {
        inner:                  RocksMeta,
        block_migration_cas:    AtomicBool,
        migration_cas_entered:  Notify,
        migration_cas_release:  Notify,
        block_recreate_cas:     AtomicBool,
        recreate_cas_entered:   Notify,
        recreate_cas_release:   Notify,
        block_history_list:     AtomicBool,
        history_list_entered:   Notify,
        history_list_release:   Notify,
        block_history_create:   AtomicBool,
        history_create_entered: Notify,
        history_create_release: Notify,
        block_finalize_cas:     AtomicBool,
        finalize_cas_arrivals:  AtomicUsize,
        finalize_cas_entered:   Notify,
        finalize_cas_release:   Semaphore,
        block_stale_finalize:   AtomicBool,
        stale_finalize_entered: Notify,
        stale_finalize_release: Notify,
    }

    #[async_trait]
    impl MetaStore for BlockingMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            self.inner.get(key).await
        }

        async fn cas(
            &self,
            key: &str,
            expected: Option<&[u8]>,
            new: &[u8],
        ) -> lake_meta::Result<bool> {
            if key.starts_with(LATEST_KEY_PREFIX)
                && expected.is_none()
                && new
                    .windows(b"\"version\"".len())
                    .any(|part| part == b"\"version\"")
                && self.block_migration_cas.swap(false, Ordering::SeqCst)
            {
                self.migration_cas_entered.notify_one();
                self.migration_cas_release.notified().await;
            }
            if key.starts_with(LATEST_KEY_PREFIX)
                && expected.is_some_and(|value| {
                    value
                        .windows(b"\"state\":\"deleted\"".len())
                        .any(|part| part == b"\"state\":\"deleted\"")
                })
                && new
                    .windows(b"\"version\"".len())
                    .any(|part| part == b"\"version\"")
                && self.block_recreate_cas.swap(false, Ordering::SeqCst)
            {
                self.recreate_cas_entered.notify_one();
                self.recreate_cas_release.notified().await;
            }
            if expected.is_some()
                && new
                    .windows(b"same-final.manifest".len())
                    .any(|part| part == b"same-final.manifest")
                && self.block_stale_finalize.swap(false, Ordering::SeqCst)
            {
                self.stale_finalize_entered.notify_one();
                self.stale_finalize_release.notified().await;
            }
            if expected.is_some()
                && new
                    .windows(b"final.manifest".len())
                    .any(|part| part == b"final.manifest")
                && self.block_finalize_cas.load(Ordering::SeqCst)
            {
                if self.finalize_cas_arrivals.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                    self.finalize_cas_entered.notify_one();
                }
                self.finalize_cas_release
                    .acquire()
                    .await
                    .expect("finalize CAS release")
                    .forget();
            }
            self.inner.cas(key, expected, new).await
        }

        async fn guarded_mutate(&self, mutation: GuardedMutation<'_>) -> lake_meta::Result<bool> {
            if self.block_history_create.swap(false, Ordering::SeqCst) {
                self.history_create_entered.notify_one();
                self.history_create_release.notified().await;
            }
            self.inner.guarded_mutate(mutation).await
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            if prefix.starts_with(KEY_PREFIX)
                && self.block_history_list.swap(false, Ordering::SeqCst)
            {
                self.history_list_entered.notify_one();
                self.history_list_release.notified().await;
            }
            self.inner.list_prefix(prefix).await
        }

        async fn delete(&self, key: &str, expected: &[u8]) -> lake_meta::Result<bool> {
            self.inner.delete(key, expected).await
        }
    }

    fn blocking_store() -> (MetaManifestStore, Arc<BlockingMeta>, tempfile::TempDir) {
        let dir = tempdir().expect("manifest metadata directory");
        let meta = Arc::new(BlockingMeta {
            inner:                  RocksMeta::open(dir.path()).expect("RocksMeta"),
            block_migration_cas:    AtomicBool::new(false),
            migration_cas_entered:  Notify::new(),
            migration_cas_release:  Notify::new(),
            block_recreate_cas:     AtomicBool::new(false),
            recreate_cas_entered:   Notify::new(),
            recreate_cas_release:   Notify::new(),
            block_history_list:     AtomicBool::new(false),
            history_list_entered:   Notify::new(),
            history_list_release:   Notify::new(),
            block_history_create:   AtomicBool::new(false),
            history_create_entered: Notify::new(),
            history_create_release: Notify::new(),
            block_finalize_cas:     AtomicBool::new(false),
            finalize_cas_arrivals:  AtomicUsize::new(0),
            finalize_cas_entered:   Notify::new(),
            finalize_cas_release:   Semaphore::new(0),
            block_stale_finalize:   AtomicBool::new(false),
            stale_finalize_entered: Notify::new(),
            stale_finalize_release: Notify::new(),
        });
        (MetaManifestStore::new(meta.clone()), meta, dir)
    }

    #[tokio::test]
    async fn removed_manifest_history_is_reclaimed_boundedly() {
        let (store, _meta, _dir, objects) = history_fixture().await;
        let first = store
            .reclaim_removed_history("cleanup-ds", &objects, 2)
            .await
            .expect("first cleanup page");
        assert_eq!(first.visited, 2);
        assert_eq!(first.removed, 1);
        assert_eq!(first.retained, 1);
        assert!(first.has_more);
        assert_eq!(objects.calls.load(Ordering::SeqCst), 2);

        let second = store
            .reclaim_removed_history("cleanup-ds", &objects, 2)
            .await
            .expect("second cleanup page");
        assert_eq!(second.visited, 2);
        assert_eq!(second.removed, 2);
        assert!(!second.has_more);
        assert_eq!(objects.calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn retained_manifest_history_survives_cleanup() {
        let (store, _meta, _dir, objects) = history_fixture().await;
        store
            .reclaim_removed_history("cleanup-ds", &objects, 8)
            .await
            .expect("cleanup history");
        assert_eq!(
            store.get("cleanup-ds", 2).await.expect("retained v2"),
            "_versions/2.manifest"
        );
        assert!(store.get("cleanup-ds", 1).await.is_err());
        assert_eq!(
            store
                .get_latest_version("cleanup-ds")
                .await
                .expect("latest"),
            Some((5, "_versions/5.manifest".to_owned()))
        );
    }

    #[tokio::test]
    async fn cleanup_cursor_resumes_without_touching_latest() {
        let (store, meta, _dir, objects) = history_fixture().await;
        let latest_key = MetaManifestStore::latest_key("cleanup-ds");
        let latest_before = meta
            .get(&latest_key)
            .await
            .expect("latest read")
            .expect("latest exists");
        let first = store
            .reclaim_removed_history("cleanup-ds", &objects, 1)
            .await
            .expect("first bounded page");
        assert!(first.has_more);
        let cursor = meta
            .get(&MetaManifestStore::cleanup_key("cleanup-ds"))
            .await
            .expect("cursor read")
            .expect("cursor exists");
        assert!(
            serde_json::from_slice::<CleanupCursor>(&cursor)
                .expect("cursor JSON")
                .continuation
                .is_some()
        );
        store
            .reclaim_removed_history("cleanup-ds", &objects, 1)
            .await
            .expect("resumed page");
        assert_eq!(objects.calls.load(Ordering::SeqCst), 2);
        assert_eq!(
            meta.get(&latest_key).await.expect("latest reread"),
            Some(latest_before)
        );
    }

    #[tokio::test]
    async fn concurrent_delete_cannot_cross_recreate() {
        let (store, meta, _dir) = blocking_store();
        let store = Arc::new(store);
        store
            .put_if_not_exists("delete-resume", 1, "old-v1", 4, None)
            .await
            .unwrap();
        store
            .put_if_not_exists("delete-resume", 2, "old-v2", 4, None)
            .await
            .unwrap();
        let old = match store.read_latest("delete-resume").await.unwrap().unwrap() {
            LatestRecord::Pointer(latest) => latest,
            LatestRecord::Delete { .. } => panic!("expected live pointer"),
        };
        let deleting = encode_delete_marker(DeleteState::Deleting, &old.pointer.incarnation)
            .expect("deleting marker");
        assert!(
            meta.cas(
                &MetaManifestStore::latest_key("delete-resume"),
                Some(&old.bytes),
                &deleting,
            )
            .await
            .unwrap()
        );

        meta.block_history_list.store(true, Ordering::SeqCst);
        let stale_delete = {
            let store = store.clone();
            tokio::spawn(async move { store.delete("delete-resume").await })
        };
        meta.history_list_entered.notified().await;
        store
            .delete("delete-resume")
            .await
            .expect("first delete finishes");
        store
            .put_if_not_exists("delete-resume", 1, "new-v1", 4, None)
            .await
            .expect("recreate v1");
        store
            .put_if_not_exists("delete-resume", 2, "new-v2", 4, None)
            .await
            .expect("recreate v2");
        let objects = FakeManifestExistence {
            existing: Mutex::new(HashSet::from(["new-v1".to_owned()])),
            calls:    AtomicUsize::new(0),
        };
        store
            .reclaim_removed_history("delete-resume", &objects, 1)
            .await
            .expect("new cleanup cursor");
        let history_before = meta.list_prefix("lance-manifest/").await.unwrap();
        let cursor_before = meta
            .get(&MetaManifestStore::cleanup_key("delete-resume"))
            .await
            .unwrap();

        meta.history_list_release.notify_one();
        assert!(stale_delete.await.expect("stale delete task").is_err());
        assert_eq!(
            meta.list_prefix("lance-manifest/").await.unwrap(),
            history_before
        );
        assert_eq!(
            meta.get(&MetaManifestStore::cleanup_key("delete-resume"))
                .await
                .unwrap(),
            cursor_before
        );
        assert_eq!(
            store.get_latest_version("delete-resume").await.unwrap(),
            Some((2, "new-v2".to_owned()))
        );
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
    async fn delete_fence_blocks_stale_migration_and_recreate() {
        let (store, meta, _dir) = blocking_store();
        let store = Arc::new(store);
        let legacy_key = MetaManifestStore::version_key("delete-race", 1);
        assert!(
            meta.cas(
                &legacy_key,
                None,
                &encode_legacy("v1.manifest").expect("legacy pointer")
            )
            .await
            .expect("legacy write")
        );

        meta.block_migration_cas.store(true, Ordering::SeqCst);
        let migrating = {
            let store = store.clone();
            tokio::spawn(async move { store.get_latest_version("delete-race").await })
        };
        meta.migration_cas_entered.notified().await;
        store.delete("delete-race").await.expect("delete wins race");
        meta.migration_cas_release.notify_one();
        assert_eq!(
            migrating
                .await
                .expect("migration task")
                .expect("migration converges"),
            None
        );
        assert_eq!(
            store
                .get_latest_version("delete-race")
                .await
                .expect("deleted latest"),
            None
        );

        store
            .put_if_not_exists("delete-race", 1, "new-v1.manifest", 4, None)
            .await
            .expect("recreate replaces durable deleted marker");
        meta.block_history_list.store(true, Ordering::SeqCst);
        let deleting = {
            let store = store.clone();
            tokio::spawn(async move { store.delete("delete-race").await })
        };
        meta.history_list_entered.notified().await;
        assert!(
            store
                .put_if_not_exists("delete-race", 1, "racing-v1.manifest", 4, None)
                .await
                .is_err(),
            "recreate cannot cross the deleting fence"
        );
        meta.history_list_release.notify_one();
        deleting
            .await
            .expect("delete task")
            .expect("delete completes");
        store
            .put_if_not_exists("delete-race", 1, "final-v1.manifest", 4, None)
            .await
            .expect("recreate starts after deletion finalizes");
    }

    #[tokio::test]
    async fn history_create_is_guarded_by_exact_latest() {
        let (store, meta, _dir) = blocking_store();
        let store = Arc::new(store);

        store
            .put_if_not_exists("archive-race", 1, "v1.manifest", 4, None)
            .await
            .expect("claim archive source");
        meta.block_history_create.store(true, Ordering::SeqCst);
        let advancing = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .put_if_not_exists("archive-race", 2, "v2.staging", 4, None)
                    .await
            })
        };
        meta.history_create_entered.notified().await;
        store
            .delete("archive-race")
            .await
            .expect("delete fences archive");
        store
            .put_if_not_exists("archive-race", 1, "v1.staging", 4, None)
            .await
            .expect("recreate same URI");
        store
            .put_if_exists("archive-race", 1, "v1.manifest", 4, None)
            .await
            .expect("restore same version and final path in new incarnation");
        meta.history_create_release.notify_one();
        assert!(
            advancing.await.expect("advance task").is_err(),
            "stale archive must fail its latest guard"
        );
        assert!(
            meta.list_prefix(&MetaManifestStore::base_prefix("archive-race"))
                .await
                .expect("archive history")
                .is_empty()
        );

        store
            .put_if_not_exists("backfill-race", 1, "v1.manifest", 4, None)
            .await
            .expect("claim v1");
        store
            .put_if_not_exists("backfill-race", 2, "v2.manifest", 4, None)
            .await
            .expect("claim v2");
        meta.block_history_create.store(true, Ordering::SeqCst);
        let backfilling = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .put_if_not_exists("backfill-race", 0, "v0.manifest", 4, None)
                    .await
            })
        };
        meta.history_create_entered.notified().await;
        store
            .delete("backfill-race")
            .await
            .expect("delete fences backfill");
        meta.history_create_release.notify_one();
        assert!(
            backfilling.await.expect("backfill task").is_err(),
            "stale backfill must fail its latest guard"
        );
        assert!(
            meta.list_prefix(&MetaManifestStore::base_prefix("backfill-race"))
                .await
                .expect("backfill history")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn stale_recreate_cannot_cross_incarnations() {
        let (store, meta, _dir) = blocking_store();
        let store = Arc::new(store);
        store
            .put_if_not_exists("recreate-aba", 1, "same.staging", 4, None)
            .await
            .expect("initial create");
        store.delete("recreate-aba").await.expect("initial delete");

        meta.block_recreate_cas.store(true, Ordering::SeqCst);
        let stale_recreate = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .put_if_not_exists("recreate-aba", 1, "same.staging", 4, None)
                    .await
            })
        };
        meta.recreate_cas_entered.notified().await;

        store
            .put_if_not_exists("recreate-aba", 1, "same.staging", 4, None)
            .await
            .expect("new incarnation wins recreate");
        store
            .put_if_exists("recreate-aba", 1, "same.final", 4, None)
            .await
            .expect("new incarnation finalizes");
        store
            .delete("recreate-aba")
            .await
            .expect("new incarnation deletes");

        meta.recreate_cas_release.notify_one();
        assert!(
            stale_recreate.await.expect("stale recreate task").is_err(),
            "old deleted-marker bytes must not match a later incarnation"
        );
        assert_eq!(
            store
                .get_latest_version("recreate-aba")
                .await
                .expect("latest after second delete"),
            None
        );
    }

    #[tokio::test]
    async fn stale_finalize_cannot_cross_incarnations() {
        let (store, meta, _dir) = blocking_store();
        let store = Arc::new(store);

        store
            .put_if_not_exists("stale-current", 1, "staging.manifest", 4, None)
            .await
            .expect("old current staging");
        meta.block_stale_finalize.store(true, Ordering::SeqCst);
        let stale_current = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .put_if_exists("stale-current", 1, "same-final.manifest", 4, None)
                    .await
            })
        };
        meta.stale_finalize_entered.notified().await;
        store
            .delete("stale-current")
            .await
            .expect("delete old current");
        store
            .put_if_not_exists("stale-current", 1, "staging.manifest", 4, None)
            .await
            .expect("recreate current");
        store
            .put_if_exists("stale-current", 1, "same-final.manifest", 4, None)
            .await
            .expect("finalize new current to same path");
        meta.stale_finalize_release.notify_one();
        assert!(
            stale_current.await.expect("stale current task").is_err(),
            "current finalizer must not converge across incarnations"
        );

        store
            .put_if_not_exists("stale-history", 1, "staging.manifest", 4, None)
            .await
            .expect("old historical staging");
        store
            .put_if_not_exists("stale-history", 2, "v2.staging", 4, None)
            .await
            .expect("archive old historical staging");
        meta.block_stale_finalize.store(true, Ordering::SeqCst);
        let stale_history = {
            let store = store.clone();
            tokio::spawn(async move {
                store
                    .put_if_exists("stale-history", 1, "same-final.manifest", 4, None)
                    .await
            })
        };
        meta.stale_finalize_entered.notified().await;
        store
            .delete("stale-history")
            .await
            .expect("delete old history");
        store
            .put_if_not_exists("stale-history", 1, "staging.manifest", 4, None)
            .await
            .expect("recreate history v1");
        store
            .put_if_not_exists("stale-history", 2, "v2.staging", 4, None)
            .await
            .expect("archive new history v1");
        store
            .put_if_exists("stale-history", 1, "same-final.manifest", 4, None)
            .await
            .expect("finalize new history to same path");
        meta.stale_finalize_release.notify_one();
        assert!(
            stale_history.await.expect("stale history task").is_err(),
            "historical finalizer must not converge across incarnations"
        );
    }

    #[tokio::test]
    async fn concurrent_finalize_converges_on_same_path() {
        async fn finalize_twice(
            store: Arc<MetaManifestStore>,
            meta: Arc<BlockingMeta>,
            dataset: &'static str,
            version: u64,
            left_path: &'static str,
            right_path: &'static str,
            expect_both: bool,
        ) {
            meta.finalize_cas_arrivals.store(0, Ordering::SeqCst);
            meta.block_finalize_cas.store(true, Ordering::SeqCst);
            let left = {
                let store = store.clone();
                tokio::spawn(async move {
                    store
                        .put_if_exists(dataset, version, left_path, 4, None)
                        .await
                })
            };
            let right = tokio::spawn(async move {
                store
                    .put_if_exists(dataset, version, right_path, 4, None)
                    .await
            });
            meta.finalize_cas_entered.notified().await;
            meta.block_finalize_cas.store(false, Ordering::SeqCst);
            meta.finalize_cas_release.add_permits(2);
            let results = [
                left.await.expect("left finalizer"),
                right.await.expect("right finalizer"),
            ];
            let successes = results.iter().filter(|result| result.is_ok()).count();
            assert_eq!(successes, if expect_both { 2 } else { 1 });
        }

        let (store, meta, _dir) = blocking_store();
        let store = Arc::new(store);
        store
            .put_if_not_exists("current", 1, "staging.manifest", 4, None)
            .await
            .expect("current staging");
        finalize_twice(
            store.clone(),
            meta.clone(),
            "current",
            1,
            "final.manifest",
            "final.manifest",
            true,
        )
        .await;

        store
            .put_if_not_exists("historical", 1, "staging.manifest", 4, None)
            .await
            .expect("historical staging");
        store
            .put_if_not_exists("historical", 2, "v2.staging", 4, None)
            .await
            .expect("archive staging v1");
        finalize_twice(
            store.clone(),
            meta.clone(),
            "historical",
            1,
            "final.manifest",
            "final.manifest",
            true,
        )
        .await;

        let key = MetaManifestStore::version_key("legacy-finalize", 1);
        assert!(
            meta.cas(
                &key,
                None,
                &encode_legacy("staging.manifest").expect("pointer")
            )
            .await
            .expect("legacy staging")
        );
        store
            .get_latest_version("legacy-finalize")
            .await
            .expect("migrate legacy latest");
        finalize_twice(
            store.clone(),
            meta.clone(),
            "legacy-finalize",
            1,
            "final.manifest",
            "final.manifest",
            true,
        )
        .await;

        store
            .put_if_not_exists("different-finalize", 1, "staging.manifest", 4, None)
            .await
            .expect("different-target staging");
        finalize_twice(
            store,
            meta,
            "different-finalize",
            1,
            "final.manifest",
            "other-final.manifest",
            false,
        )
        .await;
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
                meta.cas(&key, None, &encode_legacy(path).expect("pointer"))
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
    async fn read_only_manifest_store_never_mutates_legacy_state() {
        let dir = tempdir().unwrap();
        let meta = Arc::new(RocksMeta::open(dir.path()).unwrap());
        let base_uri = "s3://bucket/legacy.lance";
        let history_key = MetaManifestStore::version_key(base_uri, 7);
        assert!(
            meta.cas(
                &history_key,
                None,
                &encode_legacy("_versions/7.manifest").unwrap(),
            )
            .await
            .unwrap()
        );
        let store = MetaManifestStore::new_read_only(meta.clone());

        let error = store
            .get_latest_version(base_uri)
            .await
            .expect_err("read-only Query must not install a latest pointer");
        assert!(error.to_string().contains("requires metadata migration"));
        assert!(
            meta.get(&MetaManifestStore::latest_key(base_uri))
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            store
                .put_if_not_exists(base_uri, 8, "_versions/8.manifest", 0, None)
                .await
                .is_err()
        );

        let current_uri = "s3://bucket/current.lance";
        MetaManifestStore::new(meta.clone())
            .put_if_not_exists(current_uri, 3, "_versions/3.manifest", 0, None)
            .await
            .unwrap();
        assert_eq!(
            store.get_latest_version(current_uri).await.unwrap(),
            Some((3, "_versions/3.manifest".to_owned()))
        );
        assert!(
            store
                .put_if_exists(current_uri, 4, "_versions/4.manifest", 0, None)
                .await
                .is_err()
        );
        let objects = FakeManifestExistence {
            existing: Mutex::new(HashSet::new()),
            calls:    AtomicUsize::new(0),
        };
        assert!(
            store
                .reclaim_removed_history(current_uri, &objects, 1)
                .await
                .is_err()
        );
        assert_eq!(objects.calls.load(Ordering::SeqCst), 0);
        assert!(store.delete(current_uri).await.is_err());
        assert_eq!(
            store.get_latest_version(current_uri).await.unwrap(),
            Some((3, "_versions/3.manifest".to_owned()))
        );
    }

    #[tokio::test]
    async fn legacy_staging_finalize_can_advance() {
        let (store, meta, _dir) = counting_store();
        let key = MetaManifestStore::version_key("legacy-stage", 1);
        assert!(
            meta.cas(&key, None, &encode_legacy("v1.staging").expect("pointer"))
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
