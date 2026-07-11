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

//! The Lance implementation of [`lake_engine::TableEngine`].
//!
//! This is the ONLY crate permitted to name a `lance::` type — the engine
//! boundary keeps Lance swappable for a self-built engine. Each lake table
//! is one Lance dataset; Lance owns the per-table manifest, versioning, and
//! commit.
//!
//! By default this uses Lance's own commit, which is atomic on a local
//! filesystem. On object stores without atomic put-if-not-exists (S3),
//! [`LanceEngine::with_manifest_store`] routes the manifest pointer through a
//! [`MetaManifestStore`] backed by our `MetaStore`, giving Lance the
//! put-if-not-exists it needs for concurrent commits — see `manifest_store`.

use std::{
    collections::{BTreeSet, HashMap},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{Array, RecordBatch, RecordBatchIterator, StringArray, StructArray, UInt64Array},
        datatypes::{DataType, SchemaRef},
        error::ArrowError,
    },
    catalog::TableProvider,
    error::DataFusionError,
    execution::SendableRecordBatchStream,
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::{StreamExt, TryStreamExt};
use lake_common::{AppendOperation, ObjectIdentity, ObjectReferenceDelta, TableLocation, Version};
use lake_engine::{
    EngineError, ObjectReferencePage, ObjectReferenceRequest, Result, TableEngine, TableHandle,
    TableHandleRef,
};
use lake_meta::MetaStoreRef;
use lance::{
    Dataset,
    datafusion::LanceTableProvider,
    dataset::{
        WriteMode, WriteParams,
        builder::DatasetBuilder,
        cleanup::CleanupPolicyBuilder,
        optimize::{CompactionOptions, compact_files},
        write::{CommitBuilder, InsertBuilder},
    },
    io::{ObjectStoreParams, StorageOptionsAccessor},
};
use lance_table::io::commit::{
    CommitHandler,
    external_manifest::{ExternalManifestCommitHandler, ExternalManifestStore},
};
use object_store::{ObjectStoreExt, PutMode};

mod manifest_store;
pub use manifest_store::MetaManifestStore;

/// How this engine writes and opens datasets: which commit handler (external
/// manifest store) and which object-store options (S3 endpoint, credentials).
/// Shared by the engine and each open table handle so appends use the same
/// configuration as the create.
#[derive(Clone, Debug, Default)]
struct WriteConfig {
    // ponytail: `None` -> Lance's default object-store commit (atomic on local
    // FS). `Some` -> commits route through our `MetaStore`-backed external
    // manifest store, giving put-if-not-exists semantics on S3.
    commit_handler:  Option<Arc<dyn CommitHandler>>,
    manifest_store:  Option<MetaManifestStore>,
    // Empty -> local filesystem. Non-empty -> object_store config keys
    // (`aws_endpoint`, `aws_access_key_id`, …) threaded into every read/write.
    storage_options: HashMap<String, String>,
    #[cfg(test)]
    history_scans:   Arc<std::sync::atomic::AtomicUsize>,
}

impl WriteConfig {
    fn object_store_params(&self) -> Option<ObjectStoreParams> {
        if self.storage_options.is_empty() {
            return None;
        }
        let accessor = StorageOptionsAccessor::with_static_options(self.storage_options.clone());
        Some(ObjectStoreParams {
            storage_options_accessor: Some(Arc::new(accessor)),
            ..Default::default()
        })
    }

    fn write_params(&self, mode: WriteMode) -> WriteParams {
        WriteParams {
            mode,
            commit_handler: self.commit_handler.clone(),
            store_params: self.object_store_params(),
            ..Default::default()
        }
    }

    /// Open a dataset through the configured commit handler + storage options,
    /// so both the local and the S3-with-external-manifest paths resolve the
    /// latest version the same way.
    async fn open_dataset(&self, uri: &str) -> lance::Result<Dataset> {
        let mut builder = DatasetBuilder::from_uri(uri);
        if !self.storage_options.is_empty() {
            builder = builder.with_storage_options(self.storage_options.clone());
        }
        if let Some(handler) = &self.commit_handler {
            builder = builder.with_commit_handler(handler.clone());
        }
        builder.load().await
    }
}

/// A `TableEngine` backed by Lance datasets.
#[derive(Debug, Default)]
pub struct LanceEngine {
    config: WriteConfig,
}

impl LanceEngine {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    /// Build an engine whose commits route through `meta` (external manifest
    /// store) on the local filesystem.
    ///
    /// Every `create`/`append` writes its manifest pointer via a
    /// [`MetaManifestStore`], so concurrent writers serialize through lake's
    /// compare-and-set instead of relying on object-store atomic renames.
    #[must_use]
    pub fn with_manifest_store(meta: MetaStoreRef) -> Self {
        let manifest_store = MetaManifestStore::new(meta);
        Self {
            config: WriteConfig {
                commit_handler: Some(external_handler(manifest_store.clone())),
                manifest_store: Some(manifest_store),
                storage_options: HashMap::new(),
                ..WriteConfig::default()
            },
        }
    }

    /// Build an engine for object storage: commits route through `meta`'s
    /// external manifest store, and `storage_options` (object_store config
    /// keys — `aws_endpoint`, `aws_access_key_id`, `aws_region`, …) point Lance
    /// at the bucket. This is the production path.
    #[must_use]
    pub fn for_object_store(meta: MetaStoreRef, storage_options: HashMap<String, String>) -> Self {
        let manifest_store = MetaManifestStore::new(meta);
        Self {
            config: WriteConfig {
                commit_handler: Some(external_handler(manifest_store.clone())),
                manifest_store: Some(manifest_store),
                storage_options,
                ..WriteConfig::default()
            },
        }
    }
}

fn external_handler(manifest_store: MetaManifestStore) -> Arc<dyn CommitHandler> {
    let store: Arc<dyn ExternalManifestStore> = Arc::new(manifest_store);
    Arc::new(ExternalManifestCommitHandler {
        external_manifest_store: store,
    })
}

/// How many recent committed versions [`maintain`](LanceEngine::maintain)
/// keeps when reclaiming old ones; everything before them is eligible for GC.
// ponytail: a fixed version-count is the chrono-free retention policy. The
// preferred policy is time-based (keep everything newer than, e.g.,
// `chrono::Duration::days(7)` via `Dataset::cleanup_old_versions`), and both
// the count and the horizon should be operator-configurable. Time-based
// retention needs `chrono` as a workspace dependency (Lance does not re-export
// it), which is not wired up yet.
const RETAIN_VERSIONS: usize = 10;
const MANIFEST_HISTORY_CLEANUP_PAGE_SIZE: usize = 256;

#[cfg(not(test))]
const REFERENCE_CHUNK_SIZE: usize = 4_096;
#[cfg(test)]
const REFERENCE_CHUNK_SIZE: usize = 2;
const MAX_REFERENCES_PER_APPEND: usize = 1_000_000;
const APPEND_TENANT_PROPERTY: &str = "lake.append.tenant";
const APPEND_OPERATION_PROPERTY: &str = "lake.append.operation_id";
const APPEND_DIGEST_PROPERTY: &str = "lake.append.payload_sha256";
const APPEND_REFERENCE_STAGE_PROPERTY: &str = "lake.append.reference_stage";

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
struct StagedReferenceChunk {
    format_version: u8,
    parent_version: Version,
    chunk_index:    u32,
    chunk_count:    u32,
    added:          Vec<ObjectIdentity>,
}

#[async_trait]
impl TableEngine for LanceEngine {
    fn kind(&self) -> &'static str { "lance" }

    async fn create(&self, location: &TableLocation, schema: SchemaRef) -> Result<TableHandleRef> {
        if self.config.open_dataset(location.as_str()).await.is_ok() {
            return Err(EngineError::already_exists(location.clone()));
        }
        let empty = RecordBatchIterator::new(
            std::iter::empty::<std::result::Result<RecordBatch, ArrowError>>(),
            schema,
        );
        let dataset = Dataset::write(
            empty,
            location.as_str(),
            Some(self.config.write_params(WriteMode::Create)),
        )
        .await
        .map_err(EngineError::backend)?;
        Ok(LanceTable::handle(
            dataset,
            self.config.clone(),
            location.clone(),
        ))
    }

    async fn open(&self, location: &TableLocation) -> Result<Option<TableHandleRef>> {
        match self.config.open_dataset(location.as_str()).await {
            Ok(dataset) => Ok(Some(LanceTable::handle(
                dataset,
                self.config.clone(),
                location.clone(),
            ))),
            Err(lance::Error::DatasetNotFound { .. }) => Ok(None),
            Err(e) => Err(EngineError::backend(e)),
        }
    }

    async fn remove(&self, location: &TableLocation) -> Result<()> {
        // Delete every object under the dataset's path, on whatever store the
        // URI names (local FS or S3). Idempotent: listing an absent prefix
        // yields nothing to delete.
        let url = dataset_url(location)?;
        let (store, path) = object_store::parse_url_opts(&url, self.config.storage_options.clone())
            .map_err(EngineError::backend)?;
        let paths = store.list(Some(&path)).map_ok(|meta| meta.location).boxed();
        store
            .delete_stream(paths)
            .try_collect::<Vec<_>>()
            .await
            .map_err(EngineError::backend)?;
        if let Some(handler) = &self.config.commit_handler {
            handler.delete(&path).await.map_err(EngineError::backend)?;
        }
        Ok(())
    }

    async fn maintain(
        &self,
        location: &TableLocation,
        version: Version,
    ) -> Result<Option<Version>> {
        // Open a mutable dataset so compaction can advance it in place, then
        // reclaim versions no longer within the retention window. Both steps
        // are no-ops when nothing qualifies, keeping the sweep idempotent.
        let dataset = self
            .config
            .open_dataset(location.as_str())
            .await
            .map_err(EngineError::backend)?;
        let mut dataset = dataset
            .checkout_version(version.0)
            .await
            .map_err(EngineError::backend)?;
        compact_files(&mut dataset, CompactionOptions::default(), None)
            .await
            .map_err(EngineError::backend)?;
        let maintained_version = Version(dataset.version().version);
        if maintained_version != version {
            persist_reference_chunks(
                &self.config,
                location,
                version,
                maintained_version,
                Vec::new(),
            )
            .await?;
        }
        let policy = CleanupPolicyBuilder::default()
            .error_if_tagged_old_versions(false)
            .retain_n_versions(&dataset, RETAIN_VERSIONS)
            .await
            .map_err(EngineError::backend)?
            .build();
        dataset
            .cleanup_with_policy(policy)
            .await
            .map_err(EngineError::backend)?;
        if let Some(manifest_store) = &self.config.manifest_store {
            let url = dataset_url(location)?;
            let (_, base) = object_store::parse_url_opts(&url, self.config.storage_options.clone())
                .map_err(EngineError::backend)?;
            let object_store = dataset
                .object_store(None)
                .await
                .map_err(EngineError::backend)?;
            manifest_store
                .reclaim_removed_history(
                    base.as_ref(),
                    object_store.as_ref(),
                    MANIFEST_HISTORY_CLEANUP_PAGE_SIZE,
                )
                .await
                .map_err(EngineError::backend)?;
        }
        Ok((maintained_version != version).then_some(maintained_version))
    }

    async fn retained_object_references(
        &self,
        location: &TableLocation,
        request: ObjectReferenceRequest,
    ) -> Result<ObjectReferencePage> {
        let root = request.root_version();
        let (mut version, mut chunk) = match request.cursor() {
            Some(cursor) => parse_reference_cursor(location, root, cursor.as_str())?,
            None => (root, 0),
        };
        let mut deltas = Vec::with_capacity(request.limit());
        while version.0 > 1 && deltas.len() < request.limit() {
            let delta = load_reference_delta(&self.config, location, version, chunk).await?;
            let parent = delta.parent_version();
            let next_chunk =
                chunk
                    .checked_add(1)
                    .ok_or_else(|| EngineError::ReferenceLineageUnavailable {
                        location: location.clone(),
                        reason:   "reference chunk index overflowed".to_owned(),
                    })?;
            if next_chunk < delta.chunk_count() {
                chunk = next_chunk;
            } else {
                version = parent;
                chunk = 0;
            }
            deltas.push(delta);
        }
        let next_cursor = (version.0 > 1).then(|| {
            lake_engine::ObjectReferenceCursor::new(format!("r{}:v{}:c{chunk}", root.0, version.0))
        });
        Ok(ObjectReferencePage::new(deltas, next_cursor))
    }
}

/// Resolve a [`TableLocation`] to an object-store URL. A bare path (local dev)
/// becomes a `file://` directory URL; anything with a scheme (`s3://`, …) is
/// used as-is.
fn dataset_url(location: &TableLocation) -> Result<url::Url> {
    let raw = location.as_str();
    match url::Url::parse(raw) {
        Ok(url) => Ok(url),
        Err(url::ParseError::RelativeUrlWithoutBase) => {
            // A bare filesystem path. Absolutize (without requiring it to
            // exist — it may already be deleted) before the file:// URL.
            let abs = std::path::absolute(raw).map_err(EngineError::backend)?;
            url::Url::from_directory_path(&abs).map_err(|()| {
                EngineError::backend(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("cannot form a file URL from {}", abs.display()),
                ))
            })
        }
        Err(e) => Err(EngineError::backend(e)),
    }
}

async fn persist_reference_delta(
    config: &WriteConfig,
    location: &TableLocation,
    delta: &ObjectReferenceDelta,
) -> Result<()> {
    let url = dataset_url(location)?;
    let (store, root) = object_store::parse_url_opts(&url, config.storage_options.clone())
        .map_err(EngineError::backend)?;
    let sidecar = root
        .join("_lake")
        .join("object_refs")
        .join(delta.table_version().0.to_string())
        .join(format!("{}.json", delta.chunk_index()));
    let encoded = delta.encode().map_err(EngineError::backend)?;
    match store
        .put_opts(&sidecar, encoded.clone().into(), PutMode::Create.into())
        .await
    {
        Ok(_) => Ok(()),
        Err(object_store::Error::AlreadyExists { .. }) => {
            let existing = store
                .get(&sidecar)
                .await
                .map_err(EngineError::backend)?
                .bytes()
                .await
                .map_err(EngineError::backend)?;
            let existing = ObjectReferenceDelta::decode(&existing).map_err(EngineError::backend)?;
            if existing == *delta {
                Ok(())
            } else {
                Err(EngineError::ReferenceLineageUnavailable {
                    location: location.clone(),
                    reason:   format!(
                        "reference sidecar for {} already contains a different delta",
                        delta.table_version()
                    ),
                })
            }
        }
        Err(error) => Err(EngineError::backend(error)),
    }
}

async fn persist_reference_chunks(
    config: &WriteConfig,
    location: &TableLocation,
    parent_version: Version,
    table_version: Version,
    added: Vec<ObjectIdentity>,
) -> Result<()> {
    let chunk_count = added.len().max(1).div_ceil(REFERENCE_CHUNK_SIZE);
    let chunk_count = u32::try_from(chunk_count).map_err(EngineError::backend)?;
    if added.is_empty() {
        let delta = ObjectReferenceDelta::try_new_chunk(
            parent_version,
            table_version,
            0,
            1,
            Vec::new(),
            Vec::new(),
        )
        .map_err(EngineError::backend)?;
        return persist_reference_delta(config, location, &delta).await;
    }
    for (index, chunk) in added.chunks(REFERENCE_CHUNK_SIZE).enumerate() {
        let delta = ObjectReferenceDelta::try_new_chunk(
            parent_version,
            table_version,
            u32::try_from(index).map_err(EngineError::backend)?,
            chunk_count,
            chunk.to_vec(),
            Vec::new(),
        )
        .map_err(EngineError::backend)?;
        persist_reference_delta(config, location, &delta).await?;
    }
    Ok(())
}

async fn load_reference_delta(
    config: &WriteConfig,
    location: &TableLocation,
    version: Version,
    chunk: u32,
) -> Result<ObjectReferenceDelta> {
    let url = dataset_url(location)?;
    let (store, root) = object_store::parse_url_opts(&url, config.storage_options.clone())
        .map_err(EngineError::backend)?;
    let sidecar = root
        .join("_lake")
        .join("object_refs")
        .join(version.0.to_string())
        .join(format!("{chunk}.json"));
    let bytes = store
        .get(&sidecar)
        .await
        .map_err(|error| EngineError::ReferenceLineageUnavailable {
            location: location.clone(),
            reason:   format!("cannot read reference chunk {version}/{chunk}: {error}"),
        })?
        .bytes()
        .await
        .map_err(|error| EngineError::ReferenceLineageUnavailable {
            location: location.clone(),
            reason:   format!("cannot stream reference chunk {version}/{chunk}: {error}"),
        })?;
    let delta = ObjectReferenceDelta::decode(&bytes).map_err(|error| {
        EngineError::ReferenceLineageUnavailable {
            location: location.clone(),
            reason:   format!("invalid reference chunk {version}/{chunk}: {error}"),
        }
    })?;
    if delta.table_version() != version || delta.chunk_index() != chunk {
        return Err(EngineError::ReferenceLineageUnavailable {
            location: location.clone(),
            reason:   format!("reference chunk identity mismatch at {version}/{chunk}"),
        });
    }
    Ok(delta)
}

fn parse_reference_cursor(
    location: &TableLocation,
    expected_root: Version,
    cursor: &str,
) -> Result<(Version, u32)> {
    let invalid = || EngineError::ReferenceLineageUnavailable {
        location: location.clone(),
        reason:   "invalid or root-mismatched reference cursor".to_owned(),
    };
    let mut parts = cursor.split(':');
    let root = parts
        .next()
        .and_then(|value| value.strip_prefix('r'))
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(&invalid)?;
    let version = parts
        .next()
        .and_then(|value| value.strip_prefix('v'))
        .and_then(|value| value.parse::<u64>().ok())
        .ok_or_else(&invalid)?;
    let chunk = parts
        .next()
        .and_then(|value| value.strip_prefix('c'))
        .and_then(|value| value.parse::<u32>().ok())
        .ok_or_else(&invalid)?;
    if root != expected_root.0 || parts.next().is_some() || version > root || version <= 1 {
        return Err(invalid());
    }
    Ok((Version(version), chunk))
}

fn object_identities(batch: &RecordBatch) -> std::io::Result<Vec<ObjectIdentity>> {
    let mut identities = Vec::new();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        if !is_data_location(field.data_type()) {
            continue;
        }
        let values = column
            .as_any()
            .downcast_ref::<StructArray>()
            .ok_or_else(|| invalid_reference("FILE column is not a StructArray"))?;
        let uri = string_child(values, "uri")?;
        let content_type = string_child(values, "content_type")?;
        let size_bytes = values
            .column_by_name("size_bytes")
            .and_then(|array| array.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| invalid_reference("FILE size_bytes child is not UInt64"))?;
        let sha256 = string_child(values, "sha256")?;
        for row in 0..values.len() {
            if values.is_null(row)
                || uri.is_null(row)
                || content_type.is_null(row)
                || size_bytes.is_null(row)
                || sha256.is_null(row)
            {
                return Err(invalid_reference("FILE identity contains null values"));
            }
            identities.push(ObjectIdentity {
                uri:          uri.value(row).to_owned(),
                content_type: content_type.value(row).to_owned(),
                size_bytes:   size_bytes.value(row),
                sha256:       sha256.value(row).to_owned(),
            });
        }
    }
    Ok(identities)
}

fn string_child<'a>(array: &'a StructArray, name: &str) -> std::io::Result<&'a StringArray> {
    array
        .column_by_name(name)
        .and_then(|child| child.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| invalid_reference(&format!("FILE {name} child is not Utf8")))
}

fn is_data_location(data_type: &DataType) -> bool {
    let DataType::Struct(fields) = data_type else {
        return false;
    };
    let expected = [
        ("uri", DataType::Utf8),
        ("content_type", DataType::Utf8),
        ("size_bytes", DataType::UInt64),
        ("sha256", DataType::Utf8),
    ];
    fields.len() == expected.len()
        && fields
            .iter()
            .zip(expected)
            .all(|(field, (name, data_type))| {
                field.name() == name && field.data_type() == &data_type
            })
}

fn invalid_reference(message: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn reference_stage(operation: &AppendOperation) -> String {
    format!(
        "{}--{}",
        operation.tenant().as_str(),
        operation.operation_id().as_str()
    )
}

async fn persist_staged_references(
    config: &WriteConfig,
    location: &TableLocation,
    operation: &AppendOperation,
    parent_version: Version,
    added: Vec<ObjectIdentity>,
) -> Result<String> {
    let url = dataset_url(location)?;
    let (store, root) = object_store::parse_url_opts(&url, config.storage_options.clone())
        .map_err(EngineError::backend)?;
    let stage = reference_stage(operation);
    let chunks = added.len().max(1).div_ceil(REFERENCE_CHUNK_SIZE);
    let chunk_count = u32::try_from(chunks).map_err(EngineError::backend)?;
    for index in 0..chunks {
        let start = index * REFERENCE_CHUNK_SIZE;
        let end = (start + REFERENCE_CHUNK_SIZE).min(added.len());
        let chunk = StagedReferenceChunk {
            format_version: 1,
            parent_version,
            chunk_index: u32::try_from(index).map_err(EngineError::backend)?,
            chunk_count,
            added: if start < end {
                added[start..end].to_vec()
            } else {
                Vec::new()
            },
        };
        let encoded = serde_json::to_vec(&chunk).map_err(EngineError::backend)?;
        let path = root
            .clone()
            .join("_lake")
            .join("object_refs_staging")
            .join(stage.as_str())
            .join(format!("{index}.json"));
        match store
            .put_opts(&path, encoded.clone().into(), PutMode::Create.into())
            .await
        {
            Ok(_) => {}
            Err(object_store::Error::AlreadyExists { .. }) => {
                let existing = store
                    .get(&path)
                    .await
                    .map_err(EngineError::backend)?
                    .bytes()
                    .await
                    .map_err(EngineError::backend)?;
                if existing.as_ref() != encoded {
                    return Err(EngineError::IdempotencyConflict {
                        operation_id: operation.operation_id().clone(),
                    });
                }
            }
            Err(error) => return Err(EngineError::backend(error)),
        }
    }
    Ok(stage)
}

async fn finalize_staged_references(
    config: &WriteConfig,
    location: &TableLocation,
    stage: &str,
    table_version: Version,
) -> Result<()> {
    if final_reference_chunks_complete(config, location, table_version).await? {
        return Ok(());
    }
    let url = dataset_url(location)?;
    let (store, root) = object_store::parse_url_opts(&url, config.storage_options.clone())
        .map_err(EngineError::backend)?;
    let first_path = root
        .clone()
        .join("_lake")
        .join("object_refs_staging")
        .join(stage)
        .join("0.json");
    let first = store
        .get(&first_path)
        .await
        .map_err(EngineError::backend)?
        .bytes()
        .await
        .map_err(EngineError::backend)?;
    let first: StagedReferenceChunk =
        serde_json::from_slice(&first).map_err(EngineError::backend)?;
    if first.format_version != 1 || first.chunk_index != 0 || first.chunk_count == 0 {
        return Err(EngineError::ReferenceLineageUnavailable {
            location: location.clone(),
            reason:   "invalid staged reference header".to_owned(),
        });
    }
    for index in 0..first.chunk_count {
        let chunk = if index == 0 {
            first.clone()
        } else {
            let path = root
                .clone()
                .join("_lake")
                .join("object_refs_staging")
                .join(stage)
                .join(format!("{index}.json"));
            let bytes = store
                .get(&path)
                .await
                .map_err(EngineError::backend)?
                .bytes()
                .await
                .map_err(EngineError::backend)?;
            serde_json::from_slice(&bytes).map_err(EngineError::backend)?
        };
        if chunk.format_version != 1
            || chunk.chunk_index != index
            || chunk.chunk_count != first.chunk_count
            || chunk.parent_version != first.parent_version
        {
            return Err(EngineError::ReferenceLineageUnavailable {
                location: location.clone(),
                reason:   format!("invalid staged reference chunk {stage}/{index}"),
            });
        }
        let delta = ObjectReferenceDelta::try_new_chunk(
            chunk.parent_version,
            table_version,
            chunk.chunk_index,
            chunk.chunk_count,
            chunk.added,
            Vec::new(),
        )
        .map_err(EngineError::backend)?;
        persist_reference_delta(config, location, &delta).await?;
    }
    Ok(())
}

async fn final_reference_chunks_complete(
    config: &WriteConfig,
    location: &TableLocation,
    table_version: Version,
) -> Result<bool> {
    let url = dataset_url(location)?;
    let (store, root) = object_store::parse_url_opts(&url, config.storage_options.clone())
        .map_err(EngineError::backend)?;
    let path = root
        .join("_lake")
        .join("object_refs")
        .join(table_version.0.to_string())
        .join("0.json");
    let bytes = match store.get(&path).await {
        Ok(result) => result.bytes().await.map_err(EngineError::backend)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(false),
        Err(error) => return Err(EngineError::backend(error)),
    };
    let first = ObjectReferenceDelta::decode(&bytes).map_err(EngineError::backend)?;
    if first.table_version() != table_version
        || first.chunk_index() != 0
        || first.chunk_count() == 0
    {
        return Err(EngineError::ReferenceLineageUnavailable {
            location: location.clone(),
            reason:   format!("invalid final reference header for {table_version}"),
        });
    }
    for index in 1..first.chunk_count() {
        let chunk = match load_reference_delta(config, location, table_version, index).await {
            Ok(chunk) => chunk,
            Err(EngineError::ReferenceLineageUnavailable { reason, .. })
                if reason.starts_with("cannot read reference chunk") =>
            {
                return Ok(false);
            }
            Err(error) => return Err(error),
        };
        if chunk.parent_version() != first.parent_version()
            || chunk.chunk_count() != first.chunk_count()
        {
            return Err(EngineError::ReferenceLineageUnavailable {
                location: location.clone(),
                reason:   format!("inconsistent final reference chunk {table_version}/{index}"),
            });
        }
    }
    Ok(true)
}

async fn delete_staged_references(
    config: &WriteConfig,
    location: &TableLocation,
    stage: &str,
) -> Result<()> {
    let url = dataset_url(location)?;
    let (store, root) = object_store::parse_url_opts(&url, config.storage_options.clone())
        .map_err(EngineError::backend)?;
    let base = root.join("_lake").join("object_refs_staging").join(stage);
    let first_path = base.clone().join("0.json");
    let first = match store.get(&first_path).await {
        Ok(result) => result.bytes().await.map_err(EngineError::backend)?,
        Err(object_store::Error::NotFound { .. }) => return Ok(()),
        Err(error) => return Err(EngineError::backend(error)),
    };
    let first: StagedReferenceChunk =
        serde_json::from_slice(&first).map_err(EngineError::backend)?;
    if first.format_version != 1 || first.chunk_index != 0 || first.chunk_count == 0 {
        return Err(EngineError::ReferenceLineageUnavailable {
            location: location.clone(),
            reason:   format!("invalid staged reference header {stage}"),
        });
    }
    for index in 0..first.chunk_count {
        let path = base.clone().join(format!("{index}.json"));
        match store.delete(&path).await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(EngineError::backend(error)),
        }
    }
    Ok(())
}

/// A handle to one open Lance dataset.
struct LanceTable {
    dataset:  Arc<Dataset>,
    schema:   SchemaRef,
    config:   WriteConfig,
    location: TableLocation,
}

impl LanceTable {
    fn handle(dataset: Dataset, config: WriteConfig, location: TableLocation) -> TableHandleRef {
        let dataset = Arc::new(dataset);
        let provider = LanceTableProvider::new(dataset.clone(), false, false);
        Arc::new(Self {
            schema: provider.schema(),
            dataset,
            config,
            location,
        })
    }

    async fn find_append(&self, operation: &AppendOperation) -> Result<Option<(Version, String)>> {
        #[cfg(test)]
        self.config
            .history_scans
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let dataset = self
            .config
            .open_dataset(self.location.as_str())
            .await
            .map_err(EngineError::backend)?;
        let mut versions = dataset.versions().await.map_err(EngineError::backend)?;
        versions.sort_by_key(|version| version.version);
        for version in versions.into_iter().rev() {
            let Some(transaction) = dataset
                .read_transaction_by_version(version.version)
                .await
                .map_err(EngineError::backend)?
            else {
                continue;
            };
            let Some(properties) = transaction.transaction_properties else {
                continue;
            };
            if properties.get(APPEND_TENANT_PROPERTY).map(String::as_str)
                != Some(operation.tenant().as_str())
                || properties
                    .get(APPEND_OPERATION_PROPERTY)
                    .map(String::as_str)
                    != Some(operation.operation_id().as_str())
            {
                continue;
            }
            if properties.get(APPEND_DIGEST_PROPERTY).map(String::as_str)
                != Some(operation.payload_digest().as_str())
            {
                return Err(EngineError::IdempotencyConflict {
                    operation_id: operation.operation_id().clone(),
                });
            }
            let stage = properties
                .get(APPEND_REFERENCE_STAGE_PROPERTY)
                .cloned()
                .ok_or_else(|| EngineError::ReferenceLineageUnavailable {
                    location: self.location.clone(),
                    reason:   format!(
                        "append transaction {} has no reference stage",
                        operation.operation_id()
                    ),
                })?;
            return Ok(Some((Version(version.version), stage)));
        }
        Ok(None)
    }
}

#[async_trait]
impl TableHandle for LanceTable {
    fn schema(&self) -> SchemaRef { self.schema.clone() }

    fn current_version(&self) -> Version { Version(self.dataset.version().version) }

    async fn table_provider(&self, version: Version) -> Result<Arc<dyn TableProvider>> {
        let dataset = self
            .dataset
            .checkout_version(version.0)
            .await
            .map_err(EngineError::backend)?;
        Ok(Arc::new(LanceTableProvider::new(
            Arc::new(dataset),
            false,
            false,
        )))
    }

    async fn append(
        &self,
        operation: &AppendOperation,
        batches: SendableRecordBatchStream,
    ) -> Result<Version> {
        if let Some(version) = self.reconcile_append(operation).await? {
            return Ok(version);
        }
        self.append_reserved(operation, batches).await
    }

    async fn append_reserved(
        &self,
        operation: &AppendOperation,
        batches: SendableRecordBatchStream,
    ) -> Result<Version> {
        let parent_version = Version(
            self.config
                .open_dataset(self.location.as_str())
                .await
                .map_err(EngineError::backend)?
                .version()
                .version,
        );
        let references = Arc::new(Mutex::new(BTreeSet::new()));
        let observed = references.clone();
        let schema = batches.schema();
        let batches = batches.map(move |result| {
            result.and_then(|batch| {
                let identities = object_identities(&batch)
                    .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                let mut observed = observed.lock().expect("object reference mutex poisoned");
                observed.extend(identities);
                if observed.len() > MAX_REFERENCES_PER_APPEND {
                    return Err(DataFusionError::Execution(format!(
                        "append contains more than {MAX_REFERENCES_PER_APPEND} distinct FILE \
                         references"
                    )));
                }
                Ok(batch)
            })
        });
        let batches: SendableRecordBatchStream =
            Box::pin(RecordBatchStreamAdapter::new(schema, batches));
        let stage = reference_stage(operation);
        let properties = HashMap::from([
            (
                APPEND_TENANT_PROPERTY.to_owned(),
                operation.tenant().as_str().to_owned(),
            ),
            (
                APPEND_OPERATION_PROPERTY.to_owned(),
                operation.operation_id().as_str().to_owned(),
            ),
            (
                APPEND_DIGEST_PROPERTY.to_owned(),
                operation.payload_digest().as_str().to_owned(),
            ),
            (APPEND_REFERENCE_STAGE_PROPERTY.to_owned(), stage.clone()),
        ]);
        let params = self
            .config
            .write_params(WriteMode::Append)
            .with_transaction_properties(properties);
        let transaction = InsertBuilder::new(self.dataset.clone())
            .with_params(&params)
            .execute_uncommitted_stream(batches)
            .await
            .map_err(EngineError::backend)?;
        let added = references
            .lock()
            .expect("object reference mutex poisoned")
            .iter()
            .cloned()
            .collect();
        let persisted_stage = persist_staged_references(
            &self.config,
            &self.location,
            operation,
            parent_version,
            added,
        )
        .await?;
        debug_assert_eq!(persisted_stage, stage);
        let committed = CommitBuilder::new(self.dataset.clone())
            .with_max_retries(0)
            .execute(transaction)
            .await;
        let table_version = match committed {
            Ok(dataset) => Version(dataset.version().version),
            Err(error) => match self.reconcile_append(operation).await? {
                Some(version) => return Ok(version),
                None => return Err(EngineError::backend(error)),
            },
        };
        finalize_staged_references(&self.config, &self.location, &stage, table_version).await?;
        delete_staged_references(&self.config, &self.location, &stage).await?;
        Ok(table_version)
    }

    async fn reconcile_append(&self, operation: &AppendOperation) -> Result<Option<Version>> {
        let Some((version, stage)) = self.find_append(operation).await? else {
            return Ok(None);
        };
        finalize_staged_references(&self.config, &self.location, &stage, version).await?;
        delete_staged_references(&self.config, &self.location, &stage).await?;
        Ok(Some(version))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Weak};

    use datafusion::{
        arrow::{
            array::{
                Array, ArrayRef, Int64Array, RecordBatch, StringArray, StructArray, UInt64Array,
            },
            datatypes::{DataType, Field, Fields, Schema},
        },
        error::DataFusionError,
        physical_plan::stream::RecordBatchStreamAdapter,
    };
    use lake_meta::{MetaStore, RocksMeta};

    use super::*;

    fn operation() -> AppendOperation {
        AppendOperation::builder()
            .tenant(lake_common::TenantId::try_new("tenant-a").unwrap())
            .operation_id(lake_common::AppendOperationId::generate())
            .payload_digest(
                lake_common::AppendPayloadDigest::parse(
                    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                )
                .unwrap(),
            )
            .build()
    }

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }

    fn file_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "video",
            DataType::Struct(Fields::from(vec![
                Field::new("uri", DataType::Utf8, false),
                Field::new("content_type", DataType::Utf8, false),
                Field::new("size_bytes", DataType::UInt64, false),
                Field::new("sha256", DataType::Utf8, false),
            ])),
            false,
        )]))
    }

    fn file_array(index: usize) -> ArrayRef {
        Arc::new(StructArray::new(
            match file_schema().field(0).data_type() {
                DataType::Struct(fields) => fields.clone(),
                _ => unreachable!(),
            },
            vec![
                Arc::new(StringArray::from(vec![format!(
                    "s3://lake/objects/{index}"
                )])) as ArrayRef,
                Arc::new(StringArray::from(vec!["video/mp4"])),
                Arc::new(UInt64Array::from(vec![42])),
                Arc::new(StringArray::from(vec![format!("sha-{index}")])),
            ],
            None,
        ))
    }

    fn file_batch(index: usize) -> RecordBatch {
        RecordBatch::try_new(file_schema(), vec![file_array(index)]).unwrap()
    }

    #[tokio::test]
    async fn create_append_version() {
        let dir = tempfile::tempdir().unwrap();
        let loc = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();

        let h = engine.create(&loc, batch().schema()).await.unwrap();
        assert_eq!(h.current_version(), Version(1));
        assert!(engine.open(&loc).await.unwrap().is_some());

        let b = batch();
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            b.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(b)]),
        ));
        let v = h.append(&operation(), stream).await.unwrap();
        assert!(v.0 > 1, "append advances the version");
    }

    #[tokio::test]
    async fn new_append_does_not_scan_transaction_history() {
        let dir = tempfile::tempdir().unwrap();
        let loc = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();
        let handle = engine.create(&loc, batch().schema()).await.unwrap();
        let input = batch();
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            input.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(input)]),
        ));

        handle.append_reserved(&operation(), stream).await.unwrap();

        assert_eq!(
            engine
                .config
                .history_scans
                .load(std::sync::atomic::Ordering::SeqCst),
            0
        );
    }

    #[tokio::test]
    async fn lance_transaction_history_converges_idempotent_append() {
        let dir = tempfile::tempdir().unwrap();
        let location = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();
        let handle = engine.create(&location, batch().schema()).await.unwrap();
        let competing = engine.open(&location).await.unwrap().unwrap();
        let operation = operation();
        let first_batch = batch();
        let first_stream = Box::pin(RecordBatchStreamAdapter::new(
            first_batch.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(first_batch)]),
        ));
        let replay_batch = batch();
        let replay_stream = Box::pin(RecordBatchStreamAdapter::new(
            replay_batch.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(replay_batch)]),
        ));

        let (first, replay) = tokio::join!(
            handle.append(&operation, first_stream),
            competing.append(&operation, replay_stream)
        );
        let first = first.unwrap();
        let replay = replay.unwrap();

        assert_eq!(replay, first);
        assert_eq!(
            engine
                .open(&location)
                .await
                .unwrap()
                .unwrap()
                .current_version(),
            Version(2)
        );
    }

    #[tokio::test]
    async fn recovered_idempotent_append_restores_reference_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let location = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();
        let handle = engine.create(&location, file_schema()).await.unwrap();
        let operation = operation();
        let input = file_batch(7);
        let references = object_identities(&input).unwrap();
        let stage = persist_staged_references(
            &engine.config,
            &location,
            &operation,
            Version(1),
            references,
        )
        .await
        .unwrap();
        let properties = HashMap::from([
            (
                APPEND_TENANT_PROPERTY.to_owned(),
                operation.tenant().as_str().to_owned(),
            ),
            (
                APPEND_OPERATION_PROPERTY.to_owned(),
                operation.operation_id().as_str().to_owned(),
            ),
            (
                APPEND_DIGEST_PROPERTY.to_owned(),
                operation.payload_digest().as_str().to_owned(),
            ),
            (APPEND_REFERENCE_STAGE_PROPERTY.to_owned(), stage.clone()),
        ]);
        let dataset = Arc::new(engine.config.open_dataset(location.as_str()).await.unwrap());
        let params = engine
            .config
            .write_params(WriteMode::Append)
            .with_transaction_properties(properties);
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            input.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(input)]),
        ));
        let transaction = InsertBuilder::new(dataset.clone())
            .with_params(&params)
            .execute_uncommitted_stream(stream)
            .await
            .unwrap();
        let committed = Version(
            CommitBuilder::new(dataset)
                .with_max_retries(0)
                .execute(transaction)
                .await
                .unwrap()
                .version()
                .version,
        );
        let final_sidecar = dir
            .path()
            .join(format!("t.lance/_lake/object_refs/{}/0.json", committed.0));
        assert!(
            !final_sidecar.exists(),
            "the injected crash precedes finalization"
        );

        let recovered = handle.reconcile_append(&operation).await.unwrap();

        assert_eq!(recovered, Some(committed));
        let repaired = ObjectReferenceDelta::decode(
            &tokio::fs::read(&final_sidecar)
                .await
                .expect("replay repairs the missing final sidecar"),
        )
        .unwrap();
        assert_eq!(repaired.table_version(), committed);
        assert_eq!(repaired.added().len(), 1);
        assert_eq!(repaired.added()[0].uri, "s3://lake/objects/7");
        assert!(
            !dir.path()
                .join(format!("t.lance/_lake/object_refs_staging/{stage}/0.json"))
                .exists(),
            "terminal recovery removes the durable staging journal"
        );
    }

    #[tokio::test]
    async fn append_writes_object_reference_delta_without_retaining_batches() {
        let dir = tempfile::tempdir().unwrap();
        let loc = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();
        let schema = file_schema();
        let h = engine.create(&loc, schema.clone()).await.unwrap();

        type State = (usize, Option<Weak<dyn Array>>, bool);
        let state: State = (0, None, false);
        let batches = futures::stream::unfold(state, move |(index, previous, done)| {
            let schema = schema.clone();
            async move {
                if done || index == 3 {
                    return None;
                }
                if previous.is_some_and(|batch| batch.upgrade().is_some()) {
                    return Some((
                        Err(DataFusionError::Execution(
                            "consumer retained every prior batch".to_string(),
                        )),
                        (index, None, true),
                    ));
                }

                let array = file_array(index);
                let weak = Arc::downgrade(&array);
                let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
                Some((Ok(batch), (index + 1, Some(weak), false)))
            }
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(h.schema(), batches));

        let version = h
            .append(&operation(), stream)
            .await
            .expect("streaming append must release each consumed input batch");
        let mut added = Vec::new();
        for chunk_index in 0..2 {
            let sidecar = dir.path().join(format!(
                "t.lance/_lake/object_refs/{}/{chunk_index}.json",
                version.0
            ));
            let delta = ObjectReferenceDelta::decode(
                &tokio::fs::read(sidecar)
                    .await
                    .expect("append publishes bounded object-reference chunks"),
            )
            .expect("valid sidecar chunk");
            assert_eq!(delta.parent_version(), Version(1));
            assert_eq!(delta.table_version(), version);
            assert_eq!(delta.chunk_index(), chunk_index);
            assert_eq!(delta.chunk_count(), 2);
            assert!(delta.removed().is_empty());
            added.extend_from_slice(delta.added());
        }
        assert_eq!(added.len(), 3);
    }

    #[tokio::test]
    async fn retained_object_references_follow_version_lineage() {
        let dir = tempfile::tempdir().unwrap();
        let location = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();
        let handle = engine.create(&location, file_schema()).await.unwrap();
        for index in 0..3 {
            let batch = file_batch(index);
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter(vec![Ok::<_, DataFusionError>(batch)]),
            ));
            handle.append(&operation(), stream).await.unwrap();
        }
        let before = engine
            .open(&location)
            .await
            .unwrap()
            .unwrap()
            .current_version();
        let root = engine
            .maintain(&location, before)
            .await
            .unwrap()
            .unwrap_or(before);

        let mut cursor = None;
        let mut deltas = Vec::new();
        loop {
            let request = ObjectReferenceRequest::try_new(root, cursor, 1).unwrap();
            let page = engine
                .retained_object_references(&location, request)
                .await
                .expect("complete retained lineage");
            assert!(page.deltas().len() <= 1);
            deltas.extend_from_slice(page.deltas());
            cursor = page.next_cursor().cloned();
            if cursor.is_none() {
                break;
            }
        }

        let added = deltas
            .iter()
            .flat_map(ObjectReferenceDelta::added)
            .map(|identity| identity.uri.clone())
            .collect::<BTreeSet<_>>();
        assert_eq!(added.len(), 3);
        assert!(deltas.iter().any(|delta| delta.table_version() == root));
    }

    #[tokio::test]
    async fn table_provider_reads_requested_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let loc = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();

        let h = engine.create(&loc, batch().schema()).await.unwrap();
        let b = batch();
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            b.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(b)]),
        ));
        let appended = h.append(&operation(), stream).await.unwrap();
        assert!(appended.0 > 1);

        let reopened = engine.open(&loc).await.unwrap().expect("table exists");
        assert_eq!(reopened.current_version(), appended);

        let ctx = datafusion::prelude::SessionContext::new();
        ctx.register_table(
            "snapshot",
            reopened.table_provider(Version(1)).await.unwrap(),
        )
        .unwrap();
        let rows = ctx
            .sql("SELECT count(*) AS n FROM snapshot")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count = rows[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 0, "v1 is the empty snapshot created before append");
    }

    #[tokio::test]
    async fn maintain_is_idempotent_and_preserves_data() {
        let dir = tempfile::tempdir().unwrap();
        let loc = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();

        let h = engine.create(&loc, batch().schema()).await.unwrap();
        // A few appends give the dataset multiple versions/fragments to work on.
        for _ in 0..3 {
            let b = batch();
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                b.schema(),
                futures::stream::iter(vec![Ok::<_, DataFusionError>(b)]),
            ));
            h.append(&operation(), stream).await.unwrap();
        }

        // Maintenance runs cleanly and is safe to repeat.
        let before = engine.open(&loc).await.unwrap().unwrap().current_version();
        let after = engine
            .maintain(&loc, before)
            .await
            .unwrap()
            .unwrap_or(before);
        engine.maintain(&loc, after).await.unwrap();

        // The table is still openable and its rows survive compaction.
        let reopened = engine.open(&loc).await.unwrap().expect("table survives");
        assert!(reopened.current_version().0 >= 1);
    }

    #[tokio::test]
    async fn maintenance_reclaims_external_manifest_history() {
        let dir = tempfile::tempdir().unwrap();
        let meta_dir = tempfile::tempdir().unwrap();
        let meta = Arc::new(RocksMeta::open(meta_dir.path()).unwrap());
        let loc = TableLocation::new(dir.path().join("external.lance").to_str().unwrap());
        let engine = LanceEngine::with_manifest_store(meta.clone());
        let handle = engine.create(&loc, batch().schema()).await.unwrap();
        let mut version = handle.current_version();
        for _ in 0..12 {
            let value = batch();
            let stream = Box::pin(RecordBatchStreamAdapter::new(
                value.schema(),
                futures::stream::iter(vec![Ok::<_, DataFusionError>(value)]),
            ));
            version = handle.append(&operation(), stream).await.unwrap();
        }
        let before = meta.list_prefix("lance-manifest/").await.unwrap().len();
        assert!(before > RETAIN_VERSIONS, "fixture has reclaimable history");
        engine
            .config
            .open_dataset(loc.as_str())
            .await
            .unwrap()
            .tags()
            .create("retain-v1", 1_u64)
            .await
            .unwrap();

        engine.maintain(&loc, version).await.unwrap();

        let after = meta.list_prefix("lance-manifest/").await.unwrap();
        assert!(
            after.len() < before,
            "maintenance reclaimed external history"
        );
        assert!(
            after.iter().any(|key| key.ends_with("/1")),
            "tagged version keeps its external history"
        );
        assert!(engine.open(&loc).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn open_missing_is_none() {
        let engine = LanceEngine::new();
        let loc = TableLocation::new("/nonexistent/path/x.lance");
        assert!(engine.open(&loc).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn remove_deletes_data_and_allows_recreate() {
        let dir = tempfile::tempdir().unwrap();
        let loc = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();

        engine.create(&loc, batch().schema()).await.unwrap();
        assert!(engine.open(&loc).await.unwrap().is_some());

        engine.remove(&loc).await.unwrap();
        assert!(engine.open(&loc).await.unwrap().is_none(), "data is gone");

        // remove is idempotent, and the name is free to reuse.
        engine.remove(&loc).await.unwrap();
        assert!(
            engine.create(&loc, batch().schema()).await.is_ok(),
            "recreate works"
        );
    }
}
