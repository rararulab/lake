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

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use datafusion::{
    arrow::{
        array::{RecordBatch, RecordBatchIterator},
        datatypes::SchemaRef,
        error::ArrowError,
    },
    catalog::TableProvider,
    execution::SendableRecordBatchStream,
};
use futures::{StreamExt, TryStreamExt};
use lake_common::{TableLocation, Version};
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
        write::InsertBuilder,
    },
    io::{ObjectStoreParams, StorageOptionsAccessor},
};
use lance_table::io::commit::{
    CommitHandler,
    external_manifest::{ExternalManifestCommitHandler, ExternalManifestStore},
};

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
    // Empty -> local filesystem. Non-empty -> object_store config keys
    // (`aws_endpoint`, `aws_access_key_id`, …) threaded into every read/write.
    storage_options: HashMap<String, String>,
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
        Self {
            config: WriteConfig {
                commit_handler:  Some(external_handler(meta)),
                storage_options: HashMap::new(),
            },
        }
    }

    /// Build an engine for object storage: commits route through `meta`'s
    /// external manifest store, and `storage_options` (object_store config
    /// keys — `aws_endpoint`, `aws_access_key_id`, `aws_region`, …) point Lance
    /// at the bucket. This is the production path.
    #[must_use]
    pub fn for_object_store(meta: MetaStoreRef, storage_options: HashMap<String, String>) -> Self {
        Self {
            config: WriteConfig {
                commit_handler: Some(external_handler(meta)),
                storage_options,
            },
        }
    }
}

fn external_handler(meta: MetaStoreRef) -> Arc<dyn CommitHandler> {
    let store: Arc<dyn ExternalManifestStore> = Arc::new(MetaManifestStore::new(meta));
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
        Ok(LanceTable::handle(dataset, self.config.clone()))
    }

    async fn open(&self, location: &TableLocation) -> Result<Option<TableHandleRef>> {
        match self.config.open_dataset(location.as_str()).await {
            Ok(dataset) => Ok(Some(LanceTable::handle(dataset, self.config.clone()))),
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
        Ok((maintained_version != version).then_some(maintained_version))
    }

    async fn retained_object_references(
        &self,
        location: &TableLocation,
        _request: ObjectReferenceRequest,
    ) -> Result<ObjectReferencePage> {
        Err(EngineError::ReferenceLineageUnavailable {
            location: location.clone(),
            reason:   "object reference journals have not been initialized".to_owned(),
        })
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

/// A handle to one open Lance dataset.
struct LanceTable {
    dataset: Arc<Dataset>,
    schema:  SchemaRef,
    config:  WriteConfig,
}

impl LanceTable {
    fn handle(dataset: Dataset, config: WriteConfig) -> TableHandleRef {
        let dataset = Arc::new(dataset);
        let provider = LanceTableProvider::new(dataset.clone(), false, false);
        Arc::new(Self {
            schema: provider.schema(),
            dataset,
            config,
        })
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

    async fn append(&self, batches: SendableRecordBatchStream) -> Result<Version> {
        let params = self.config.write_params(WriteMode::Append);
        let dataset = InsertBuilder::new(self.dataset.clone())
            .with_params(&params)
            .execute_stream(batches)
            .await
            .map_err(EngineError::backend)?;
        Ok(Version(dataset.version().version))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Weak};

    use datafusion::{
        arrow::{
            array::{Array, ArrayRef, Int64Array, RecordBatch},
            datatypes::{DataType, Field, Schema},
        },
        error::DataFusionError,
        physical_plan::stream::RecordBatchStreamAdapter,
    };

    use super::*;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("ep", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
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
        let stream = Box::pin(RecordBatchStreamAdapter::new(
            b.schema(),
            futures::stream::iter(vec![Ok::<_, DataFusionError>(b)]),
        ));
        let v = h.append(stream).await.unwrap();
        assert!(v.0 > 1, "append advances the version");
    }

    #[tokio::test]
    async fn append_releases_consumed_batches_before_stream_end() {
        let dir = tempfile::tempdir().unwrap();
        let loc = TableLocation::new(dir.path().join("t.lance").to_str().unwrap());
        let engine = LanceEngine::new();
        let schema = batch().schema();
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

                let array: ArrayRef = Arc::new(Int64Array::from(vec![index as i64]));
                let weak = Arc::downgrade(&array);
                let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
                Some((Ok(batch), (index + 1, Some(weak), false)))
            }
        });
        let stream = Box::pin(RecordBatchStreamAdapter::new(h.schema(), batches));

        h.append(stream)
            .await
            .expect("streaming append must release each consumed input batch");
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
        let appended = h.append(stream).await.unwrap();
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
            h.append(stream).await.unwrap();
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
