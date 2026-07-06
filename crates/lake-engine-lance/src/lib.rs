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
//! put-if-not-exists it needs for concurrent commits — see [`manifest_store`].

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
use futures::TryStreamExt;
use lake_common::{TableLocation, Version};
use lake_engine::{EngineError, Result, TableEngine, TableHandle, TableHandleRef};
use lake_meta::MetaStoreRef;
use lance::{
    Dataset,
    datafusion::LanceTableProvider,
    dataset::{WriteMode, WriteParams, builder::DatasetBuilder},
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
}

/// A handle to one open Lance dataset.
struct LanceTable {
    uri:     String,
    dataset: Arc<Dataset>,
    schema:  SchemaRef,
    config:  WriteConfig,
}

impl LanceTable {
    fn handle(dataset: Dataset, config: WriteConfig) -> TableHandleRef {
        let dataset = Arc::new(dataset);
        let provider = LanceTableProvider::new(dataset.clone(), false, false);
        Arc::new(Self {
            uri: dataset.uri().to_string(),
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

    fn table_provider(&self, _version: Version) -> Arc<dyn TableProvider> {
        // ponytail: v0 always serves the version this handle was opened at.
        // Snapshot-pinned reads (Dataset::checkout_version) land when
        // cross-version isolation matters — see architecture.md.
        Arc::new(LanceTableProvider::new(self.dataset.clone(), false, false))
    }

    async fn append(&self, batches: SendableRecordBatchStream) -> Result<Version> {
        let schema = batches.schema();
        // ponytail: collect the stream before writing — fine for bounded
        // append batches; stream straight through when payloads grow.
        let collected: Vec<RecordBatch> =
            batches.try_collect().await.map_err(EngineError::backend)?;
        let reader = RecordBatchIterator::new(
            collected
                .into_iter()
                .map(std::result::Result::<_, ArrowError>::Ok),
            schema,
        );
        let dataset = Dataset::write(
            reader,
            &self.uri,
            Some(self.config.write_params(WriteMode::Append)),
        )
        .await
        .map_err(EngineError::backend)?;
        Ok(Version(dataset.version().version))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::{
        arrow::{
            array::{Int64Array, RecordBatch},
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
    async fn open_missing_is_none() {
        let engine = LanceEngine::new();
        let loc = TableLocation::new("/nonexistent/path/x.lance");
        assert!(engine.open(&loc).await.unwrap().is_none());
    }
}
