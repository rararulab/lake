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
//! v0 uses Lance's default commit, which is atomic on a local filesystem.
//! ponytail: on object stores without atomic put-if-not-exists (S3), Lance
//! needs an `ExternalManifestStore` backed by our `MetaStore` — that adapter
//! is v1, see `docs/architecture.md`.
//!
//! All Arrow types come through `datafusion::arrow` so the engine and the
//! query layer share exactly one Arrow version.

use std::sync::Arc;

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
use lance::{
    Dataset,
    datafusion::LanceTableProvider,
    dataset::{WriteMode, WriteParams},
};

/// A `TableEngine` backed by Lance datasets.
#[derive(Debug, Default)]
pub struct LanceEngine;

impl LanceEngine {
    pub fn new() -> Self { Self }
}

#[async_trait]
impl TableEngine for LanceEngine {
    fn kind(&self) -> &'static str { "lance" }

    async fn create(&self, location: &TableLocation, schema: SchemaRef) -> Result<TableHandleRef> {
        if Dataset::open(location.as_str()).await.is_ok() {
            return Err(EngineError::already_exists(location.clone()));
        }
        let empty = RecordBatchIterator::new(
            std::iter::empty::<std::result::Result<RecordBatch, ArrowError>>(),
            schema,
        );
        let params = WriteParams {
            mode: WriteMode::Create,
            ..Default::default()
        };
        let dataset = Dataset::write(empty, location.as_str(), Some(params))
            .await
            .map_err(EngineError::backend)?;
        Ok(LanceTable::handle(dataset))
    }

    async fn open(&self, location: &TableLocation) -> Result<Option<TableHandleRef>> {
        match Dataset::open(location.as_str()).await {
            Ok(dataset) => Ok(Some(LanceTable::handle(dataset))),
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
}

impl LanceTable {
    fn handle(dataset: Dataset) -> TableHandleRef {
        let dataset = Arc::new(dataset);
        let provider = LanceTableProvider::new(dataset.clone(), false, false);
        Arc::new(Self {
            uri: dataset.uri().to_string(),
            schema: provider.schema(),
            dataset,
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
        let params = WriteParams {
            mode: WriteMode::Append,
            ..Default::default()
        };
        let dataset = Dataset::write(reader, &self.uri, Some(params))
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
