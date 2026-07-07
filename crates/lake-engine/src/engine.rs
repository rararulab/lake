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

//! The storage-engine traits.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::{
    arrow::datatypes::SchemaRef, catalog::TableProvider, execution::SendableRecordBatchStream,
};
use lake_common::{TableLocation, Version};

use crate::error::Result;

pub type TableEngineRef = Arc<dyn TableEngine>;
pub type TableHandleRef = Arc<dyn TableHandle>;

/// A storage engine: creates and opens tables at a [`TableLocation`].
///
/// One process holds one engine per backend kind. The engine owns the
/// per-table manifest / versioning; lake's registry only records which
/// engine and location back a table name.
#[async_trait]
pub trait TableEngine: Send + Sync {
    /// Short stable identifier persisted in the registry (e.g. `"lance"`),
    /// so a table opened later is routed back to the right engine.
    fn kind(&self) -> &'static str;

    /// Create a new, empty table with the given schema. Fails if one
    /// already exists at `location`.
    async fn create(&self, location: &TableLocation, schema: SchemaRef) -> Result<TableHandleRef>;

    /// Open an existing table, or `None` if nothing lives at `location`.
    async fn open(&self, location: &TableLocation) -> Result<Option<TableHandleRef>>;

    /// Delete all storage backing a table. Idempotent — removing an absent
    /// table is not an error. Used by drop-table; the registry entry is
    /// removed separately by the metadata layer.
    async fn remove(&self, location: &TableLocation) -> Result<()>;
}

/// A handle to one table backed by an engine. Resolves to DataFusion for
/// reads and accepts appends for writes.
#[async_trait]
pub trait TableHandle: Send + Sync {
    /// The table's Arrow schema at its current version.
    fn schema(&self) -> SchemaRef;

    /// The current (latest) committed version.
    fn current_version(&self) -> Version;

    /// A DataFusion table at a specific snapshot — how the query layer reads.
    fn table_provider(&self, version: Version) -> Arc<dyn TableProvider>;

    /// Append rows, producing a new immutable version. The engine performs
    /// its own manifest-first-then-pointer commit; lake's registry pointer
    /// update happens separately in the metadata layer.
    async fn append(&self, batches: SendableRecordBatchStream) -> Result<Version>;
}
