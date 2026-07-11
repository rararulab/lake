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
use lake_common::{ObjectReferenceDelta, TableLocation, Version};

use crate::error::{EngineError, Result};

pub type TableEngineRef = Arc<dyn TableEngine>;
pub type TableHandleRef = Arc<dyn TableHandle>;
pub const MAX_REFERENCE_PAGE_DELTAS: usize = 1_024;

/// Opaque continuation owned and interpreted by one engine implementation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectReferenceCursor(String);

impl ObjectReferenceCursor {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self { Self(value.into()) }

    #[must_use]
    pub fn as_str(&self) -> &str { &self.0 }
}

/// One bounded request rooted at the registry-visible table version.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectReferenceRequest {
    root_version: Version,
    cursor:       Option<ObjectReferenceCursor>,
    limit:        usize,
}

impl ObjectReferenceRequest {
    pub fn try_new(
        root_version: Version,
        cursor: Option<ObjectReferenceCursor>,
        limit: usize,
    ) -> Result<Self> {
        if limit == 0 || limit > MAX_REFERENCE_PAGE_DELTAS {
            return Err(EngineError::InvalidReferencePageSize { size: limit });
        }
        Ok(Self {
            root_version,
            cursor,
            limit,
        })
    }

    #[must_use]
    pub const fn root_version(&self) -> Version { self.root_version }

    #[must_use]
    pub fn cursor(&self) -> Option<&ObjectReferenceCursor> { self.cursor.as_ref() }

    #[must_use]
    pub const fn limit(&self) -> usize { self.limit }
}

/// One deterministic page of live managed-object identities.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ObjectReferencePage {
    deltas:      Vec<ObjectReferenceDelta>,
    next_cursor: Option<ObjectReferenceCursor>,
}

impl ObjectReferencePage {
    #[must_use]
    pub fn new(
        deltas: Vec<ObjectReferenceDelta>,
        next_cursor: Option<ObjectReferenceCursor>,
    ) -> Self {
        Self {
            deltas,
            next_cursor,
        }
    }

    #[must_use]
    pub fn deltas(&self) -> &[ObjectReferenceDelta] { &self.deltas }

    #[must_use]
    pub fn next_cursor(&self) -> Option<&ObjectReferenceCursor> { self.next_cursor.as_ref() }
}

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

    /// Compact fragments and reclaim old versions starting from the registered
    /// `version`. Returns the new version when compaction commits, or `None`
    /// when no version-producing work was needed.
    ///
    /// The caller must publish a returned version through the registry CAS
    /// before readers may observe it.
    async fn maintain(&self, location: &TableLocation, version: Version)
    -> Result<Option<Version>>;

    /// Enumerate object identities reachable from the registry root and every
    /// engine-retained snapshot, without scanning table RecordBatches.
    async fn retained_object_references(
        &self,
        location: &TableLocation,
        request: ObjectReferenceRequest,
    ) -> Result<ObjectReferencePage>;
}

/// A handle to one table backed by an engine. Resolves to DataFusion for
/// reads and accepts appends for writes.
#[async_trait]
pub trait TableHandle: Send + Sync {
    /// The table's Arrow schema at its current version.
    fn schema(&self) -> SchemaRef;

    /// The current (latest) committed version.
    fn current_version(&self) -> Version;

    /// A DataFusion table pinned to `version` — how the query layer reads.
    ///
    /// Implementations must not silently substitute their latest version: the
    /// registry pointer is lake's visibility boundary.
    async fn table_provider(&self, version: Version) -> Result<Arc<dyn TableProvider>>;

    /// Append rows, producing a new immutable version. The engine performs
    /// its own manifest-first-then-pointer commit; lake's registry pointer
    /// update happens separately in the metadata layer.
    async fn append(&self, batches: SendableRecordBatchStream) -> Result<Version>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_reference_requests_are_bounded() {
        assert!(matches!(
            ObjectReferenceRequest::try_new(Version(7), None, 0),
            Err(EngineError::InvalidReferencePageSize { size: 0 })
        ));
        assert!(matches!(
            ObjectReferenceRequest::try_new(Version(7), None, 1_025),
            Err(EngineError::InvalidReferencePageSize { size: 1_025 })
        ));
        let request = ObjectReferenceRequest::try_new(
            Version(7),
            Some(ObjectReferenceCursor::new("v7:page-2")),
            512,
        )
        .unwrap();
        assert_eq!(request.root_version(), Version(7));
        assert_eq!(request.limit(), 512);
        assert_eq!(request.cursor().unwrap().as_str(), "v7:page-2");

        let delta = ObjectReferenceDelta::try_new(
            Version(7),
            Version(8),
            vec![lake_common::ObjectIdentity {
                uri:          "s3://lake/objects/a".to_owned(),
                content_type: "video/mp4".to_owned(),
                size_bytes:   1,
                sha256:       "aa".to_owned(),
            }],
            Vec::new(),
        )
        .unwrap();
        let page = ObjectReferencePage::new(vec![delta.clone()], None);
        assert_eq!(page.deltas(), &[delta]);
    }
}
