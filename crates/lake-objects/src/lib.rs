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

//! Managed large-object values and their Arrow representation.

use std::{ops::Range, path::PathBuf, pin::Pin, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::{
    array::{Array, ArrayRef, StringArray, StructArray, UInt64Array},
    datatypes::{DataType, Field, Fields},
};
use lake_common::DataLocation;
use snafu::{OptionExt, Snafu};
use tokio::io::AsyncRead;

mod checkpoint;
mod gc;
mod gc_apply;
mod gc_plan;
mod inventory;
mod local;
mod reference_index;
pub use gc::{GcPlanPage, GcPlanner, ObjectCandidate};
pub use gc_apply::{DeleteOutcome, GcApplyProgress, GcPlanApplier, ManagedObjectDeleter};
pub use gc_plan::{GcPlan, GcPlanWriter};
pub use inventory::{InventoryPage, InventoryRequest, ManagedObjectInventory};
pub use local::LocalObjectStore;
pub use reference_index::{LiveReferenceIndex, LiveReferenceIndexBuild, LiveReferenceIndexBuilder};
mod s3;
pub use s3::S3ObjectStore;

/// Errors converting managed-object values at the Arrow boundary.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ObjectError {
    #[snafu(display("DataLocation column '{column}' is missing or has an unexpected type"))]
    InvalidDataLocation { column: &'static str },

    #[snafu(display("DataLocation column '{column}' is null at row {row}"))]
    NullDataLocation { column: &'static str, row: usize },

    #[snafu(display("object storage I/O failed while {action} {path:?}"))]
    Io {
        action: String,
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("managed object path {path:?} cannot form a file URI"))]
    FileUri { path: PathBuf },

    #[snafu(display("DataLocation URI '{uri}' is not a valid local file URI"))]
    InvalidLocalUri { uri: String },

    #[snafu(display("DataLocation path {path:?} escapes managed object root {root:?}"))]
    OutsideManagedPrefix { path: PathBuf, root: PathBuf },

    #[snafu(display("managed S3 stage requires a non-empty bucket and prefix"))]
    InvalidS3Stage,

    #[snafu(display("DataLocation URI '{uri}' is not a valid s3:// URI"))]
    InvalidS3Uri { uri: String },

    #[snafu(display(
        "DataLocation URI '{uri}' is outside managed S3 prefix s3://{bucket}/{prefix}/"
    ))]
    OutsideManagedS3Prefix {
        uri:    String,
        bucket: String,
        prefix: String,
    },

    #[snafu(display("S3 operation '{action}' failed: {message}"))]
    S3 {
        action:  &'static str,
        message: String,
    },

    #[snafu(display("reading the object source failed"))]
    Read { source: std::io::Error },

    #[snafu(display("upload checkpoint I/O failed while {action} {path:?}"))]
    CheckpointIo {
        action: &'static str,
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("upload checkpoint {path:?} is invalid"))]
    InvalidCheckpoint {
        path:   PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("upload checkpoint does not match {field}"))]
    CheckpointMismatch { field: &'static str },

    #[snafu(display("upload checkpoint {path:?} is already in use"))]
    CheckpointInUse { path: PathBuf },

    #[snafu(display("this managed object store does not support resumable uploads"))]
    ResumeUnsupported,

    #[snafu(display("object GC cannot plan while retained reference lineage is incomplete"))]
    GcLineageIncomplete,

    #[snafu(display("invalid object GC configuration: {message}"))]
    InvalidGcConfig { message: String },

    #[snafu(display("GC candidate '{uri}' is outside managed stage '{stage}'"))]
    GcCandidateOutsideStage { uri: String, stage: String },

    #[snafu(display("object GC {input} input is not strictly URI-sorted"))]
    GcInputUnsorted { input: &'static str },

    #[snafu(display("object GC reference index I/O failed while {action} {path:?}"))]
    GcReferenceIndexIo {
        action: &'static str,
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("object GC reference index {path:?} is corrupt"))]
    GcReferenceIndexCorrupt {
        path:   PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("object URI '{uri}' has conflicting immutable identities"))]
    GcIdentityConflict { uri: String },

    #[snafu(display(
        "object GC refuses reference removals until retained-snapshot removal semantics exist"
    ))]
    GcReferenceRemovalsUnsupported,

    #[snafu(display("object GC plan I/O failed while {action} {path:?}"))]
    GcPlanIo {
        action: &'static str,
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("object GC plan document {path:?} is corrupt"))]
    GcPlanCorrupt {
        path:   PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("object GC plan does not match {field}"))]
    GcPlanMismatch { field: &'static str },

    #[snafu(display("object GC apply checkpoint I/O failed while {action} {path:?}"))]
    GcApplyCheckpointIo {
        action: &'static str,
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("object GC apply checkpoint {path:?} is corrupt"))]
    GcApplyCheckpointCorrupt {
        path:   PathBuf,
        source: serde_json::Error,
    },

    #[snafu(display("object GC apply checkpoint does not match {field}"))]
    GcApplyCheckpointMismatch { field: &'static str },

    #[snafu(display(
        "byte range {start}..{end} is invalid for DataLocation '{uri}' with size {size_bytes}"
    ))]
    InvalidRange {
        uri:        String,
        start:      u64,
        end:        u64,
        size_bytes: u64,
    },
}

/// The result type for managed-object operations.
pub type Result<T> = std::result::Result<T, ObjectError>;

/// A bounded-memory direct object stream returned by a managed stage.
pub type ObjectReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

/// Storage boundary used by the SDK for direct managed-object I/O.
#[async_trait]
pub trait ManagedObjectStore: Send + Sync {
    /// Stable, credential-free identity used to namespace local checkpoints.
    fn stage_identity(&self) -> String { "managed-stage".to_owned() }

    /// Upload one stream and return its stable immutable identity.
    async fn put_reader(&self, input: ObjectReader, content_type: String) -> Result<DataLocation>;

    /// Upload a seekable local path. Stores that support restart checkpoints
    /// override this method; the default keeps bounded streaming behavior.
    async fn put_path(
        &self,
        path: PathBuf,
        content_type: String,
        checkpoint: Option<PathBuf>,
    ) -> Result<DataLocation> {
        let _ = checkpoint;
        let input = tokio::fs::File::open(&path)
            .await
            .map_err(|source| ObjectError::Io {
                action: "opening".to_owned(),
                path,
                source,
            })?;
        self.put_reader(Box::pin(input), content_type).await
    }

    /// Explicitly abandon one resumable upload checkpoint.
    async fn cancel_upload(&self, _checkpoint: PathBuf) -> Result<()> {
        Err(ObjectError::ResumeUnsupported)
    }

    /// Open a direct reader after validating the location belongs to this
    /// managed stage.
    async fn open_reader(&self, location: &DataLocation) -> Result<ObjectReader>;

    /// Open exactly one non-empty half-open byte range.
    async fn open_range(&self, location: &DataLocation, range: Range<u64>) -> Result<ObjectReader>;
}

fn validate_range(location: &DataLocation, range: &Range<u64>) -> Result<u64> {
    let is_non_empty = range.start < range.end;
    let is_in_bounds = range.end <= location.size_bytes;
    if !is_non_empty || !is_in_bounds {
        return Err(ObjectError::InvalidRange {
            uri:        location.uri.clone(),
            start:      range.start,
            end:        range.end,
            size_bytes: location.size_bytes,
        });
    }
    Ok(range.end - range.start)
}

/// Arrow field encoding a logical SQL `FILE` table column as `DataLocation`.
#[must_use]
pub fn data_location_field(name: impl Into<String>, nullable: bool) -> Field {
    Field::new(name, DataType::Struct(data_location_fields()), nullable)
}

/// Encode locations as the Arrow struct stored in Lance tables.
#[must_use]
pub fn data_location_array(locations: &[DataLocation]) -> StructArray {
    let uri = StringArray::from_iter_values(locations.iter().map(|value| value.uri.as_str()));
    let content_type =
        StringArray::from_iter_values(locations.iter().map(|value| value.content_type.as_str()));
    let size_bytes = UInt64Array::from_iter_values(locations.iter().map(|value| value.size_bytes));
    let sha256 = StringArray::from_iter_values(locations.iter().map(|value| value.sha256.as_str()));
    StructArray::new(
        data_location_fields(),
        vec![
            Arc::new(uri) as ArrayRef,
            Arc::new(content_type),
            Arc::new(size_bytes),
            Arc::new(sha256),
        ],
        None,
    )
}

/// Decode one `DataLocation` row from its Arrow representation.
pub fn data_location_from_array(array: &StructArray, row: usize) -> Result<DataLocation> {
    let uri = string_value(array, "uri", row)?;
    let content_type = string_value(array, "content_type", row)?;
    let size_bytes = u64_value(array, "size_bytes", row)?;
    let sha256 = string_value(array, "sha256", row)?;
    Ok(DataLocation::builder()
        .uri(uri)
        .content_type(content_type)
        .size_bytes(size_bytes)
        .sha256(sha256)
        .build())
}

fn data_location_fields() -> Fields {
    Fields::from(vec![
        Field::new("uri", DataType::Utf8, false),
        Field::new("content_type", DataType::Utf8, false),
        Field::new("size_bytes", DataType::UInt64, false),
        Field::new("sha256", DataType::Utf8, false),
    ])
}

fn string_value(array: &StructArray, column: &'static str, row: usize) -> Result<String> {
    let values = array
        .column_by_name(column)
        .and_then(|values| values.as_any().downcast_ref::<StringArray>())
        .context(InvalidDataLocationSnafu { column })?;
    if values.is_null(row) {
        return Err(ObjectError::NullDataLocation { column, row });
    }
    Ok(values.value(row).to_owned())
}

fn u64_value(array: &StructArray, column: &'static str, row: usize) -> Result<u64> {
    let values = array
        .column_by_name(column)
        .and_then(|values| values.as_any().downcast_ref::<UInt64Array>())
        .context(InvalidDataLocationSnafu { column })?;
    if values.is_null(row) {
        return Err(ObjectError::NullDataLocation { column, row });
    }
    Ok(values.value(row))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::{Credentials, Region};
    use lake_common::DataLocation;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tokio::io::AsyncReadExt;

    use crate::{
        InventoryRequest, LocalObjectStore, ManagedObjectInventory, ManagedObjectStore,
        ObjectError, S3ObjectStore, data_location_array, data_location_from_array,
    };

    #[test]
    fn datalocation_arrow_roundtrip_preserves_identity() {
        let location = DataLocation::builder()
            .uri("file:///lake/objects/episode.mp4")
            .content_type("video/mp4")
            .size_bytes(4_294_967_296)
            .sha256("7f83b1657ff1fc53b92dc18148a1d65dfa135014")
            .build();

        let array = data_location_array(std::slice::from_ref(&location));

        assert_eq!(data_location_from_array(&array, 0).unwrap(), location);
    }

    #[tokio::test]
    async fn put_file_streams_bytes_and_returns_verified_location() {
        let source_dir = tempdir().unwrap();
        let source = source_dir.path().join("episode.mp4");
        let bytes = (0..(3 * 64 * 1024))
            .map(|index| u8::try_from(index % 251).unwrap())
            .collect::<Vec<_>>();
        tokio::fs::write(&source, &bytes).await.unwrap();

        let destination_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(destination_dir.path())
            .await
            .unwrap();
        let location = store.put_file(&source, "video/mp4").await.unwrap();

        assert_eq!(location.content_type, "video/mp4");
        assert_eq!(location.size_bytes, bytes.len() as u64);
        assert_eq!(location.sha256, format!("{:x}", Sha256::digest(&bytes)));
        assert!(location.uri.starts_with("file://"));
        let path = location.uri.strip_prefix("file://").unwrap();
        assert_eq!(tokio::fs::read(path).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn local_inventory_is_bounded_sorted_and_excludes_uploads() {
        let managed_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(managed_dir.path()).await.unwrap();
        for value in [b"third".as_slice(), b"first", b"second"] {
            store
                .put_reader(std::io::Cursor::new(value), "application/octet-stream")
                .await
                .unwrap();
        }
        tokio::fs::write(managed_dir.path().join(".stale.uploading"), b"partial")
            .await
            .unwrap();

        let first = store
            .inventory_page(InventoryRequest::try_new(None, 2).unwrap())
            .await
            .unwrap();
        assert_eq!(first.candidates().len(), 2);
        let second = store
            .inventory_page(
                InventoryRequest::try_new(first.next_cursor().map(ToOwned::to_owned), 2).unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(second.candidates().len(), 1);
        assert!(second.next_cursor().is_none());

        let candidates = first
            .candidates()
            .iter()
            .chain(second.candidates())
            .collect::<Vec<_>>();
        assert!(candidates.windows(2).all(|pair| pair[0].uri < pair[1].uri));
        assert!(
            candidates
                .iter()
                .all(|candidate| !candidate.uri.contains("uploading"))
        );
    }

    #[tokio::test]
    async fn local_range_reader_returns_exact_interval() {
        let managed_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(managed_dir.path()).await.unwrap();
        let location = store
            .put_reader(
                std::io::Cursor::new(b"0123456789"),
                "application/octet-stream",
            )
            .await
            .unwrap();

        let mut reader = store.open_range(&location, 2..7).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();

        assert_eq!(actual, b"23456");
    }

    #[tokio::test]
    async fn path_aware_managed_store_preserves_local_atomic_upload() {
        let source_dir = tempdir().unwrap();
        let source = source_dir.path().join("episode.mp4");
        tokio::fs::write(&source, b"path-backed upload")
            .await
            .unwrap();
        let managed_dir = tempdir().unwrap();
        let store: Arc<dyn ManagedObjectStore> =
            Arc::new(LocalObjectStore::open(managed_dir.path()).await.unwrap());
        let checkpoint = source_dir.path().join("episode.upload.json");

        let location = store
            .put_path(source, "video/mp4".to_owned(), Some(checkpoint.clone()))
            .await
            .unwrap();

        let mut reader = store.open_reader(&location).await.unwrap();
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        assert_eq!(bytes, b"path-backed upload");
        assert!(!checkpoint.exists());
        assert!(matches!(
            store.cancel_upload(checkpoint).await,
            Err(ObjectError::ResumeUnsupported)
        ));
    }

    #[tokio::test]
    async fn range_reader_rejects_empty_reversed_and_out_of_bounds_ranges() {
        let managed_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(managed_dir.path()).await.unwrap();
        let missing = DataLocation::builder()
            .uri(
                url::Url::from_file_path(managed_dir.path().join("missing"))
                    .unwrap()
                    .to_string(),
            )
            .content_type("application/octet-stream")
            .size_bytes(10)
            .sha256("unused")
            .build();

        let reversed_start = 8;
        let reversed_end = 4;
        for range in [0..0, reversed_start..reversed_end, 0..11] {
            assert!(matches!(
                store.open_range(&missing, range).await,
                Err(ObjectError::InvalidRange { .. })
            ));
        }
    }

    #[tokio::test]
    async fn local_reader_rejects_locations_outside_the_managed_prefix() {
        let outside_dir = tempdir().unwrap();
        let outside = outside_dir.path().join("secret.txt");
        tokio::fs::write(&outside, b"not a managed object")
            .await
            .unwrap();
        let managed_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(managed_dir.path()).await.unwrap();
        let location = DataLocation::builder()
            .uri(url::Url::from_file_path(outside).unwrap().to_string())
            .content_type("text/plain")
            .size_bytes(20)
            .sha256("unused")
            .build();

        assert!(matches!(
            store.open_reader(&location).await,
            Err(ObjectError::OutsideManagedPrefix { .. })
        ));
    }

    #[tokio::test]
    async fn s3_reader_rejects_locations_outside_managed_prefix() {
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url("http://127.0.0.1:1")
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "test"))
            .force_path_style(true)
            .build();
        let store = S3ObjectStore::new(
            aws_sdk_s3::Client::from_conf(config),
            "lake-managed",
            "objects",
        )
        .unwrap();

        for uri in [
            "s3://somebody-else/objects/object-id",
            "s3://lake-managed/objects-neighbor/object-id",
            "s3://lake-managed:9000/objects/object-id",
            "https://lake-managed.s3.amazonaws.com/objects/object-id",
        ] {
            let location = DataLocation::builder()
                .uri(uri)
                .content_type("video/mp4")
                .size_bytes(42)
                .sha256("unused")
                .build();

            assert!(matches!(
                store.open_reader(&location).await,
                Err(ObjectError::OutsideManagedS3Prefix { .. })
                    | Err(ObjectError::InvalidS3Uri { .. })
            ));
        }
    }
}
