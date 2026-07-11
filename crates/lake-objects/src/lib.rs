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

use std::{path::PathBuf, pin::Pin, sync::Arc};

use async_trait::async_trait;
use datafusion::arrow::{
    array::{Array, ArrayRef, StringArray, StructArray, UInt64Array},
    datatypes::{DataType, Field, Fields},
};
use lake_common::DataLocation;
use snafu::{OptionExt, Snafu};
use tokio::io::AsyncRead;

mod local;
pub use local::LocalObjectStore;

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

    #[snafu(display("reading the object source failed"))]
    Read { source: std::io::Error },
}

/// The result type for managed-object operations.
pub type Result<T> = std::result::Result<T, ObjectError>;

/// A bounded-memory direct object stream returned by a managed stage.
pub type ObjectReader = Pin<Box<dyn AsyncRead + Send + Unpin>>;

/// Storage boundary used by the SDK for direct managed-object I/O.
#[async_trait]
pub trait ManagedObjectStore: Send + Sync {
    /// Upload one stream and return its stable immutable identity.
    async fn put_reader(&self, input: ObjectReader, content_type: String) -> Result<DataLocation>;

    /// Open a direct reader after validating the location belongs to this
    /// managed stage.
    async fn open_reader(&self, location: &DataLocation) -> Result<ObjectReader>;
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
    use lake_common::DataLocation;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;

    use crate::{LocalObjectStore, ObjectError, data_location_array, data_location_from_array};

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
}
