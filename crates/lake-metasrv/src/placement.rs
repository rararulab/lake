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

//! Server-authoritative table dataset placement.

use std::path::PathBuf;

use lake_common::{TableLocation, TableRef};
use snafu::Snafu;

const MAX_STORAGE_SEGMENT_BYTES: usize = 255;
const DATASET_SUFFIX: &str = ".lance";
const MAX_TABLE_NAME_BYTES: usize = MAX_STORAGE_SEGMENT_BYTES - DATASET_SUFFIX.len();

/// A table location derivation failure detected before storage is mutated.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum PlacementError {
    /// A namespace or table name is not a safe single storage path segment.
    #[snafu(display("invalid {component}: {reason}"))]
    InvalidIdentifier {
        component: &'static str,
        reason:    &'static str,
    },

    /// The trusted S3 placement configuration is malformed.
    #[snafu(display("invalid S3 table placement: {reason}"))]
    InvalidS3Config { reason: &'static str },

    /// The trusted local root cannot be represented by `TableLocation`.
    #[snafu(display("table location below local root {root:?} is not valid UTF-8"))]
    NonUtf8LocalLocation { root: PathBuf },
}

/// Trusted policy that deterministically places table datasets.
///
/// The metadata service constructs this value from process configuration and
/// never from a remote DDL request. [`Self::place`] validates both table
/// identifier components as single path segments before deriving a location.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TablePlacement {
    backend: PlacementBackend,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum PlacementBackend {
    /// Place datasets below a local filesystem root.
    Local { root: PathBuf },
    /// Place datasets below an S3 bucket and optional key prefix.
    S3 { bucket: String, prefix: String },
}

impl TablePlacement {
    /// Construct a trusted local-filesystem placement policy.
    #[must_use]
    pub fn local(root: PathBuf) -> Self {
        Self {
            backend: PlacementBackend::Local { root },
        }
    }

    /// Construct a trusted S3 placement policy.
    ///
    /// `prefix` may be empty. A non-empty prefix must consist of safe,
    /// slash-separated key segments and must not start or end with `/`.
    pub fn s3(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, PlacementError> {
        let bucket = bucket.into();
        let prefix = prefix.into();
        validate_bucket(&bucket)?;
        validate_prefix(&prefix)?;
        Ok(Self {
            backend: PlacementBackend::S3 { bucket, prefix },
        })
    }

    /// Derive the only dataset location allowed for `table` by this policy.
    ///
    /// Validation completes before a [`TableLocation`] is returned, so callers
    /// can reject unsafe DDL before invoking an engine or registry operation.
    pub fn place(&self, table: &TableRef) -> Result<TableLocation, PlacementError> {
        validate_identifier("namespace", &table.namespace.0, MAX_STORAGE_SEGMENT_BYTES)?;
        validate_identifier("table name", &table.name.0, MAX_TABLE_NAME_BYTES)?;
        let dataset = format!("{}{DATASET_SUFFIX}", table.name.0);

        match &self.backend {
            PlacementBackend::Local { root } => {
                let location = root.join(&table.namespace.0).join(dataset);
                location
                    .to_str()
                    .map(TableLocation::new)
                    .ok_or_else(|| PlacementError::NonUtf8LocalLocation { root: root.clone() })
            }
            PlacementBackend::S3 { bucket, prefix } => {
                let prefix = if prefix.is_empty() {
                    String::new()
                } else {
                    format!("{prefix}/")
                };
                Ok(TableLocation::new(format!(
                    "s3://{bucket}/{prefix}{}/{dataset}",
                    table.namespace.0
                )))
            }
        }
    }
}

fn validate_identifier(
    component: &'static str,
    value: &str,
    max_bytes: usize,
) -> Result<(), PlacementError> {
    let reason = if value.is_empty() {
        Some("must not be empty")
    } else if value.len() > max_bytes {
        Some(if max_bytes == MAX_TABLE_NAME_BYTES {
            "must not exceed 249 UTF-8 bytes before the .lance suffix"
        } else {
            "must not exceed 255 UTF-8 bytes"
        })
    } else if matches!(value, "." | "..") {
        Some("must not be a dot segment")
    } else if value.contains(['/', '\\']) {
        Some("must be one path segment")
    } else if value.contains(['?', '#', '%']) {
        Some("must not contain URI delimiters or escapes")
    } else if value.chars().any(char::is_control) {
        Some("must not contain control characters")
    } else {
        None
    };

    reason.map_or(Ok(()), |reason| {
        Err(PlacementError::InvalidIdentifier { component, reason })
    })
}

fn validate_bucket(bucket: &str) -> Result<(), PlacementError> {
    let valid_len = (3..=63).contains(&bucket.len());
    let valid_chars = bucket.bytes().all(|byte| {
        byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'.' | b'-')
    });
    let valid_edges = bucket
        .as_bytes()
        .first()
        .zip(bucket.as_bytes().last())
        .is_some_and(|(first, last)| first.is_ascii_alphanumeric() && last.is_ascii_alphanumeric());
    let is_ipv4_address = bucket.parse::<std::net::Ipv4Addr>().is_ok();
    if valid_len && valid_chars && valid_edges && !bucket.contains("..") && !is_ipv4_address {
        Ok(())
    } else {
        Err(PlacementError::InvalidS3Config {
            reason: "bucket must be a valid lowercase S3 bucket name",
        })
    }
}

fn validate_prefix(prefix: &str) -> Result<(), PlacementError> {
    if prefix.is_empty() {
        return Ok(());
    }
    let valid = !prefix.starts_with('/')
        && !prefix.ends_with('/')
        && prefix.split('/').all(|segment| {
            validate_identifier("S3 prefix segment", segment, MAX_STORAGE_SEGMENT_BYTES).is_ok()
        });
    if valid {
        Ok(())
    } else {
        Err(PlacementError::InvalidS3Config {
            reason: "prefix must contain safe non-empty path segments",
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use lake_common::TableRef;

    use super::TablePlacement;

    #[test]
    fn table_placement_derives_managed_locations() {
        let table = TableRef::new("robots", "episodes");
        let local = TablePlacement::local(PathBuf::from("/srv/lake/tables"));
        let s3 = TablePlacement::s3("lake-prod", "datasets").expect("valid S3 placement");

        assert_eq!(
            local.place(&table).expect("valid local placement").as_str(),
            "/srv/lake/tables/robots/episodes.lance"
        );
        assert_eq!(
            s3.place(&table).expect("valid S3 placement").as_str(),
            "s3://lake-prod/datasets/robots/episodes.lance"
        );

        let boundary = TableRef::new("robots", "x".repeat(249));
        let boundary = local
            .place(&boundary)
            .expect("249-byte table name leaves room for .lance");
        assert_eq!(
            std::path::Path::new(boundary.as_str())
                .file_name()
                .expect("dataset filename")
                .as_encoded_bytes()
                .len(),
            255
        );
    }

    #[test]
    fn table_placement_rejects_unsafe_identifiers() {
        let placement = TablePlacement::local(PathBuf::from("/srv/lake/tables"));
        let overlong = "x".repeat(256);
        let invalid = [
            TableRef::new("", "episodes"),
            TableRef::new(".", "episodes"),
            TableRef::new("..", "episodes"),
            TableRef::new("../escape", "episodes"),
            TableRef::new("robots/escape", "episodes"),
            TableRef::new(r"robots\escape", "episodes"),
            TableRef::new("robots\u{0}hidden", "episodes"),
            TableRef::new("robots", ""),
            TableRef::new("robots", "."),
            TableRef::new("robots", ".."),
            TableRef::new("robots", "../escape"),
            TableRef::new("robots", "episode/video"),
            TableRef::new("robots", r"episode\video"),
            TableRef::new("robots", "episode\nvideo"),
            TableRef::new("robots?shadow", "episodes"),
            TableRef::new("robots", "episodes#shadow"),
            TableRef::new("robots", "episodes%2fescape"),
            TableRef::new(overlong, "episodes"),
            TableRef::new("robots", "x".repeat(250)),
            TableRef::new("robots", "x".repeat(256)),
        ];

        for table in invalid {
            assert!(
                placement.place(&table).is_err(),
                "unsafe table identifier was accepted: {table:?}"
            );
        }
    }

    #[test]
    fn table_placement_rejects_unsafe_storage_config() {
        for bucket in ["", "UPPERCASE", "192.168.1.1", "-leading", "trailing-"] {
            assert!(
                TablePlacement::s3(bucket, "datasets").is_err(),
                "invalid S3 bucket was accepted: {bucket:?}"
            );
        }
        for prefix in ["/datasets", "datasets/", "datasets//episodes", "../escape"] {
            assert!(
                TablePlacement::s3("lake-prod", prefix).is_err(),
                "invalid S3 prefix was accepted: {prefix:?}"
            );
        }
    }
}
