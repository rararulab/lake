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

//! Canonical per-version managed-object reference deltas.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::{DataLocation, Version};

const FORMAT_VERSION: u16 = 1;

/// Immutable identity recorded by an engine reference journal.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ObjectIdentity {
    pub uri:          String,
    pub content_type: String,
    pub size_bytes:   u64,
    pub sha256:       String,
}

impl From<DataLocation> for ObjectIdentity {
    fn from(value: DataLocation) -> Self {
        Self {
            uri:          value.uri,
            content_type: value.content_type,
            size_bytes:   value.size_bytes,
            sha256:       value.sha256,
        }
    }
}

/// One canonical parent→child reference change in a table's version lineage.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectReferenceDelta {
    format_version: u16,
    parent_version: Version,
    table_version:  Version,
    added:          Vec<ObjectIdentity>,
    removed:        Vec<ObjectIdentity>,
}

#[derive(Debug, Snafu)]
pub enum ObjectReferenceError {
    #[snafu(display("object reference delta JSON is corrupt"))]
    Corrupt { source: serde_json::Error },

    #[snafu(display(
        "object reference format version {version} is unsupported; expected {supported}"
    ))]
    UnsupportedVersion { version: u16, supported: u16 },

    #[snafu(display("object reference version edge {parent}->{child} is not monotonic"))]
    InvalidVersionEdge { parent: Version, child: Version },

    #[snafu(display("object identity '{uri}' is both added and removed"))]
    ConflictingIdentity { uri: String },

    #[snafu(display("object reference delta is not canonically ordered"))]
    NonCanonical,
}

impl ObjectReferenceDelta {
    pub fn try_new(
        parent_version: Version,
        table_version: Version,
        added: Vec<ObjectIdentity>,
        removed: Vec<ObjectIdentity>,
    ) -> Result<Self, ObjectReferenceError> {
        if parent_version >= table_version {
            return Err(ObjectReferenceError::InvalidVersionEdge {
                parent: parent_version,
                child:  table_version,
            });
        }
        let added = added.into_iter().collect::<BTreeSet<_>>();
        let removed = removed.into_iter().collect::<BTreeSet<_>>();
        if let Some(identity) = added.intersection(&removed).next() {
            return Err(ObjectReferenceError::ConflictingIdentity {
                uri: identity.uri.clone(),
            });
        }
        Ok(Self {
            format_version: FORMAT_VERSION,
            parent_version,
            table_version,
            added: added.into_iter().collect(),
            removed: removed.into_iter().collect(),
        })
    }

    pub fn parent_version(&self) -> Version { self.parent_version }

    pub fn table_version(&self) -> Version { self.table_version }

    pub fn added(&self) -> &[ObjectIdentity] { &self.added }

    pub fn removed(&self) -> &[ObjectIdentity] { &self.removed }

    pub fn encode(&self) -> Result<Vec<u8>, ObjectReferenceError> {
        serde_json::to_vec(self).map_err(|source| ObjectReferenceError::Corrupt { source })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ObjectReferenceError> {
        let wire: Self = serde_json::from_slice(bytes)
            .map_err(|source| ObjectReferenceError::Corrupt { source })?;
        if wire.format_version != FORMAT_VERSION {
            return Err(ObjectReferenceError::UnsupportedVersion {
                version:   wire.format_version,
                supported: FORMAT_VERSION,
            });
        }
        let canonical = Self::try_new(
            wire.parent_version,
            wire.table_version,
            wire.added.clone(),
            wire.removed.clone(),
        )?;
        if canonical != wire {
            return Err(ObjectReferenceError::NonCanonical);
        }
        Ok(wire)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::Value;

    use super::*;

    fn object(uri: &str, sha256: &str) -> ObjectIdentity {
        ObjectIdentity {
            uri:          uri.to_owned(),
            content_type: "video/mp4".to_owned(),
            size_bytes:   42,
            sha256:       sha256.to_owned(),
        }
    }

    #[test]
    fn object_reference_delta_roundtrips_canonically() {
        let first = object("s3://lake/objects/a", "aa");
        let second = object("s3://lake/objects/b", "bb");
        let delta = ObjectReferenceDelta::try_new(
            Version(7),
            Version(8),
            vec![second.clone(), first.clone(), second],
            Vec::new(),
        )
        .expect("valid version edge");

        assert_eq!(
            delta.added(),
            &[first.clone(), object("s3://lake/objects/b", "bb")]
        );
        assert!(delta.removed().is_empty());
        let encoded = delta.encode().expect("encode canonical delta");
        assert_eq!(ObjectReferenceDelta::decode(&encoded).unwrap(), delta);

        assert!(matches!(
            ObjectReferenceDelta::try_new(Version(8), Version(8), vec![first.clone()], Vec::new(),),
            Err(ObjectReferenceError::InvalidVersionEdge { .. })
        ));
        assert!(matches!(
            ObjectReferenceDelta::try_new(Version(8), Version(9), vec![first.clone()], vec![first],),
            Err(ObjectReferenceError::ConflictingIdentity { .. })
        ));

        let mut future: Value = serde_json::from_slice(&encoded).unwrap();
        future["format_version"] = Value::from(99);
        assert!(matches!(
            ObjectReferenceDelta::decode(&serde_json::to_vec(&future).unwrap()),
            Err(ObjectReferenceError::UnsupportedVersion {
                version:   99,
                supported: 1,
            })
        ));
        assert!(matches!(
            ObjectReferenceDelta::decode(b"not-json"),
            Err(ObjectReferenceError::Corrupt { .. })
        ));
    }
}
