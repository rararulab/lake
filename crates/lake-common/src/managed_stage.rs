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

//! Credential-free managed-stage discovery contract.

use std::path::Path;

use serde::{Deserialize, Serialize};
use snafu::Snafu;

use crate::TenantId;

/// Flight action used by an SDK to discover the query endpoint's managed stage.
pub const MANAGED_STAGE_DISCOVERY_ACTION: &str = "lake.managed_stage.v1";

/// Wire protocol version understood by this release.
pub const MANAGED_STAGE_PROTOCOL_VERSION: u16 = 1;

/// Errors encoding or validating a managed-stage discovery result.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum ManagedStageError {
    /// The descriptor is malformed JSON or violates its serde schema.
    #[snafu(display("managed-stage descriptor JSON is invalid"))]
    Json { source: serde_json::Error },

    /// A client must fail closed when backend semantics are newer than it
    /// understands.
    #[snafu(display(
        "managed-stage protocol version {version} is unsupported; this client supports {supported}"
    ))]
    UnsupportedVersion { version: u16, supported: u16 },
}

/// Backend identity and non-secret connection hints returned by query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum ManagedStageBackend {
    /// Development stage available on the SDK host's filesystem.
    Local {
        /// Absolute or process-resolved root containing managed objects.
        root: String,
    },
    /// Production stage in one Lake-owned S3 prefix.
    S3 {
        /// S3 bucket containing managed object keys.
        bucket:           String,
        /// Non-empty Lake-owned key prefix.
        prefix:           String,
        /// Optional fixed AWS region; otherwise the SDK credential/config
        /// chain resolves it.
        region:           Option<String>,
        /// Optional S3-compatible endpoint used for private clouds/emulators.
        endpoint:         Option<String>,
        /// Whether requests must use path-style bucket addressing.
        force_path_style: bool,
    },
}

/// Versioned, credential-free managed-stage descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedStageDescriptor {
    version: u16,
    backend: ManagedStageBackend,
}

impl ManagedStageDescriptor {
    /// Describe a local development managed stage.
    #[must_use]
    pub fn local(root: impl Into<String>) -> Self {
        Self {
            version: MANAGED_STAGE_PROTOCOL_VERSION,
            backend: ManagedStageBackend::Local { root: root.into() },
        }
    }

    /// Describe an S3 managed stage without returning credentials.
    #[must_use]
    pub fn s3(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        region: Option<String>,
        endpoint: Option<String>,
        force_path_style: bool,
    ) -> Self {
        Self {
            version: MANAGED_STAGE_PROTOCOL_VERSION,
            backend: ManagedStageBackend::S3 {
                bucket: bucket.into(),
                prefix: prefix.into(),
                region,
                endpoint,
                force_path_style,
            },
        }
    }

    /// Return the negotiated protocol version.
    #[must_use]
    pub fn version(&self) -> u16 { self.version }

    /// Return the backend configuration carried by this descriptor.
    #[must_use]
    pub fn backend(&self) -> &ManagedStageBackend { &self.backend }

    /// Derive the exact credential-free child stage owned by `tenant`.
    #[must_use]
    pub fn scope_to_tenant(&self, tenant: &TenantId) -> Self {
        let backend = match &self.backend {
            ManagedStageBackend::Local { root } => ManagedStageBackend::Local {
                root: Path::new(root)
                    .join("tenants")
                    .join(tenant.as_str())
                    .to_string_lossy()
                    .into_owned(),
            },
            ManagedStageBackend::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                force_path_style,
            } => {
                let prefix = prefix.trim_end_matches('/');
                let prefix = if prefix.is_empty() {
                    format!("tenants/{}", tenant.as_str())
                } else {
                    format!("{prefix}/tenants/{}", tenant.as_str())
                };
                ManagedStageBackend::S3 {
                    bucket: bucket.clone(),
                    prefix,
                    region: region.clone(),
                    endpoint: endpoint.clone(),
                    force_path_style: *force_path_style,
                }
            }
        };
        Self {
            version: self.version,
            backend,
        }
    }

    /// Encode a descriptor for a Flight result body.
    pub fn to_wire(&self) -> Result<Vec<u8>, ManagedStageError> {
        serde_json::to_vec(self).map_err(|source| ManagedStageError::Json { source })
    }

    /// Decode and version-check a Flight result body.
    pub fn from_wire(wire: &[u8]) -> Result<Self, ManagedStageError> {
        let descriptor: Self =
            serde_json::from_slice(wire).map_err(|source| ManagedStageError::Json { source })?;
        if descriptor.version != MANAGED_STAGE_PROTOCOL_VERSION {
            return Err(ManagedStageError::UnsupportedVersion {
                version:   descriptor.version,
                supported: MANAGED_STAGE_PROTOCOL_VERSION,
            });
        }
        Ok(descriptor)
    }
}
