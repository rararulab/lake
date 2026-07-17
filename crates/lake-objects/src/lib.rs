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

use std::{
    fmt,
    ops::Range,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use datafusion::arrow::{
    array::{Array, ArrayRef, StringArray, StructArray, UInt64Array},
    datatypes::{DataType, Field, Fields},
};
use lake_common::{DataLocation, ManagedStageDescriptor};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, Snafu};
use tokio::io::AsyncRead;

mod checkpoint;
mod gc;
mod gc_apply;
mod gc_plan;
mod integrity;
mod inventory;
mod local;
mod reference_index;
pub use gc::{GcPlanPage, GcPlanner, ObjectCandidate};
pub use gc_apply::{DeleteOutcome, GcApplyProgress, GcPlanApplier, ManagedObjectDeleter};
pub use gc_plan::{GcPlan, GcPlanWriter};
pub use integrity::{
    ObjectIntegrityError, open_exact_range, open_verified, validate_integrity, verify_reader,
};
pub use inventory::{InventoryPage, InventoryRequest, ManagedObjectInventory};
pub use local::LocalObjectStore;
pub use reference_index::{LiveReferenceIndex, LiveReferenceIndexBuild, LiveReferenceIndexBuilder};
mod s3;
pub use s3::{S3ObjectStore, S3ReadCapabilityIssuer};

/// Flight action used to request a server-issued managed-object GET capability.
pub const MANAGED_READ_CAPABILITY_ACTION: &str = "lake.managed_read_capability.v1";

/// Wire protocol version for managed-read capability actions.
pub const MANAGED_READ_CAPABILITY_PROTOCOL_VERSION: u16 = 1;

const MAX_MANAGED_READ_CAPABILITY_WIRE_BYTES: usize = 16 * 1024;

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

    #[snafu(display("S3 upload concurrency {value} is outside the supported range 1..={maximum}"))]
    InvalidS3UploadConcurrency { value: usize, maximum: usize },

    #[snafu(display(
        "S3 multipart part number {part_number} is outside the supported range 1..={maximum}"
    ))]
    S3MultipartPartLimit { part_number: i32, maximum: i32 },

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

    #[snafu(display("S3 Range GET response metadata does not match the requested interval"))]
    InvalidS3RangeResponse,

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

    #[snafu(display("this managed object store does not support presigned reads"))]
    PresignUnsupported,

    #[snafu(display("direct managed-object access is unavailable on this SDK client"))]
    DirectObjectAccessUnavailable,

    #[snafu(display("presigned read expiration {expires_in:?} is outside 1s..=1h"))]
    InvalidPresignExpiration { expires_in: Duration },

    #[snafu(display("managed object store returned an invalid presigned capability lifetime"))]
    InvalidPresignedCapabilityLifetime,

    #[snafu(display("managed-read capability wire is invalid"))]
    ManagedReadCapabilityWire { source: serde_json::Error },

    #[snafu(display(
        "managed-read capability wire has {actual} bytes, exceeding the {maximum}-byte limit"
    ))]
    ManagedReadCapabilityWireTooLarge { actual: usize, maximum: usize },

    #[snafu(display(
        "managed-read capability protocol version {version} is unsupported; this release supports \
         {supported}"
    ))]
    UnsupportedManagedReadCapabilityVersion { version: u16, supported: u16 },

    #[snafu(display("managed-read capability has an invalid expiration timestamp"))]
    InvalidManagedReadCapabilityExpiration,

    #[snafu(display("managed object scope is invalid"))]
    InvalidManagedObjectScope,

    #[snafu(display("this managed object store does not support scoped writes"))]
    ScopedWriteUnsupported,

    #[snafu(display("this managed object store does not support scoped deletion"))]
    ScopedDeleteUnsupported,

    #[snafu(display("managed object scope contains too many objects to delete safely"))]
    ScopedDeleteTooLarge,

    #[snafu(display("DataLocation has an invalid integrity identity"))]
    Integrity { source: ObjectIntegrityError },

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

/// Capability signer owned by a Query server, never by its SDK clients.
///
/// Query passes the exact stage already scoped to the authenticated tenant.
/// Implementations must reject a location outside that stage in
/// [`Self::validate`] before [`Self::issue`] can construct a signed URL.
#[async_trait]
pub trait ManagedReadCapabilityIssuer: Send + Sync {
    /// Reject an object identity outside the supplied tenant-scoped stage.
    fn validate(&self, stage: &ManagedStageDescriptor, location: &DataLocation) -> Result<()>;

    /// Mint one short-lived GET capability after successful validation.
    async fn issue(
        &self,
        stage: &ManagedStageDescriptor,
        location: &DataLocation,
        expires_in: Duration,
    ) -> Result<PresignedRead>;
}

/// Shared dynamic signer configured only in an S3 Query deployment.
pub type ManagedReadCapabilityIssuerRef = Arc<dyn ManagedReadCapabilityIssuer>;

/// One bounded request for a Query-issued managed-object GET capability.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedReadCapabilityRequest {
    location:   DataLocation,
    expires_in: Duration,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManagedReadCapabilityRequestWire {
    version:       u16,
    location:      DataLocation,
    expires_in_ms: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ManagedReadCapabilityResponseWire {
    version:            u16,
    url:                String,
    headers:            Vec<(String, String)>,
    expires_at_unix_ms: u64,
}

impl ManagedReadCapabilityRequest {
    /// Validate one immutable object identity and requested capability
    /// lifetime.
    pub fn try_new(location: DataLocation, expires_in: Duration) -> Result<Self> {
        validate_presign_expiration(expires_in)?;
        Ok(Self {
            location,
            expires_in,
        })
    }

    /// Return the immutable object identity to authorize and sign.
    #[must_use]
    pub fn location(&self) -> &DataLocation { &self.location }

    /// Return the caller-selected bounded capability lifetime.
    #[must_use]
    pub const fn expires_in(&self) -> Duration { self.expires_in }

    /// Encode this request for one Flight action body.
    pub fn to_wire(&self) -> Result<Vec<u8>> {
        let wire = serde_json::to_vec(&ManagedReadCapabilityRequestWire {
            version:       MANAGED_READ_CAPABILITY_PROTOCOL_VERSION,
            location:      self.location.clone(),
            expires_in_ms: self.expires_in.as_millis() as u64,
        })
        .map_err(|source| ObjectError::ManagedReadCapabilityWire { source })?;
        validate_managed_read_capability_wire_len(wire.len())?;
        Ok(wire)
    }

    /// Decode, size-bound, version-check, and validate one action body.
    pub fn from_wire(wire: &[u8]) -> Result<Self> {
        validate_managed_read_capability_wire_len(wire.len())?;
        let request: ManagedReadCapabilityRequestWire = serde_json::from_slice(wire)
            .map_err(|source| ObjectError::ManagedReadCapabilityWire { source })?;
        if request.version != MANAGED_READ_CAPABILITY_PROTOCOL_VERSION {
            return Err(ObjectError::UnsupportedManagedReadCapabilityVersion {
                version:   request.version,
                supported: MANAGED_READ_CAPABILITY_PROTOCOL_VERSION,
            });
        }
        Self::try_new(
            request.location,
            Duration::from_millis(request.expires_in_ms),
        )
    }
}

fn validate_managed_read_capability_wire_len(actual: usize) -> Result<()> {
    if actual > MAX_MANAGED_READ_CAPABILITY_WIRE_BYTES {
        return Err(ObjectError::ManagedReadCapabilityWireTooLarge {
            actual,
            maximum: MAX_MANAGED_READ_CAPABILITY_WIRE_BYTES,
        });
    }
    Ok(())
}

/// One opaque response to a managed-read capability action.
///
/// The contained URL and headers are bearer credentials. Its [`Debug`] output
/// deliberately delegates to [`PresignedRead`] so neither value is exposed.
pub struct ManagedReadCapabilityResponse {
    capability: PresignedRead,
}

impl ManagedReadCapabilityResponse {
    /// Wrap a capability created by an authorized Query issuer.
    #[must_use]
    pub fn new(capability: PresignedRead) -> Self { Self { capability } }

    /// Encode this response for one Flight result body.
    pub fn to_wire(&self) -> Result<Vec<u8>> {
        let expires_at_unix_ms = self
            .capability
            .expires_at()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| ObjectError::InvalidManagedReadCapabilityExpiration)?
            .as_millis() as u64;
        let wire = serde_json::to_vec(&ManagedReadCapabilityResponseWire {
            version: MANAGED_READ_CAPABILITY_PROTOCOL_VERSION,
            url: self.capability.url.clone(),
            headers: self.capability.headers.clone(),
            expires_at_unix_ms,
        })
        .map_err(|source| ObjectError::ManagedReadCapabilityWire { source })?;
        validate_managed_read_capability_wire_len(wire.len())?;
        Ok(wire)
    }

    /// Decode, size-bound, and version-check one action response body.
    pub fn from_wire(wire: &[u8]) -> Result<Self> {
        validate_managed_read_capability_wire_len(wire.len())?;
        let response: ManagedReadCapabilityResponseWire = serde_json::from_slice(wire)
            .map_err(|source| ObjectError::ManagedReadCapabilityWire { source })?;
        if response.version != MANAGED_READ_CAPABILITY_PROTOCOL_VERSION {
            return Err(ObjectError::UnsupportedManagedReadCapabilityVersion {
                version:   response.version,
                supported: MANAGED_READ_CAPABILITY_PROTOCOL_VERSION,
            });
        }
        let expires_at = UNIX_EPOCH
            .checked_add(Duration::from_millis(response.expires_at_unix_ms))
            .ok_or(ObjectError::InvalidManagedReadCapabilityExpiration)?;
        Ok(Self::new(PresignedRead::new(
            response.url,
            response.headers,
            expires_at,
        )))
    }

    /// Consume the response and reveal the opaque capability explicitly.
    #[must_use]
    pub fn into_capability(self) -> PresignedRead { self.capability }
}

impl fmt::Debug for ManagedReadCapabilityResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ManagedReadCapabilityResponse")
            .field("capability", &self.capability)
            .finish()
    }
}

/// A short-lived HTTP GET capability for one immutable managed object.
///
/// The URL and required header values are credentials. `Debug` deliberately
/// redacts them; callers must explicitly access or consume the capability.
pub struct PresignedRead {
    url:        String,
    headers:    Vec<(String, String)>,
    expires_at: SystemTime,
}

impl PresignedRead {
    /// Construct a capability returned by a custom managed object store.
    ///
    /// The store must not make `expires_at` later than the requested lifetime;
    /// SDK callers revalidate this boundary for embedding stores.
    #[must_use]
    pub fn new(
        url: impl Into<String>,
        headers: Vec<(String, String)>,
        expires_at: SystemTime,
    ) -> Self {
        Self {
            url: url.into(),
            headers,
            expires_at,
        }
    }

    /// Explicitly reveal the sensitive capability URL.
    #[must_use]
    pub fn url(&self) -> &str { &self.url }

    /// Required HTTP headers, excluding `Host`.
    #[must_use]
    pub fn headers(&self) -> &[(String, String)] { &self.headers }

    /// Wall-clock time after which this capability must be treated as expired.
    #[must_use]
    pub fn expires_at(&self) -> SystemTime { self.expires_at }

    /// Consume the capability and reveal its URL and required headers.
    #[must_use]
    pub fn into_parts(self) -> (String, Vec<(String, String)>, SystemTime) {
        (self.url, self.headers, self.expires_at)
    }
}

impl fmt::Debug for PresignedRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PresignedRead")
            .field("url", &"<redacted>")
            .field(
                "headers",
                &format_args!("<{} redacted>", self.headers.len()),
            )
            .field("expires_at", &self.expires_at)
            .finish()
    }
}

/// Storage boundary used by the SDK for direct managed-object I/O.
#[async_trait]
pub trait ManagedObjectStore: Send + Sync {
    /// Stable, credential-free identity used to namespace local checkpoints.
    fn stage_identity(&self) -> String { "managed-stage".to_owned() }

    /// Upload one stream and return its stable immutable identity.
    async fn put_reader(&self, input: ObjectReader, content_type: String) -> Result<DataLocation>;

    /// Upload an immutable object below an exact tenant/query/class prefix.
    /// Async result stores override this; the default fails closed rather
    /// than flattening data into an unscoped stage.
    async fn put_scoped_reader(
        &self,
        _scope: &ManagedObjectScope,
        _class: &str,
        _input: ObjectReader,
        _content_type: String,
    ) -> Result<DataLocation> {
        Err(ObjectError::ScopedWriteUnsupported)
    }

    /// Delete every service-owned object below an exact tenant/query scope.
    async fn delete_scope(&self, _scope: &ManagedObjectScope) -> Result<()> {
        Err(ObjectError::ScopedDeleteUnsupported)
    }

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

    /// Mint one short-lived HTTP GET capability after validating stage scope.
    /// Implementations must honor `expires_in`; callers may reject an expired
    /// result or one whose remaining lifetime exceeds the request.
    async fn presign_read(
        &self,
        _location: &DataLocation,
        expires_in: Duration,
    ) -> Result<PresignedRead> {
        validate_presign_expiration(expires_in)?;
        Err(ObjectError::PresignUnsupported)
    }
}

/// Validated hierarchy for service-owned async query objects.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ManagedObjectScope {
    tenant: String,
    query:  String,
}

impl ManagedObjectScope {
    pub fn try_new(tenant: impl Into<String>, query: impl Into<String>) -> Result<Self> {
        let tenant = tenant.into();
        let query = query.into();
        if !valid_scope_segment(&tenant, 64) || !valid_scope_segment(&query, 64) {
            return Err(ObjectError::InvalidManagedObjectScope);
        }
        Ok(Self { tenant, query })
    }

    fn relative_prefix(&self, class: &str) -> Result<String> {
        if !valid_scope_segment(class, 32) {
            return Err(ObjectError::InvalidManagedObjectScope);
        }
        Ok(format!("{}/{}/{class}", self.tenant, self.query))
    }

    pub(crate) fn relative_scope_prefix(&self) -> String {
        format!("{}/{}", self.tenant, self.query)
    }
}

fn valid_scope_segment(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        && value != "."
        && value != ".."
}

/// Validate Lake's bounded lifetime policy for delegated read capabilities.
pub fn validate_presign_expiration(expires_in: Duration) -> Result<()> {
    if expires_in < Duration::from_secs(1) || expires_in > Duration::from_hours(1) {
        return Err(ObjectError::InvalidPresignExpiration { expires_in });
    }
    Ok(())
}

/// Validate one non-empty half-open byte range and return its exact length.
pub fn validate_range(location: &DataLocation, range: &Range<u64>) -> Result<u64> {
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
    use std::{
        ffi::OsString,
        io,
        path::Path,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::{Duration, SystemTime},
    };

    use async_trait::async_trait;
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::{Credentials, Region};
    use lake_common::DataLocation;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, ReadBuf},
        sync::oneshot,
    };

    use crate::{
        InventoryRequest, LocalObjectStore, ManagedObjectInventory, ManagedObjectScope,
        ManagedObjectStore, ManagedReadCapabilityRequest, ManagedReadCapabilityResponse,
        ObjectError, ObjectIntegrityError, ObjectReader, PresignedRead, Result as ObjectResult,
        S3ObjectStore, data_location_array, data_location_from_array, open_verified,
    };

    struct StaticReadStore {
        bytes: Vec<u8>,
        opens: Arc<AtomicUsize>,
    }

    /// Returns one copy chunk, then signals that the upload is blocked before
    /// it can observe EOF.
    struct BlockedLocalUploadReader {
        emitted: bool,
        blocked: Option<oneshot::Sender<()>>,
    }

    impl AsyncRead for BlockedLocalUploadReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let reader = self.get_mut();
            if !reader.emitted {
                reader.emitted = true;
                output.put_slice(b"partial local upload");
                return Poll::Ready(Ok(()));
            }
            if let Some(blocked) = reader.blocked.take() {
                let _ = blocked.send(());
            }
            Poll::Pending
        }
    }

    /// Returns one copy chunk, then injects a source I/O failure.
    struct FailingLocalUploadReader {
        emitted: bool,
    }

    impl AsyncRead for FailingLocalUploadReader {
        fn poll_read(
            self: Pin<&mut Self>,
            _context: &mut Context<'_>,
            output: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            let reader = self.get_mut();
            if !reader.emitted {
                reader.emitted = true;
                output.put_slice(b"partial local upload");
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Err(io::Error::other("injected source failure")))
        }
    }

    async fn stage_entries(root: &Path) -> Vec<OsString> {
        let mut entries = tokio::fs::read_dir(root).await.expect("read managed stage");
        let mut names = Vec::new();
        while let Some(entry) = entries.next_entry().await.expect("read stage entry") {
            names.push(entry.file_name());
        }
        names
    }

    #[async_trait]
    impl ManagedObjectStore for StaticReadStore {
        async fn put_reader(
            &self,
            _input: ObjectReader,
            _content_type: String,
        ) -> ObjectResult<DataLocation> {
            panic!("verification tests must not upload")
        }

        async fn open_reader(&self, _location: &DataLocation) -> ObjectResult<ObjectReader> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(std::io::Cursor::new(self.bytes.clone())))
        }

        async fn open_range(
            &self,
            _location: &DataLocation,
            _range: std::ops::Range<u64>,
        ) -> ObjectResult<ObjectReader> {
            panic!("verification tests must not range-read")
        }
    }

    fn integrity_location(expected: &[u8]) -> DataLocation {
        DataLocation::builder()
            .uri("s3://managed/objects/test")
            .content_type("application/octet-stream")
            .size_bytes(expected.len() as u64)
            .sha256(format!("{:x}", Sha256::digest(expected)))
            .build()
    }

    fn terminal_integrity_error(error: &std::io::Error) -> &ObjectIntegrityError {
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        error
            .get_ref()
            .and_then(|source| source.downcast_ref::<ObjectIntegrityError>())
            .expect("typed object integrity error")
    }

    fn test_s3_store() -> S3ObjectStore {
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url("http://127.0.0.1:1")
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new(
                "test-key",
                "test-secret",
                None,
                None,
                "test",
            ))
            .force_path_style(true)
            .build();
        S3ObjectStore::new(
            aws_sdk_s3::Client::from_conf(config),
            "lake-managed",
            "tenants/tenant-a/objects",
        )
        .unwrap()
    }

    fn s3_location(uri: &str) -> DataLocation {
        DataLocation::builder()
            .uri(uri)
            .content_type("video/mp4")
            .size_bytes(42)
            .sha256("unused")
            .build()
    }

    #[tokio::test]
    async fn verified_reader_accepts_exact_identity_while_streaming() {
        for expected in [b"streamed video model bytes".as_slice(), b"".as_slice()] {
            let opens = Arc::new(AtomicUsize::new(0));
            let store = StaticReadStore {
                bytes: expected.to_vec(),
                opens: opens.clone(),
            };
            let location = integrity_location(expected);
            let mut reader = open_verified(&store, &location).await.unwrap();
            let mut actual = Vec::new();
            let mut chunk = [0_u8; 3];
            loop {
                let read = reader.read(&mut chunk).await.unwrap();
                if read == 0 {
                    break;
                }
                actual.extend_from_slice(&chunk[..read]);
            }

            assert_eq!(actual, expected);
            assert_eq!(opens.load(Ordering::SeqCst), 1);
        }
    }

    #[tokio::test]
    async fn verified_reader_rejects_invalid_short_long_and_hash_mismatch() {
        let opens = Arc::new(AtomicUsize::new(0));
        let invalid_store = StaticReadStore {
            bytes: b"unreachable".to_vec(),
            opens: opens.clone(),
        };
        let invalid = DataLocation::builder()
            .uri("s3://managed/objects/invalid")
            .content_type("application/octet-stream")
            .size_bytes(11)
            .sha256("not-a-sha256")
            .build();
        assert!(matches!(
            open_verified(&invalid_store, &invalid).await,
            Err(ObjectError::Integrity {
                source: ObjectIntegrityError::InvalidSha256,
            })
        ));
        assert_eq!(opens.load(Ordering::SeqCst), 0);

        let cases = [
            (b"abc".as_slice(), integrity_location(b"abcd"), "short"),
            (b"abcd".as_slice(), integrity_location(b"abc"), "long"),
            (b"abc".as_slice(), integrity_location(b"xyz"), "hash"),
        ];
        for (bytes, location, expected_error) in cases {
            let store = StaticReadStore {
                bytes: bytes.to_vec(),
                opens: Arc::new(AtomicUsize::new(0)),
            };
            let mut reader = open_verified(&store, &location).await.unwrap();
            let mut actual = Vec::new();
            let error = reader.read_to_end(&mut actual).await.unwrap_err();
            match (expected_error, terminal_integrity_error(&error)) {
                ("short", ObjectIntegrityError::PrematureEof { expected, actual }) => {
                    assert_eq!((*expected, *actual), (4, 3));
                }
                ("long", ObjectIntegrityError::SizeExceeded { expected }) => {
                    assert_eq!(*expected, 3);
                    assert_eq!(actual, b"abc");
                }
                ("hash", ObjectIntegrityError::Sha256Mismatch { .. }) => {
                    assert_eq!(actual, b"abc");
                }
                (name, error) => panic!("unexpected {name} integrity error: {error:?}"),
            }
        }
    }

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
        assert!(
            stage_entries(destination_dir.path())
                .await
                .iter()
                .all(|name| !name.to_string_lossy().ends_with(".uploading"))
        );
    }

    #[tokio::test]
    async fn cancelled_local_upload_removes_unpublished_staging_file() {
        let destination_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(destination_dir.path())
            .await
            .unwrap();
        let (blocked, entered_blocked_read) = oneshot::channel();
        let upload = tokio::spawn(async move {
            store
                .put_reader(
                    BlockedLocalUploadReader {
                        emitted: false,
                        blocked: Some(blocked),
                    },
                    "video/mp4",
                )
                .await
        });

        entered_blocked_read
            .await
            .expect("reader blocks after staging receives bytes");
        assert!(
            stage_entries(destination_dir.path())
                .await
                .iter()
                .any(|name| name.to_string_lossy().ends_with(".uploading"))
        );

        upload.abort();
        assert!(
            upload
                .await
                .expect_err("upload task is cancelled")
                .is_cancelled()
        );

        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if stage_entries(destination_dir.path()).await.is_empty() {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled upload cleanup removes staging");
    }

    #[tokio::test]
    async fn local_upload_source_error_removes_unpublished_staging_file() {
        let destination_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(destination_dir.path())
            .await
            .unwrap();
        let error = store
            .put_reader(FailingLocalUploadReader { emitted: false }, "video/mp4")
            .await
            .expect_err("injected source failure must fail the upload");

        assert!(matches!(error, ObjectError::Read { .. }));
        assert!(stage_entries(destination_dir.path()).await.is_empty());
    }

    #[tokio::test]
    async fn async_result_store_scopes_objects_by_tenant_and_query() {
        let destination_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(destination_dir.path())
            .await
            .unwrap();
        let scope = ManagedObjectScope::try_new("tenant-a", "0198f73b-12b0-7d20-b8ab-8195ce8bfe73")
            .expect("safe tenant/query scope");

        let location = ManagedObjectStore::put_scoped_reader(
            &store,
            &scope,
            "job",
            Box::pin(std::io::Cursor::new(b"encrypted-job".to_vec())),
            "application/vnd.lake.async-job".to_owned(),
        )
        .await
        .expect("scoped immutable object");

        assert!(location.uri.contains("/tenant-a/"));
        assert!(
            location
                .uri
                .contains("/0198f73b-12b0-7d20-b8ab-8195ce8bfe73/job/")
        );
        assert!(matches!(
            ManagedObjectScope::try_new("../escape", "query"),
            Err(ObjectError::InvalidManagedObjectScope)
        ));
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
    async fn local_range_reader_rejects_truncated_object() {
        let managed_dir = tempdir().unwrap();
        let store = LocalObjectStore::open(managed_dir.path()).await.unwrap();
        let location = store
            .put_reader(
                std::io::Cursor::new(b"0123456789"),
                "application/octet-stream",
            )
            .await
            .unwrap();
        let path = url::Url::parse(&location.uri)
            .expect("local DataLocation URI")
            .to_file_path()
            .expect("local DataLocation file path");
        tokio::fs::OpenOptions::new()
            .write(true)
            .open(path)
            .await
            .expect("open managed object for truncation")
            .set_len(7)
            .await
            .expect("truncate managed object");

        let mut reader = store.open_range(&location, 4..10).await.unwrap();
        let mut actual = Vec::new();
        let error = reader.read_to_end(&mut actual).await.unwrap_err();

        assert_eq!(actual, b"456");
        assert!(matches!(
            terminal_integrity_error(&error),
            ObjectIntegrityError::PrematureEof {
                expected: 6,
                actual:   3,
            }
        ));
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

    #[tokio::test]
    async fn s3_presigned_read_is_scoped_bounded_and_redacted() {
        let store = test_s3_store();
        let location = s3_location("s3://lake-managed/tenants/tenant-a/objects/0197f8b8-object");
        let before = SystemTime::now();

        let capability = store
            .presign_read(&location, Duration::from_mins(1))
            .await
            .unwrap();

        let url = url::Url::parse(capability.url()).unwrap();
        assert_eq!(url.scheme(), "http");
        assert_eq!(url.host_str(), Some("127.0.0.1"));
        assert!(
            url.path()
                .ends_with("/lake-managed/tenants/tenant-a/objects/0197f8b8-object")
        );
        assert!(
            url.query_pairs().any(|(name, value)| {
                name.eq_ignore_ascii_case("X-Amz-Expires") && value == "60"
            })
        );
        let signed_headers = url
            .query_pairs()
            .find(|(name, _)| name.eq_ignore_ascii_case("X-Amz-SignedHeaders"))
            .map(|(_, value)| value.into_owned())
            .expect("SigV4 signed headers");
        assert!(!signed_headers.to_ascii_lowercase().contains("range"));
        assert!(
            capability
                .headers()
                .iter()
                .all(|(name, _)| !name.eq_ignore_ascii_case("range"))
        );
        assert!(capability.expires_at() >= before + Duration::from_mins(1));
        assert!(capability.expires_at() <= SystemTime::now() + Duration::from_mins(1));
        let debug = format!("{capability:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("X-Amz-Signature"));
        assert!(!debug.contains("test-key"));
    }

    #[tokio::test]
    async fn presigned_read_rejects_escape_and_invalid_expiration() {
        let store = test_s3_store();
        let valid = s3_location("s3://lake-managed/tenants/tenant-a/objects/object");
        for expires_in in [
            Duration::ZERO,
            Duration::from_millis(999),
            Duration::from_secs(3_601),
        ] {
            assert!(matches!(
                store.presign_read(&valid, expires_in).await,
                Err(ObjectError::InvalidPresignExpiration { .. })
            ));
        }
        for uri in [
            "s3://somebody-else/tenants/tenant-a/objects/object",
            "s3://lake-managed/tenants/tenant-b/objects/object",
            "s3://lake-managed/tenants/tenant-a/objects/object?versionId=secret",
        ] {
            assert!(matches!(
                store
                    .presign_read(&s3_location(uri), Duration::from_mins(1))
                    .await,
                Err(ObjectError::OutsideManagedS3Prefix { .. })
                    | Err(ObjectError::InvalidS3Uri { .. })
            ));
        }

        let credential_uri = concat!(
            "s3://user:userinfo-secret@lake-managed/tenants/tenant-a/objects/object?",
            "X-Amz-Signature=signature-secret&X-Amz-Security-Token=token-secret#fragment-secret",
        );
        let error = store
            .presign_read(&s3_location(credential_uri), Duration::from_mins(1))
            .await
            .unwrap_err();
        for formatted in [format!("{error}"), format!("{error:?}")] {
            for secret in [
                "userinfo-secret",
                "signature-secret",
                "token-secret",
                "fragment-secret",
            ] {
                assert!(!formatted.contains(secret));
            }
        }

        let local = LocalObjectStore::open(tempdir().unwrap().path())
            .await
            .unwrap();
        assert!(matches!(
            local.presign_read(&valid, Duration::from_mins(1)).await,
            Err(ObjectError::PresignUnsupported)
        ));
    }

    #[test]
    fn managed_read_capability_request_roundtrips_with_bounded_expiration() {
        let location = s3_location("s3://lake-managed/tenants/tenant-a/objects/episode");
        let request =
            ManagedReadCapabilityRequest::try_new(location.clone(), Duration::from_mins(1))
                .expect("bounded request");

        let decoded =
            ManagedReadCapabilityRequest::from_wire(&request.to_wire().expect("encode request"))
                .expect("decode request");

        assert_eq!(decoded.location(), &location);
        assert_eq!(decoded.expires_in(), Duration::from_mins(1));
        assert!(matches!(
            ManagedReadCapabilityRequest::try_new(location, Duration::ZERO),
            Err(ObjectError::InvalidPresignExpiration { .. })
        ));
    }

    #[test]
    fn managed_read_capability_response_roundtrips_without_debug_secret_leak() {
        let secret_url = "https://objects.example/episode?X-Amz-Signature=secret-signature";
        let response = ManagedReadCapabilityResponse::new(PresignedRead::new(
            secret_url,
            vec![("x-amz-security-token".to_owned(), "secret-token".to_owned())],
            SystemTime::now() + Duration::from_mins(1),
        ));

        let decoded =
            ManagedReadCapabilityResponse::from_wire(&response.to_wire().expect("encode response"))
                .expect("decode response")
                .into_capability();

        assert_eq!(decoded.url(), secret_url);
        assert_eq!(decoded.headers()[0].1, "secret-token");
        let debug = format!("{response:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("secret-signature"));
        assert!(!debug.contains("secret-token"));
    }
}
