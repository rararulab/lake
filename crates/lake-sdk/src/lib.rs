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

//! Rust SDK for parameterized SQL inserts containing managed `FILE` values.

mod append_checkpoint;

use std::{
    collections::BTreeMap,
    fmt,
    ops::Range,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{ArrayRef, StringArray, StructArray},
    datatypes::{DataType, Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use arrow_flight::{
    Action, CancelFlightInfoRequest, CancelStatus, FlightClient, FlightData, FlightDescriptor,
    FlightInfo, PollInfo, PutResult, Ticket,
    encode::FlightDataEncoderBuilder,
    error::FlightError,
    sql::{CommandStatementQuery, ProstMessageExt, client::FlightSqlServiceClient},
};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use futures::{Stream, StreamExt, TryStreamExt};
use lake_common::{
    AppendOperationId, DataLocation, FILE_APPEND_TYPE_URL, FileAppendRequest,
    MANAGED_STAGE_DISCOVERY_ACTION, ManagedStageBackend, ManagedStageDescriptor, TableRef, Version,
};
use lake_flight::{ClientSecurity, append_flight_payload_digest};
use lake_objects::{
    LocalObjectStore, ManagedObjectStore, ObjectReader, S3ObjectStore, data_location_array,
    data_location_field, data_location_from_array, open_verified, validate_presign_expiration,
};
pub use lake_objects::{ObjectIntegrityError, PresignedRead};
use moka::{future::Cache, ops::compute::Op};
use prost::Message;
use prost_types::Any;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use snafu::{OptionExt, ResultExt, Snafu};
use tokio::{io::AsyncRead, sync::Mutex};
use tonic::transport::Channel;

const APPEND_RETRY_WINDOW: std::time::Duration = std::time::Duration::from_secs(30);
const APPEND_RETRY_INITIAL_BACKOFF: std::time::Duration = std::time::Duration::from_millis(100);
const APPEND_RETRY_MAX_BACKOFF: std::time::Duration = std::time::Duration::from_secs(1);
const DEFAULT_SCHEMA_CACHE_CAPACITY: u64 = 1_024;
const DEFAULT_SCHEMA_CACHE_TTL: Duration = Duration::from_mins(1);
const MAX_SCHEMA_CACHE_CAPACITY: u64 = 65_536;
const MAX_SCHEMA_CACHE_TTL: Duration = Duration::from_hours(1);
const MAX_INSERT_BATCH_ROWS: usize = 10_000;
const MAX_INSERT_INPUT_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_INSERT_OUTPUT_METADATA_BYTES: usize = 16 * 1024 * 1024;
const MAX_INSERT_FLIGHT_BYTES: usize = 64 * 1024 * 1024;
const MAX_PENDING_APPEND_CHECKPOINTS: usize = 1_024;
const ASYNC_QUERY_HANDLE_VERSION: u8 = 1;
const MAX_ASYNC_QUERY_HANDLE_BYTES: usize = 16 * 1024;
const MAX_ASYNC_RESULT_ENDPOINTS: usize = 4_096;
const MAX_QUERY_RESULT_ENDPOINTS: usize = 256;
const MAX_QUERY_RESULT_TICKET_BYTES: usize = 512 * 1024;
const MAX_QUERY_RESULT_TICKET_TOTAL_BYTES: usize = 8 * 1024 * 1024;
const MAX_QUERY_RESULT_FLIGHT_INFO_BYTES: usize = MAX_QUERY_RESULT_TICKET_TOTAL_BYTES + 1024 * 1024;
const FLIGHT_REUSE_CONNECTION_LOCATION: &str = "arrow-flight-reuse-connection://?";
const ASYNC_SUBMIT_RETRY_WINDOW: Duration = Duration::from_secs(30);
const ASYNC_POLL_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const ASYNC_POLL_MAX_BACKOFF: Duration = Duration::from_secs(2);

fn ambiguous_append_error(error: &SdkError) -> bool {
    match error {
        SdkError::MissingAppendResult => true,
        SdkError::Flight {
            source: arrow_flight::error::FlightError::Tonic(status),
        } => matches!(
            status.code(),
            tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Internal
                | tonic::Code::Unavailable
        ),
        // Non-status Flight failures describe the response transport or
        // decoding path. The request may already have committed, so none of
        // them proves a server-side rejection.
        SdkError::Flight { .. } => true,
        _ => false,
    }
}

fn ambiguous_async_submission_error(error: &FlightError) -> bool {
    match error {
        FlightError::Tonic(status) => matches!(
            status.code(),
            tonic::Code::Cancelled
                | tonic::Code::Unknown
                | tonic::Code::DeadlineExceeded
                | tonic::Code::Internal
                | tonic::Code::Unavailable
        ),
        _ => true,
    }
}

#[derive(Debug)]
enum AppendRetryFailure {
    Sdk(SdkError),
    Expired,
}

async fn retry_ambiguous_append_with_window<F, Fut>(
    mut attempt: F,
    window: std::time::Duration,
) -> std::result::Result<PutResult, AppendRetryFailure>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<PutResult>>,
{
    tokio::time::timeout(window, async {
        let mut backoff = APPEND_RETRY_INITIAL_BACKOFF;
        loop {
            match attempt().await {
                Ok(result) => return Ok(result),
                Err(error) if ambiguous_append_error(&error) => {
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2).min(APPEND_RETRY_MAX_BACKOFF);
                }
                Err(error) => return Err(AppendRetryFailure::Sdk(error)),
            }
        }
    })
    .await
    .map_err(|_| AppendRetryFailure::Expired)?
}

async fn resume_pending_with<F, Fut>(
    pending: PendingAppend,
    window: std::time::Duration,
    mut attempt: F,
) -> Result<Version>
where
    F: FnMut(Vec<FlightData>) -> Fut,
    Fut: std::future::Future<Output = Result<PutResult>>,
{
    let result =
        retry_ambiguous_append_with_window(|| attempt(pending.messages.clone()), window).await;
    let result = match result {
        Ok(result) => result,
        Err(AppendRetryFailure::Sdk(error)) => {
            if append_checkpoint::remove(pending.checkpoint.as_deref())
                .await
                .is_err()
            {
                tracing::warn!(
                    operation_id = %pending.operation_id,
                    "conclusive append rejection could not remove its durable checkpoint"
                );
            }
            return Err(error);
        }
        Err(AppendRetryFailure::Expired) => {
            return Err(SdkError::AppendRetryExpired { window, pending });
        }
    };
    let version = serde_json::from_slice(&result.app_metadata).map_err(|source| {
        SdkError::AppendResultUncertain {
            pending: pending.clone(),
            source,
        }
    })?;
    if append_checkpoint::remove(pending.checkpoint.as_deref())
        .await
        .is_err()
    {
        tracing::warn!(
            operation_id = %pending.operation_id,
            "committed append could not remove its replay-safe durable checkpoint"
        );
    }
    Ok(version)
}

/// Errors raised by the typed Rust SDK.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum SdkError {
    #[snafu(display("unsupported INSERT SQL: {message}"))]
    InvalidSql { message: String },

    #[snafu(display("invalid SDK schema cache configuration: {message}"))]
    InvalidSchemaCacheConfig { message: String },

    #[snafu(display("durable append checkpoints are not configured"))]
    AppendCheckpointingDisabled,

    #[snafu(display("append checkpoint I/O failed while {action} {path:?}"))]
    AppendCheckpointIo {
        action: &'static str,
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display(
        "append checkpoint was published at {path:?}, but its directory sync failed; recover the \
         returned pending append instead of preparing a new operation"
    ))]
    AppendCheckpointPublishUncertain {
        path:    PathBuf,
        pending: PendingAppend,
        source:  std::io::Error,
    },

    #[snafu(display("append checkpoint {path:?} is invalid: {message}"))]
    InvalidAppendCheckpoint { path: PathBuf, message: String },

    #[snafu(display(
        "append checkpoint {path:?} is {actual} bytes, above the {maximum}-byte limit"
    ))]
    AppendCheckpointTooLarge {
        path:    PathBuf,
        actual:  u64,
        maximum: u64,
    },

    #[snafu(display(
        "append checkpoint directory contains more than {maximum} pending operations"
    ))]
    TooManyPendingAppendCheckpoints { maximum: usize },

    #[snafu(display(
        "ambiguous FILE append did not converge within {window:?}; resume the returned pending \
         append"
    ))]
    AppendRetryExpired {
        window:  std::time::Duration,
        pending: PendingAppend,
    },

    #[snafu(display("INSERT binds {actual} values but SQL declares {expected} placeholders"))]
    ParameterCount { expected: usize, actual: usize },

    #[snafu(display("INSERT batch row count {actual} is outside 1..={maximum}"))]
    BatchRowCount { actual: usize, maximum: usize },

    #[snafu(display("INSERT batch metadata is {actual} bytes, above the {maximum}-byte limit"))]
    BatchMetadataSize { actual: usize, maximum: usize },

    #[snafu(display("INSERT column '{column}' is missing from table schema"))]
    UnknownColumn { column: String },

    #[snafu(display("INSERT must bind every table column; '{column}' is missing"))]
    MissingColumn { column: String },

    #[snafu(display("INSERT value for '{column}' does not match its table type"))]
    TypeMismatch { column: String },

    #[snafu(display("table '{table}' not found"))]
    NotFound { table: String },

    #[snafu(display("query connection failed"))]
    Connect { source: tonic::transport::Error },

    #[snafu(display("invalid query endpoint: {message}"))]
    InvalidEndpoint { message: String },

    #[snafu(display("query Flight operation failed"))]
    Flight {
        source: arrow_flight::error::FlightError,
    },

    #[snafu(display("query returned no managed FILE stage descriptor"))]
    MissingManagedStage,

    #[snafu(display("query returned more than one managed FILE stage descriptor"))]
    MultipleManagedStages,

    #[snafu(display("query returned an invalid managed FILE stage descriptor"))]
    InvalidManagedStage {
        source: lake_common::ManagedStageError,
    },

    #[snafu(display("query returned no FILE append result"))]
    MissingAppendResult,

    #[snafu(display("query returned no Flight endpoint"))]
    MissingQueryEndpoint,

    #[snafu(display("query returned a Flight endpoint without a ticket"))]
    MissingQueryTicket,

    #[snafu(display("query returned invalid Flight endpoint metadata"))]
    InvalidQueryResultEndpoint,

    #[snafu(display("query returned an unsupported Flight endpoint location"))]
    UnsupportedQueryResultLocation,

    #[snafu(display("asynchronous query did not complete within {timeout:?}"))]
    AsyncQueryTimeout { timeout: Duration },

    #[snafu(display("asynchronous query handle is invalid"))]
    InvalidAsyncQueryHandle,

    #[snafu(display("asynchronous query capability expired at Unix second {expires_at}"))]
    AsyncQueryHandleExpired { expires_at: u64 },

    #[snafu(display("asynchronous query handle JSON is invalid"))]
    AsyncQueryHandleJson { source: serde_json::Error },

    #[snafu(display("asynchronous query result capability is invalid"))]
    InvalidAsyncQueryResult,

    #[snafu(display("query result column '{column}' is missing"))]
    MissingResultColumn { column: String },

    #[snafu(display("query result column '{column}' is not a FILE value"))]
    InvalidFileColumn { column: String },

    #[snafu(display("query result row {row} is outside the batch of {rows} rows"))]
    RowOutOfBounds { row: usize, rows: usize },

    #[snafu(display("query returned an invalid FILE append version"))]
    InvalidAppendResult { source: serde_json::Error },

    #[snafu(display(
        "FILE append may have committed, but its result metadata was invalid; resume the returned \
         pending append"
    ))]
    AppendResultUncertain {
        pending: PendingAppend,
        source:  serde_json::Error,
    },

    #[snafu(display("managed object operation failed"))]
    Object { source: lake_objects::ObjectError },

    #[snafu(display("could not open FILE upload source {path:?}"))]
    SourceFile {
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("could not build INSERT record batch"))]
    Arrow { source: ArrowError },

    #[snafu(display("invalid query Flight security configuration"))]
    Security {
        source: lake_flight::FlightSecurityError,
    },
}

/// The result type for Rust SDK operations.
pub type Result<T> = std::result::Result<T, SdkError>;

/// A prepared append whose uploaded objects and operation identity are stable.
///
/// When an ambiguous append exhausts the automatic transport retry window,
/// [`SdkError::AppendRetryExpired`] returns this value. Passing it to
/// [`LakeClient::resume_append`] continues the same logical append without
/// uploading objects again or allocating a new idempotency identity.
#[derive(Clone, Debug)]
pub struct PendingAppend {
    operation_id: AppendOperationId,
    messages:     Vec<FlightData>,
    checkpoint:   Option<PathBuf>,
}

impl PendingAppend {
    /// Return the durable identity reused by every retry of this append.
    #[must_use]
    pub fn operation_id(&self) -> &AppendOperationId { &self.operation_id }
}

impl SdkError {
    /// Recover the pending append from an exhausted ambiguous retry window.
    #[must_use]
    pub fn into_pending_append(self) -> Option<PendingAppend> {
        match self {
            Self::AppendRetryExpired { pending, .. }
            | Self::AppendCheckpointPublishUncertain { pending, .. }
            | Self::AppendResultUncertain { pending, .. } => Some(pending),
            _ => None,
        }
    }
}

/// An SDK upload source bound to a SQL `FILE` value.
///
/// The SDK streams this source directly into a Lake-managed stage. It never
/// sends the file bytes through SQL, Flight, or the metadata service.
pub struct FileUpload {
    source:       ObjectSource,
    content_type: String,
}

enum ObjectSource {
    Path(PathBuf),
    Reader(Box<dyn AsyncRead + Send + Unpin>),
}

impl fmt::Debug for FileUpload {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match &self.source {
            ObjectSource::Path(path) => path.display().to_string(),
            ObjectSource::Reader(_) => "reader".to_owned(),
        };
        formatter
            .debug_struct("FileUpload")
            .field("source", &source)
            .field("content_type", &self.content_type)
            .finish()
    }
}

impl FileUpload {
    /// Bind `path` as a SQL `FILE` with the supplied IANA media type.
    #[must_use]
    pub fn from_path(path: impl AsRef<Path>, content_type: impl Into<String>) -> Self {
        Self {
            source:       ObjectSource::Path(path.as_ref().to_path_buf()),
            content_type: content_type.into(),
        }
    }

    /// Bind an async source that the SDK streams directly to the managed stage.
    #[must_use]
    pub fn from_reader<R>(reader: R, content_type: impl Into<String>) -> Self
    where
        R: AsyncRead + Send + Unpin + 'static,
    {
        Self {
            source:       ObjectSource::Reader(Box::new(reader)),
            content_type: content_type.into(),
        }
    }
}

/// A typed parameter accepted by the SDK's narrow INSERT binding.
#[derive(Debug)]
pub enum InsertValue {
    /// UTF-8 scalar value.
    Utf8(String),
    /// SQL `FILE` streamed directly to the Lake-managed stage.
    File(FileUpload),
}

#[derive(Clone, Copy, Debug)]
struct SchemaCacheConfig {
    capacity: u64,
    ttl:      Duration,
}

impl Default for SchemaCacheConfig {
    fn default() -> Self {
        Self {
            capacity: DEFAULT_SCHEMA_CACHE_CAPACITY,
            ttl:      DEFAULT_SCHEMA_CACHE_TTL,
        }
    }
}

impl SchemaCacheConfig {
    fn validate(capacity: u64, ttl: Duration) -> Result<Self> {
        if capacity == 0 || capacity > MAX_SCHEMA_CACHE_CAPACITY {
            return Err(SdkError::InvalidSchemaCacheConfig {
                message: format!("capacity must be in 1..={MAX_SCHEMA_CACHE_CAPACITY}"),
            });
        }
        if ttl.is_zero() || ttl > MAX_SCHEMA_CACHE_TTL {
            return Err(SdkError::InvalidSchemaCacheConfig {
                message: format!("TTL must be in 1ns..={MAX_SCHEMA_CACHE_TTL:?}"),
            });
        }
        Ok(Self { capacity, ttl })
    }
}

#[derive(Clone)]
struct SchemaCache {
    entries: Cache<TableRef, Arc<Mutex<Option<SchemaLoadResult>>>>,
}

type SchemaLoadResult = std::result::Result<SchemaRef, SchemaLoadError>;

#[derive(Clone, Debug)]
enum SchemaLoadError {
    Flight(FlightErrorSnapshot),
    Arrow(ArrowErrorSnapshot),
}

impl SchemaLoadError {
    fn into_sdk_error(self) -> SdkError {
        match self {
            Self::Flight(source) => SdkError::Flight {
                source: source.into_error(),
            },
            Self::Arrow(source) => SdkError::Arrow {
                source: source.into_error(),
            },
        }
    }
}

#[derive(Clone, Debug)]
enum FlightErrorSnapshot {
    Arrow(ArrowErrorSnapshot),
    NotYetImplemented(String),
    Tonic(tonic::Status),
    Protocol(String),
    Decode(String),
    External(String),
}

impl FlightErrorSnapshot {
    fn from_error(error: arrow_flight::error::FlightError) -> Self {
        use arrow_flight::error::FlightError;
        match error {
            FlightError::Arrow(source) => Self::Arrow(ArrowErrorSnapshot::from_error(source)),
            FlightError::NotYetImplemented(message) => Self::NotYetImplemented(message),
            FlightError::Tonic(status) => Self::Tonic(*status),
            FlightError::ProtocolError(message) => Self::Protocol(message),
            FlightError::DecodeError(message) => Self::Decode(message),
            FlightError::ExternalError(source) => Self::External(source.to_string()),
        }
    }

    fn into_error(self) -> arrow_flight::error::FlightError {
        use arrow_flight::error::FlightError;
        match self {
            Self::Arrow(source) => FlightError::Arrow(source.into_error()),
            Self::NotYetImplemented(message) => FlightError::NotYetImplemented(message),
            Self::Tonic(status) => FlightError::Tonic(Box::new(status)),
            Self::Protocol(message) => FlightError::ProtocolError(message),
            Self::Decode(message) => FlightError::DecodeError(message),
            Self::External(message) => {
                FlightError::ExternalError(Box::new(std::io::Error::other(message)))
            }
        }
    }
}

#[derive(Clone, Debug)]
enum ArrowErrorSnapshot {
    NotYetImplemented(String),
    External(String),
    Cast(String),
    Memory(String),
    Parse(String),
    Schema(String),
    Compute(String),
    DivideByZero,
    ArithmeticOverflow(String),
    Csv(String),
    Json(String),
    Avro(String),
    Io(String, std::io::ErrorKind, String),
    Ipc(String),
    InvalidArgument(String),
    Parquet(String),
    CDataInterface(String),
    DictionaryKeyOverflow,
    RunEndIndexOverflow,
    OffsetOverflow(usize),
}

impl ArrowErrorSnapshot {
    fn from_error(error: ArrowError) -> Self {
        match error {
            ArrowError::NotYetImplemented(message) => Self::NotYetImplemented(message),
            ArrowError::ExternalError(source) => Self::External(source.to_string()),
            ArrowError::CastError(message) => Self::Cast(message),
            ArrowError::MemoryError(message) => Self::Memory(message),
            ArrowError::ParseError(message) => Self::Parse(message),
            ArrowError::SchemaError(message) => Self::Schema(message),
            ArrowError::ComputeError(message) => Self::Compute(message),
            ArrowError::DivideByZero => Self::DivideByZero,
            ArrowError::ArithmeticOverflow(message) => Self::ArithmeticOverflow(message),
            ArrowError::CsvError(message) => Self::Csv(message),
            ArrowError::JsonError(message) => Self::Json(message),
            ArrowError::AvroError(message) => Self::Avro(message),
            ArrowError::IoError(message, source) => {
                Self::Io(message, source.kind(), source.to_string())
            }
            ArrowError::IpcError(message) => Self::Ipc(message),
            ArrowError::InvalidArgumentError(message) => Self::InvalidArgument(message),
            ArrowError::ParquetError(message) => Self::Parquet(message),
            ArrowError::CDataInterface(message) => Self::CDataInterface(message),
            ArrowError::DictionaryKeyOverflowError => Self::DictionaryKeyOverflow,
            ArrowError::RunEndIndexOverflowError => Self::RunEndIndexOverflow,
            ArrowError::OffsetOverflowError(offset) => Self::OffsetOverflow(offset),
        }
    }

    fn into_error(self) -> ArrowError {
        match self {
            Self::NotYetImplemented(message) => ArrowError::NotYetImplemented(message),
            Self::External(message) => {
                ArrowError::ExternalError(Box::new(std::io::Error::other(message)))
            }
            Self::Cast(message) => ArrowError::CastError(message),
            Self::Memory(message) => ArrowError::MemoryError(message),
            Self::Parse(message) => ArrowError::ParseError(message),
            Self::Schema(message) => ArrowError::SchemaError(message),
            Self::Compute(message) => ArrowError::ComputeError(message),
            Self::DivideByZero => ArrowError::DivideByZero,
            Self::ArithmeticOverflow(message) => ArrowError::ArithmeticOverflow(message),
            Self::Csv(message) => ArrowError::CsvError(message),
            Self::Json(message) => ArrowError::JsonError(message),
            Self::Avro(message) => ArrowError::AvroError(message),
            Self::Io(message, kind, source) => {
                ArrowError::IoError(message, std::io::Error::new(kind, source))
            }
            Self::Ipc(message) => ArrowError::IpcError(message),
            Self::InvalidArgument(message) => ArrowError::InvalidArgumentError(message),
            Self::Parquet(message) => ArrowError::ParquetError(message),
            Self::CDataInterface(message) => ArrowError::CDataInterface(message),
            Self::DictionaryKeyOverflow => ArrowError::DictionaryKeyOverflowError,
            Self::RunEndIndexOverflow => ArrowError::RunEndIndexOverflowError,
            Self::OffsetOverflow(offset) => ArrowError::OffsetOverflowError(offset),
        }
    }
}

impl SchemaCache {
    fn new(config: SchemaCacheConfig) -> Self {
        Self {
            entries: Cache::builder()
                .max_capacity(config.capacity)
                .time_to_live(config.ttl)
                .build(),
        }
    }

    async fn resolve<F, Fut>(&self, table: TableRef, load: F) -> SchemaLoadResult
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = SchemaLoadResult>,
    {
        // The cell is published before the loader starts. Invalidating the key
        // therefore detaches an in-flight loader: it can finish for its caller,
        // but it cannot repopulate the cache after a table incarnation change.
        let cell = self
            .entries
            .get_with(table.clone(), async { Arc::new(Mutex::new(None)) })
            .await;
        let mut cached = cell.lock().await;
        if let Some(result) = cached.as_ref() {
            return result.clone();
        }
        let result = load().await;
        *cached = Some(result.clone());
        drop(cached);

        if result.is_err() {
            // Existing waiters retain this cell and observe the same typed
            // failure. Remove only this exact generation so a request arriving
            // after the cohort can retry immediately; never remove a newer cell
            // installed by explicit invalidation.
            let failed_cell = cell.clone();
            self.entries
                .entry(table)
                .and_compute_with(move |entry| {
                    let op = entry.map_or(Op::Nop, |entry| {
                        if Arc::ptr_eq(&entry.into_value(), &failed_cell) {
                            Op::Remove
                        } else {
                            Op::Nop
                        }
                    });
                    std::future::ready(op)
                })
                .await;
        }
        result
    }

    async fn invalidate(&self, table: &TableRef) { self.entries.invalidate(table).await; }

    fn clear(&self) { self.entries.invalidate_all(); }

    fn entry_count(&self) -> u64 { self.entries.entry_count() }
}

/// A Rust SDK client connected to the stateless query endpoint.
#[derive(Clone)]
pub struct LakeClient {
    query:                 Channel,
    objects:               Arc<dyn ManagedObjectStore>,
    security:              ClientSecurity,
    schema_cache:          SchemaCache,
    upload_checkpoint_dir: Option<PathBuf>,
}

/// Type-erased stream for the complete result of one synchronous SQL query.
///
/// Consume its [`Stream`] items with `futures::TryStreamExt`, such as
/// `try_next` or `try_collect`. The SDK redeems every validated local result
/// endpoint in declared order without collecting the complete result.
///
/// This deliberately replaces Arrow's `FlightRecordBatchStream` as the
/// synchronous query return type. Per-`DoGet` headers and trailers have no
/// well-defined whole-result meaning, so this stream exposes only
/// [`RecordBatch`] values and terminal [`FlightError`]s.
pub type QueryResultStream =
    Pin<Box<dyn Stream<Item = std::result::Result<RecordBatch, FlightError>> + Send>>;

/// Ordered Arrow batches materialized from a durable asynchronous-query result
/// manifest.
///
/// This alias and its asynchronous manifest API retain their existing behavior;
/// the synchronous [`QueryResultStream`] migration does not change them.
pub type AsyncQueryResultStream = QueryResultStream;

/// Result of one non-blocking durable query poll.
pub enum AsyncQueryPoll {
    /// Query remains queued or running; persist the refreshed capability.
    Pending(AsyncQueryHandle),
    /// Every immutable result part is published and ready for consumption.
    Complete(AsyncQueryResult),
}

impl fmt::Debug for AsyncQueryPoll {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending(handle) => formatter.debug_tuple("Pending").field(handle).finish(),
            Self::Complete(result) => formatter.debug_tuple("Complete").field(result).finish(),
        }
    }
}

/// Bounded exact Flight tickets for an already completed async query.
pub struct AsyncQueryResult {
    tickets: Vec<Ticket>,
}

impl AsyncQueryResult {
    /// Number of ordered immutable result parts.
    #[must_use]
    pub fn part_count(&self) -> usize { self.tickets.len() }
}

impl fmt::Debug for AsyncQueryResult {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AsyncQueryResult")
            .field("part_count", &self.tickets.len())
            .field("tickets", &"<redacted>")
            .finish()
    }
}

/// Persistable opaque capability for one durable asynchronous query.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AsyncQueryHandle {
    version:         u8,
    poll_descriptor: Vec<u8>,
    expires_at_secs: u64,
}

impl AsyncQueryHandle {
    fn try_new(poll_descriptor: Vec<u8>, expires_at_secs: u64) -> Result<Self> {
        let handle = Self {
            version: ASYNC_QUERY_HANDLE_VERSION,
            poll_descriptor,
            expires_at_secs,
        };
        handle.validate()?;
        Ok(handle)
    }

    /// Serialize this opaque capability for caller-owned durable storage.
    pub fn to_json(&self) -> Result<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|source| SdkError::AsyncQueryHandleJson { source })
    }

    /// Restore and fully validate a serialized capability.
    pub fn from_json(encoded: &[u8]) -> Result<Self> {
        if encoded.is_empty() || encoded.len() > MAX_ASYNC_QUERY_HANDLE_BYTES * 5 {
            return Err(SdkError::InvalidAsyncQueryHandle);
        }
        let handle: Self = serde_json::from_slice(encoded)
            .map_err(|source| SdkError::AsyncQueryHandleJson { source })?;
        handle.validate()?;
        Ok(handle)
    }

    /// Unix timestamp after which the current poll capability is invalid.
    #[must_use]
    pub const fn expires_at_unix_seconds(&self) -> u64 { self.expires_at_secs }

    fn validate(&self) -> Result<()> {
        if self.version != ASYNC_QUERY_HANDLE_VERSION
            || self.poll_descriptor.is_empty()
            || self.poll_descriptor.len() > MAX_ASYNC_QUERY_HANDLE_BYTES
            || self.expires_at_secs == 0
        {
            return Err(SdkError::InvalidAsyncQueryHandle);
        }
        Ok(())
    }
}

impl fmt::Debug for AsyncQueryHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AsyncQueryHandle")
            .field("version", &self.version)
            .field("poll_descriptor", &"<redacted>")
            .field("expires_at_secs", &self.expires_at_secs)
            .finish()
    }
}

fn async_poll_expiration(poll: &PollInfo) -> Result<u64> {
    let expiration = poll
        .expiration_time
        .as_ref()
        .ok_or(SdkError::InvalidAsyncQueryHandle)?;
    if expiration.seconds <= 0 || !(0..1_000_000_000).contains(&expiration.nanos) {
        return Err(SdkError::InvalidAsyncQueryHandle);
    }
    u64::try_from(expiration.seconds).map_err(|_| SdkError::InvalidAsyncQueryHandle)
}

fn async_handle_from_poll(poll: &PollInfo) -> Result<AsyncQueryHandle> {
    let descriptor = poll
        .flight_descriptor
        .as_ref()
        .ok_or(SdkError::InvalidAsyncQueryHandle)?;
    if descriptor.cmd.is_empty() || !descriptor.path.is_empty() {
        return Err(SdkError::InvalidAsyncQueryHandle);
    }
    AsyncQueryHandle::try_new(descriptor.cmd.to_vec(), async_poll_expiration(poll)?)
}

fn async_result_from_poll(poll: PollInfo) -> Result<AsyncQueryResult> {
    let info = poll.info.context(MissingQueryEndpointSnafu)?;
    Ok(AsyncQueryResult {
        tickets: LakeClient::async_result_tickets(info)?,
    })
}

/// Builder for authenticated and TLS-verified SDK connections.
#[derive(Clone, Debug)]
pub struct LakeClientBuilder {
    query_endpoint:        String,
    security:              ClientSecurity,
    schema_cache:          SchemaCacheConfig,
    upload_checkpoint_dir: Option<PathBuf>,
}

impl LakeClientBuilder {
    /// Configure the finite client-local table schema cache.
    pub fn with_schema_cache(mut self, capacity: u64, ttl: Duration) -> Result<Self> {
        self.schema_cache = SchemaCacheConfig::validate(capacity, ttl)?;
        Ok(self)
    }

    /// Persist resumable path-upload state and prepared append metadata in
    /// this local directory.
    #[must_use]
    pub fn with_upload_checkpoint_dir(mut self, directory: impl Into<PathBuf>) -> Self {
        self.upload_checkpoint_dir = Some(directory.into());
        self
    }

    /// Attach the bearer credential sent on every Query Flight RPC.
    pub fn with_bearer_token(mut self, value: impl Into<String>) -> Result<Self> {
        self.security = self
            .security
            .with_bearer_token(value)
            .context(SecuritySnafu)?;
        Ok(self)
    }

    /// Trust an additional PEM CA certificate for the Query endpoint.
    #[must_use]
    pub fn with_ca_certificate_pem(mut self, certificate: Vec<u8>) -> Self {
        self.security = self.security.with_ca_certificate_pem(certificate);
        self
    }

    /// Override the TLS certificate DNS name for internal service routing.
    #[must_use]
    pub fn with_server_name(mut self, server_name: impl Into<String>) -> Self {
        self.security = self.security.with_server_name(server_name);
        self
    }

    /// Require TLS using enabled public trust roots.
    #[must_use]
    pub fn with_tls(mut self) -> Self {
        self.security = self.security.with_tls();
        self
    }

    /// Connect and discover the managed `FILE` stage once.
    pub async fn connect(self) -> Result<LakeClient> {
        prepare_checkpoint_dir(self.upload_checkpoint_dir.as_deref()).await?;
        let query = self
            .security
            .connect(self.query_endpoint)
            .await
            .context(SecuritySnafu)?;
        let descriptor = discover_managed_stage(query.clone(), &self.security).await?;
        let objects = open_managed_stage(&descriptor).await?;
        Ok(LakeClient {
            query,
            objects,
            security: self.security,
            schema_cache: SchemaCache::new(self.schema_cache),
            upload_checkpoint_dir: self.upload_checkpoint_dir,
        })
    }

    /// Connect with an explicitly injected stage while retaining TLS/auth.
    pub async fn connect_with_store<S>(self, objects: S) -> Result<LakeClient>
    where
        S: ManagedObjectStore + 'static,
    {
        prepare_checkpoint_dir(self.upload_checkpoint_dir.as_deref()).await?;
        let query = self
            .security
            .connect(self.query_endpoint)
            .await
            .context(SecuritySnafu)?;
        Ok(LakeClient {
            query,
            objects: Arc::new(objects),
            security: self.security,
            schema_cache: SchemaCache::new(self.schema_cache),
            upload_checkpoint_dir: self.upload_checkpoint_dir,
        })
    }
}

impl LakeClient {
    /// Configure an authenticated and optionally TLS-verified Query connection.
    pub fn builder(query_endpoint: impl Into<String>) -> LakeClientBuilder {
        LakeClientBuilder {
            query_endpoint:        query_endpoint.into(),
            security:              ClientSecurity::new(),
            schema_cache:          SchemaCacheConfig::default(),
            upload_checkpoint_dir: None,
        }
    }

    /// Connect through query and discover the managed `FILE` stage once.
    pub async fn connect(query_endpoint: impl AsRef<str>) -> Result<Self> {
        Self::builder(query_endpoint.as_ref().to_owned())
            .connect()
            .await
    }

    /// Connect with an explicitly injected managed stage for tests and
    /// advanced embedding.
    pub async fn connect_with_store<S>(query_endpoint: impl AsRef<str>, objects: S) -> Result<Self>
    where
        S: ManagedObjectStore + 'static,
    {
        Self::builder(query_endpoint.as_ref().to_owned())
            .connect_with_store(objects)
            .await
    }

    /// Expire one cached table schema immediately.
    pub async fn invalidate_table_schema(&self, table: &TableRef) {
        self.schema_cache.invalidate(table).await;
    }

    /// Expire every cached table schema immediately.
    pub fn clear_schema_cache(&self) { self.schema_cache.clear(); }

    /// Execute a parameterized, single-row INSERT with typed scalar/`FILE`
    /// values.
    pub async fn insert(&self, sql: &str, values: Vec<InsertValue>) -> Result<Version> {
        self.insert_many(sql, vec![values]).await
    }

    /// Execute one bounded multi-row INSERT with typed scalar/`FILE` values.
    pub async fn insert_many(&self, sql: &str, rows: Vec<Vec<InsertValue>>) -> Result<Version> {
        let pending = self.prepare_insert_many(sql, rows).await?;
        self.resume_append(pending).await
    }

    /// Resume a prepared or ambiguously timed-out append with the same
    /// operation identity and already-uploaded object references.
    pub async fn resume_append(&self, pending: PendingAppend) -> Result<Version> {
        self.resume_append_with_window(pending, APPEND_RETRY_WINDOW)
            .await
    }

    async fn resume_append_with_window(
        &self,
        pending: PendingAppend,
        window: std::time::Duration,
    ) -> Result<Version> {
        resume_pending_with(pending, window, |messages| self.put_append_once(messages)).await
    }

    /// List durable append operation IDs without loading their Arrow payloads.
    pub async fn pending_append_ids(&self) -> Result<Vec<AppendOperationId>> {
        append_checkpoint::list(
            self.upload_checkpoint_dir.as_deref(),
            MAX_PENDING_APPEND_CHECKPOINTS,
        )
        .await
    }

    /// Load one exact durable append for explicit inspection or resumption.
    pub async fn load_pending_append(
        &self,
        operation_id: &AppendOperationId,
    ) -> Result<PendingAppend> {
        append_checkpoint::load(
            self.upload_checkpoint_dir.as_deref(),
            operation_id,
            &self.objects.stage_identity(),
            MAX_INSERT_FLIGHT_BYTES,
        )
        .await
    }

    /// Resume one durable append by operation ID after an SDK restart.
    ///
    /// Resume before the server's `LAKE_APPEND_OPERATION_RETENTION_SECS`
    /// horizon (seven days by default). An expired operation is conclusively
    /// rejected and its local checkpoint is removed.
    pub async fn resume_pending_append(&self, operation_id: &AppendOperationId) -> Result<Version> {
        let pending = self.load_pending_append(operation_id).await?;
        self.resume_append(pending).await
    }

    async fn prepare_insert(&self, sql: &str, values: Vec<InsertValue>) -> Result<PendingAppend> {
        self.prepare_insert_many(sql, vec![values]).await
    }

    async fn prepare_insert_many(
        &self,
        sql: &str,
        rows: Vec<Vec<InsertValue>>,
    ) -> Result<PendingAppend> {
        if rows.is_empty() || rows.len() > MAX_INSERT_BATCH_ROWS {
            return Err(SdkError::BatchRowCount {
                actual:  rows.len(),
                maximum: MAX_INSERT_BATCH_ROWS,
            });
        }
        let input_bytes = batch_input_metadata_bytes(sql, &rows);
        if input_bytes > MAX_INSERT_INPUT_METADATA_BYTES {
            return Err(SdkError::BatchMetadataSize {
                actual:  input_bytes,
                maximum: MAX_INSERT_INPUT_METADATA_BYTES,
            });
        }
        let insert = parse_insert(sql)?;
        for values in &rows {
            if insert.columns.len() != values.len() {
                return Err(SdkError::ParameterCount {
                    expected: insert.columns.len(),
                    actual:   values.len(),
                });
            }
        }
        let schema = self.table_schema(&insert.table).await?;
        for values in &rows {
            validate_bindings(&schema, &insert.columns, values)?;
        }

        let mut column_values = (0..schema.fields().len())
            .map(|_| Vec::with_capacity(rows.len()))
            .collect::<Vec<_>>();
        for values in rows {
            let mut bindings = insert
                .columns
                .iter()
                .cloned()
                .zip(values)
                .collect::<BTreeMap<_, _>>();
            for (index, field) in schema.fields().iter().enumerate() {
                column_values[index].push(bindings.remove(field.name()).ok_or_else(|| {
                    SdkError::MissingColumn {
                        column: field.name().to_owned(),
                    }
                })?);
            }
        }
        let mut arrays = Vec::<ArrayRef>::with_capacity(schema.fields().len());
        let mut output_metadata_bytes = 0usize;
        for (field, values) in schema.fields().iter().zip(column_values) {
            arrays.push(
                self.upload_and_encode_column(
                    field.data_type(),
                    values,
                    &mut output_metadata_bytes,
                )
                .await?,
            );
        }
        let batch = RecordBatch::try_new(schema, arrays).context(ArrowSnafu)?;
        let mut messages = FlightDataEncoderBuilder::new()
            .with_schema(batch.schema())
            .build(futures::stream::iter(vec![Ok(batch)]))
            .try_collect::<Vec<_>>()
            .await
            .context(FlightSnafu)?;
        let operation_id = AppendOperationId::generate();
        let append = FileAppendRequest::new(
            insert.table,
            operation_id.clone(),
            append_flight_payload_digest(&messages),
        );
        let descriptor = FlightDescriptor::new_cmd(
            Any {
                type_url: FILE_APPEND_TYPE_URL.to_owned(),
                value:    append.command_payload(),
            }
            .encode_to_vec(),
        );
        messages
            .first_mut()
            .expect("Flight encoder emits a schema message")
            .flight_descriptor = Some(descriptor);
        validate_flight_payload_size(&messages, MAX_INSERT_FLIGHT_BYTES)?;
        let mut pending = PendingAppend {
            operation_id,
            messages,
            checkpoint: None,
        };
        pending.checkpoint = append_checkpoint::save(
            self.upload_checkpoint_dir.as_deref(),
            &pending,
            &self.objects.stage_identity(),
            MAX_INSERT_FLIGHT_BYTES,
        )
        .await?;
        Ok(pending)
    }

    async fn put_append_once(&self, messages: Vec<FlightData>) -> Result<PutResult> {
        let stream = futures::stream::iter(messages.into_iter().map(Ok));
        let mut client = FlightClient::new(self.query.clone());
        self.security
            .apply_to_flight_client(&mut client)
            .context(SecuritySnafu)?;
        client
            .do_put(stream)
            .await
            .context(FlightSnafu)?
            .try_next()
            .await
            .context(FlightSnafu)?
            .context(MissingAppendResultSnafu)
    }

    /// Execute read-only SQL through the query endpoint and stream its complete
    /// Arrow result.
    ///
    /// Before the first `DoGet`, validates every endpoint in the returned
    /// `FlightInfo`: each must have a bounded non-empty ticket and may have no
    /// locations or only the exact `arrow-flight-reuse-connection://?`
    /// location. The SDK then consumes the validated endpoints sequentially
    /// in declared order. An invalid endpoint returns a typed, redacted
    /// [`SdkError`] before any result stream is redeemed; the SDK neither
    /// follows external endpoint locations nor forwards credentials to
    /// them.
    ///
    /// The returned [`QueryResultStream`] is the deliberate semver migration
    /// from Arrow's `FlightRecordBatchStream`: callers keep normal
    /// `futures::TryStreamExt` consumption, while per-`DoGet` headers and
    /// trailers are not exposed because they have no whole-result meaning.
    pub async fn query(&self, sql: &str) -> Result<QueryResultStream> {
        let client =
            arrow_flight::flight_service_client::FlightServiceClient::new(self.query.clone())
                .max_decoding_message_size(MAX_QUERY_RESULT_FLIGHT_INFO_BYTES);
        let mut client = FlightSqlServiceClient::new_from_inner(client);
        self.security.apply_to_sql_client(&mut client);
        let info = client
            .execute(sql.to_owned(), None)
            .await
            .context(FlightSnafu)?;
        self.open_query_result(Self::query_result_tickets(info)?)
    }

    /// Submit read-only SQL through standard Flight `PollFlightInfo`, wait for
    /// durable completion, then stream every ordered result endpoint.
    pub async fn query_async(&self, sql: &str) -> Result<AsyncQueryResultStream> {
        self.query_async_with_timeout(sql, Duration::from_hours(24))
            .await
    }

    /// As [`Self::query_async`], with a client-side bound on polling time.
    pub async fn query_async_with_timeout(
        &self,
        sql: &str,
        timeout: Duration,
    ) -> Result<AsyncQueryResultStream> {
        if timeout.is_zero() {
            return Err(SdkError::AsyncQueryTimeout { timeout });
        }
        tokio::time::timeout(timeout, async {
            let handle = self.submit_async(sql).await?;
            self.resume_async(handle).await
        })
        .await
        .map_err(|_| SdkError::AsyncQueryTimeout { timeout })?
    }

    /// Submit once and return a persistable capability without waiting for
    /// execution. Ambiguous initial responses retry with one stable id.
    pub async fn submit_async(&self, sql: &str) -> Result<AsyncQueryHandle> {
        self.submit_async_with_timeout(sql, ASYNC_SUBMIT_RETRY_WINDOW)
            .await
    }

    /// As [`Self::submit_async`], with a finite initial-response retry window.
    pub async fn submit_async_with_timeout(
        &self,
        sql: &str,
        timeout: Duration,
    ) -> Result<AsyncQueryHandle> {
        if timeout.is_zero() {
            return Err(SdkError::AsyncQueryTimeout { timeout });
        }
        let submission_id = uuid::Uuid::now_v7();
        let command = CommandStatementQuery {
            query:          sql.to_owned(),
            transaction_id: Some(submission_id.as_bytes().to_vec().into()),
        };
        let descriptor = FlightDescriptor::new_cmd(command.as_any().encode_to_vec());
        let mut client = FlightClient::new(self.query.clone());
        self.security
            .apply_to_flight_client(&mut client)
            .context(SecuritySnafu)?;
        tokio::time::timeout(timeout, async {
            let mut backoff = ASYNC_POLL_INITIAL_BACKOFF;
            loop {
                match client.poll_flight_info(descriptor.clone()).await {
                    Ok(poll) => return async_handle_from_poll(&poll),
                    Err(error) if ambiguous_async_submission_error(&error) => {
                        tokio::time::sleep(backoff).await;
                        backoff = backoff.saturating_mul(2).min(ASYNC_POLL_MAX_BACKOFF);
                    }
                    Err(source) => return Err(SdkError::Flight { source }),
                }
            }
        })
        .await
        .map_err(|_| SdkError::AsyncQueryTimeout { timeout })?
    }

    /// Perform one standard `PollFlightInfo` call using a restored handle.
    pub async fn poll_async(&self, handle: &AsyncQueryHandle) -> Result<AsyncQueryPoll> {
        handle.validate()?;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(u64::MAX, |duration| duration.as_secs());
        if now >= handle.expires_at_secs {
            return Err(SdkError::AsyncQueryHandleExpired {
                expires_at: handle.expires_at_secs,
            });
        }
        let mut client = FlightClient::new(self.query.clone());
        self.security
            .apply_to_flight_client(&mut client)
            .context(SecuritySnafu)?;
        let poll = client
            .poll_flight_info(FlightDescriptor::new_cmd(handle.poll_descriptor.clone()))
            .await
            .context(FlightSnafu)?;
        if poll.flight_descriptor.is_some() {
            Ok(AsyncQueryPoll::Pending(async_handle_from_poll(&poll)?))
        } else {
            Ok(AsyncQueryPoll::Complete(async_result_from_poll(poll)?))
        }
    }

    /// Resume a restored handle until completion and open its ordered parts.
    pub async fn resume_async(&self, handle: AsyncQueryHandle) -> Result<AsyncQueryResultStream> {
        self.resume_async_with_timeout(handle, Duration::from_hours(24))
            .await
    }

    /// As [`Self::resume_async`], bounded by caller-selected wall time.
    pub async fn resume_async_with_timeout(
        &self,
        mut handle: AsyncQueryHandle,
        timeout: Duration,
    ) -> Result<AsyncQueryResultStream> {
        if timeout.is_zero() {
            return Err(SdkError::AsyncQueryTimeout { timeout });
        }
        let result = tokio::time::timeout(timeout, async {
            let mut backoff = ASYNC_POLL_INITIAL_BACKOFF;
            loop {
                match self.poll_async(&handle).await? {
                    AsyncQueryPoll::Pending(refreshed) => handle = refreshed,
                    AsyncQueryPoll::Complete(result) => return Ok::<_, SdkError>(result),
                }
                tokio::time::sleep(backoff).await;
                backoff = backoff.saturating_mul(2).min(ASYNC_POLL_MAX_BACKOFF);
            }
        })
        .await
        .map_err(|_| SdkError::AsyncQueryTimeout { timeout })??;
        self.open_async_result(result)
    }

    /// Cancel queued or running work represented by a restored handle.
    pub async fn cancel_async(&self, handle: &AsyncQueryHandle) -> Result<CancelStatus> {
        handle.validate()?;
        let mut client = FlightClient::new(self.query.clone());
        self.security
            .apply_to_flight_client(&mut client)
            .context(SecuritySnafu)?;
        let result = client
            .cancel_flight_info(CancelFlightInfoRequest::new(
                FlightInfo::new().with_app_metadata(handle.poll_descriptor.clone()),
            ))
            .await
            .context(FlightSnafu)?;
        Ok(result.status())
    }

    /// Consume exact completed result tickets in manifest order.
    pub fn open_async_result(&self, result: AsyncQueryResult) -> Result<AsyncQueryResultStream> {
        if result.tickets.is_empty() || result.tickets.len() > MAX_ASYNC_RESULT_ENDPOINTS {
            return Err(SdkError::MissingQueryEndpoint);
        }
        let tickets = result.tickets;
        let mut requests = Vec::with_capacity(tickets.len());
        for ticket in tickets {
            let mut client = FlightClient::new(self.query.clone());
            self.security
                .apply_to_flight_client(&mut client)
                .context(SecuritySnafu)?;
            requests.push((client, ticket));
        }
        let streams = futures::stream::iter(requests)
            .then(|(mut client, ticket)| async move { client.do_get(ticket).await });
        Ok(Box::pin(streams.try_flatten()))
    }

    fn open_query_result(&self, tickets: Vec<Ticket>) -> Result<QueryResultStream> {
        let mut requests = Vec::with_capacity(tickets.len());
        for ticket in tickets {
            let mut client = FlightClient::new(self.query.clone());
            self.security
                .apply_to_flight_client(&mut client)
                .context(SecuritySnafu)?;
            requests.push((client, ticket));
        }
        let streams = futures::stream::iter(requests)
            .then(|(mut client, ticket)| async move { client.do_get(ticket).await });
        Ok(Box::pin(streams.try_flatten()))
    }

    fn query_result_tickets(info: FlightInfo) -> Result<Vec<Ticket>> {
        if info.endpoint.is_empty() {
            return Err(SdkError::MissingQueryEndpoint);
        }
        if info.endpoint.len() > MAX_QUERY_RESULT_ENDPOINTS {
            return Err(SdkError::InvalidQueryResultEndpoint);
        }
        let mut tickets = Vec::with_capacity(info.endpoint.len());
        let mut ticket_bytes = 0usize;
        for endpoint in info.endpoint {
            if endpoint
                .location
                .iter()
                .any(|location| location.uri != FLIGHT_REUSE_CONNECTION_LOCATION)
            {
                return Err(SdkError::UnsupportedQueryResultLocation);
            }
            let ticket = endpoint.ticket.context(MissingQueryTicketSnafu)?;
            if ticket.ticket.is_empty()
                || ticket.ticket.len() > MAX_QUERY_RESULT_TICKET_BYTES
                || ticket_bytes
                    .checked_add(ticket.ticket.len())
                    .is_none_or(|total| total > MAX_QUERY_RESULT_TICKET_TOTAL_BYTES)
            {
                return Err(SdkError::InvalidQueryResultEndpoint);
            }
            ticket_bytes += ticket.ticket.len();
            tickets.push(ticket);
        }
        Ok(tickets)
    }

    fn async_result_tickets(info: FlightInfo) -> Result<Vec<Ticket>> {
        if info.endpoint.is_empty() || info.endpoint.len() > MAX_ASYNC_RESULT_ENDPOINTS {
            return Err(SdkError::MissingQueryEndpoint);
        }
        let tickets = info
            .endpoint
            .into_iter()
            .map(|endpoint| endpoint.ticket.context(MissingQueryTicketSnafu))
            .collect::<Result<Vec<_>>>()?;
        if tickets.iter().any(|ticket| {
            ticket.ticket.is_empty() || ticket.ticket.len() > MAX_ASYNC_QUERY_HANDLE_BYTES
        }) {
            return Err(SdkError::InvalidAsyncQueryResult);
        }
        Ok(tickets)
    }

    /// Open a direct storage reader that verifies size and SHA-256 at EOF.
    ///
    /// Callers must drain the reader to EOF for verification to complete.
    pub async fn open(&self, location: &DataLocation) -> Result<ObjectReader> {
        open_verified(self.objects.as_ref(), location)
            .await
            .context(ObjectSnafu)
    }

    /// Open exactly one non-empty half-open byte range directly from storage.
    pub async fn open_range(
        &self,
        location: &DataLocation,
        range: Range<u64>,
    ) -> Result<ObjectReader> {
        self.objects
            .open_range(location, range)
            .await
            .context(ObjectSnafu)
    }

    /// Mint a short-lived direct HTTP GET capability for a managed object.
    pub async fn presign_read(
        &self,
        location: &DataLocation,
        expires_in: Duration,
    ) -> Result<PresignedRead> {
        validate_presign_expiration(expires_in).context(ObjectSnafu)?;
        let capability = self
            .objects
            .presign_read(location, expires_in)
            .await
            .context(ObjectSnafu)?;
        let now = SystemTime::now();
        let maximum = now
            .checked_add(expires_in)
            .expect("validated one-hour expiration fits SystemTime");
        if capability.expires_at() <= now || capability.expires_at() > maximum {
            return Err(SdkError::Object {
                source: lake_objects::ObjectError::InvalidPresignedCapabilityLifetime,
            });
        }
        Ok(capability)
    }

    async fn table_schema(&self, table: &TableRef) -> Result<SchemaRef> {
        self.schema_cache
            .resolve(table.clone(), || self.fetch_table_schema(table))
            .await
            .map_err(SchemaLoadError::into_sdk_error)
    }

    async fn fetch_table_schema(&self, table: &TableRef) -> SchemaLoadResult {
        let mut client = FlightSqlServiceClient::new(self.query.clone());
        self.security.apply_to_sql_client(&mut client);
        let info = client
            .execute(format!("SELECT * FROM lake.{table} LIMIT 0"), None)
            .await
            .map_err(|source| SchemaLoadError::Flight(FlightErrorSnapshot::from_error(source)))?;
        let schema = Schema::try_from(info)
            .map_err(|source| SchemaLoadError::Arrow(ArrowErrorSnapshot::from_error(source)))?;
        Ok(Arc::new(schema))
    }

    async fn upload_and_encode_column(
        &self,
        data_type: &DataType,
        values: Vec<InsertValue>,
        output_metadata_bytes: &mut usize,
    ) -> Result<ArrayRef> {
        if data_type == &DataType::Utf8 {
            let values = values
                .into_iter()
                .map(|value| match value {
                    InsertValue::Utf8(value) => Ok(value),
                    InsertValue::File(_) => Err(SdkError::TypeMismatch {
                        column: "bound value".to_owned(),
                    }),
                })
                .collect::<Result<Vec<_>>>()?;
            return Ok(Arc::new(StringArray::from(values)));
        }
        if data_type == data_location_field("ignored", false).data_type() {
            let mut locations = Vec::with_capacity(values.len());
            for value in values {
                let InsertValue::File(file) = value else {
                    return Err(SdkError::TypeMismatch {
                        column: "bound value".to_owned(),
                    });
                };
                let location = match file.source {
                    ObjectSource::Path(path) => {
                        let checkpoint = self.checkpoint_path(&path).await?;
                        self.objects
                            .put_path(path, file.content_type, checkpoint)
                            .await
                    }
                    ObjectSource::Reader(reader) => {
                        self.objects
                            .put_reader(Box::into_pin(reader), file.content_type)
                            .await
                    }
                };
                let location = location.context(ObjectSnafu)?;
                *output_metadata_bytes =
                    output_metadata_bytes.saturating_add(data_location_metadata_bytes(&location));
                if *output_metadata_bytes > MAX_INSERT_OUTPUT_METADATA_BYTES {
                    return Err(SdkError::BatchMetadataSize {
                        actual:  *output_metadata_bytes,
                        maximum: MAX_INSERT_OUTPUT_METADATA_BYTES,
                    });
                }
                locations.push(location);
            }
            return Ok(Arc::new(data_location_array(&locations)));
        }
        Err(SdkError::TypeMismatch {
            column: "bound value".to_owned(),
        })
    }

    async fn checkpoint_path(&self, source: &Path) -> Result<Option<PathBuf>> {
        let Some(directory) = &self.upload_checkpoint_dir else {
            return Ok(None);
        };
        let canonical = tokio::fs::canonicalize(source)
            .await
            .map_err(|source_error| SdkError::SourceFile {
                path:   source.to_path_buf(),
                source: source_error,
            })?;
        let mut hasher = Sha256::new();
        hasher.update(self.objects.stage_identity());
        hasher.update([0]);
        hasher.update(canonical.as_os_str().as_encoded_bytes());
        Ok(Some(
            directory.join(format!("{:x}.upload.json", hasher.finalize())),
        ))
    }
}

async fn prepare_checkpoint_dir(directory: Option<&Path>) -> Result<()> {
    if let Some(directory) = directory {
        tokio::fs::create_dir_all(directory)
            .await
            .map_err(|source| SdkError::SourceFile {
                path: directory.to_path_buf(),
                source,
            })?;
    }
    Ok(())
}

async fn discover_managed_stage(
    query: Channel,
    security: &ClientSecurity,
) -> Result<ManagedStageDescriptor> {
    let mut client = FlightClient::new(query);
    security
        .apply_to_flight_client(&mut client)
        .context(SecuritySnafu)?;
    let mut results = client
        .do_action(Action {
            r#type: MANAGED_STAGE_DISCOVERY_ACTION.to_owned(),
            body:   Vec::new().into(),
        })
        .await
        .context(FlightSnafu)?;
    let wire = results
        .try_next()
        .await
        .context(FlightSnafu)?
        .context(MissingManagedStageSnafu)?;
    if results.try_next().await.context(FlightSnafu)?.is_some() {
        return Err(SdkError::MultipleManagedStages);
    }
    ManagedStageDescriptor::from_wire(&wire).context(InvalidManagedStageSnafu)
}

async fn open_managed_stage(
    descriptor: &ManagedStageDescriptor,
) -> Result<Arc<dyn ManagedObjectStore>> {
    match descriptor.backend() {
        ManagedStageBackend::Local { root } => Ok(Arc::new(
            LocalObjectStore::open(root).await.context(ObjectSnafu)?,
        )),
        ManagedStageBackend::S3 {
            bucket,
            prefix,
            region,
            endpoint,
            force_path_style,
        } => {
            let mut loader = aws_config::defaults(BehaviorVersion::latest());
            if let Some(region) = region {
                loader = loader.region(Region::new(region.clone()));
            }
            let shared = loader.load().await;
            let mut config =
                aws_sdk_s3::config::Builder::from(&shared).force_path_style(*force_path_style);
            if let Some(endpoint) = endpoint {
                config = config.endpoint_url(endpoint);
            }
            let store = S3ObjectStore::new(
                aws_sdk_s3::Client::from_conf(config.build()),
                bucket,
                prefix,
            )
            .context(ObjectSnafu)?;
            Ok(Arc::new(store))
        }
    }
}

/// Decode one logical SQL `FILE` value from a named query-result column.
pub fn data_location(batch: &RecordBatch, column: &str, row: usize) -> Result<DataLocation> {
    if row >= batch.num_rows() {
        return Err(SdkError::RowOutOfBounds {
            row,
            rows: batch.num_rows(),
        });
    }
    let values = batch
        .column_by_name(column)
        .ok_or_else(|| SdkError::MissingResultColumn {
            column: column.to_owned(),
        })?
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| SdkError::InvalidFileColumn {
            column: column.to_owned(),
        })?;
    data_location_from_array(values, row).context(ObjectSnafu)
}

#[derive(Debug)]
struct Insert {
    table:   TableRef,
    columns: Vec<String>,
}

fn parse_insert(sql: &str) -> Result<Insert> {
    let sql = sql.trim().trim_end_matches(';').trim();
    let rest = sql
        .strip_prefix("INSERT INTO ")
        .ok_or_else(|| SdkError::InvalidSql {
            message: "statement must begin with INSERT INTO".to_owned(),
        })?;
    let (target, rest) = rest.split_once('(').ok_or_else(|| SdkError::InvalidSql {
        message: "missing column list".to_owned(),
    })?;
    let (columns, values) = rest
        .split_once(") VALUES (")
        .ok_or_else(|| SdkError::InvalidSql {
            message: "expected ) VALUES (".to_owned(),
        })?;
    let values = values
        .strip_suffix(')')
        .ok_or_else(|| SdkError::InvalidSql {
            message: "missing closing value list".to_owned(),
        })?;
    let (namespace, name) = target
        .trim()
        .split_once('.')
        .ok_or_else(|| SdkError::InvalidSql {
            message: "table must be <namespace>.<name>".to_owned(),
        })?;
    if !identifier(namespace) || !identifier(name) {
        return Err(SdkError::InvalidSql {
            message: "table identifiers may contain only ASCII letters, digits, and underscores"
                .to_owned(),
        });
    }
    let columns = columns
        .split(',')
        .map(str::trim)
        .map(|column| {
            if !identifier(column) {
                return Err(SdkError::InvalidSql {
                    message: format!("invalid column identifier '{column}'"),
                });
            }
            Ok(column.to_owned())
        })
        .collect::<Result<Vec<_>>>()?;
    if columns.is_empty() || values.split(',').any(|value| value.trim() != "?") {
        return Err(SdkError::InvalidSql {
            message: "VALUES must contain one ? placeholder per column".to_owned(),
        });
    }
    if values.split(',').count() != columns.len() || duplicate(&columns) {
        return Err(SdkError::InvalidSql {
            message: "column names and placeholders must be one-to-one".to_owned(),
        });
    }
    Ok(Insert {
        table: TableRef::new(namespace, name),
        columns,
    })
}

fn validate_bindings(schema: &SchemaRef, columns: &[String], values: &[InsertValue]) -> Result<()> {
    for (column, value) in columns.iter().zip(values) {
        let field = schema
            .fields()
            .iter()
            .find(|field| field.name() == column)
            .ok_or_else(|| SdkError::UnknownColumn {
                column: column.clone(),
            })?;
        let matches = match value {
            InsertValue::Utf8(_) => field.data_type() == &DataType::Utf8,
            InsertValue::File(_) => {
                field.data_type() == data_location_field("ignored", false).data_type()
            }
        };
        if !matches {
            return Err(SdkError::TypeMismatch {
                column: column.clone(),
            });
        }
    }
    for field in schema.fields() {
        if !columns.iter().any(|column| column == field.name()) {
            return Err(SdkError::MissingColumn {
                column: field.name().to_owned(),
            });
        }
    }
    Ok(())
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn duplicate(values: &[String]) -> bool {
    values
        .iter()
        .enumerate()
        .any(|(index, value)| values[..index].iter().any(|prior| prior == value))
}

fn batch_input_metadata_bytes(sql: &str, rows: &[Vec<InsertValue>]) -> usize {
    rows.iter().flatten().fold(sql.len(), |total, value| {
        let bytes = match value {
            InsertValue::Utf8(value) => value.len(),
            InsertValue::File(file) => {
                let source = match &file.source {
                    ObjectSource::Path(path) => path.as_os_str().as_encoded_bytes().len(),
                    ObjectSource::Reader(_) => 0,
                };
                source.saturating_add(file.content_type.len())
            }
        };
        total.saturating_add(bytes)
    })
}

fn data_location_metadata_bytes(location: &DataLocation) -> usize {
    location
        .uri
        .len()
        .saturating_add(location.content_type.len())
        .saturating_add(location.sha256.len())
        .saturating_add(std::mem::size_of::<u64>())
}

fn validate_flight_payload_size(messages: &[FlightData], maximum: usize) -> Result<()> {
    let actual = messages.iter().fold(0usize, |total, message| {
        total.saturating_add(message.encoded_len())
    });
    if actual > maximum {
        return Err(SdkError::BatchMetadataSize { actual, maximum });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io,
        path::PathBuf,
        pin::Pin,
        sync::{
            Arc, Mutex as StdMutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::{Array, StringArray, StructArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use arrow_flight::{
        CancelStatus, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo, Ticket,
        encode::FlightDataEncoderBuilder,
        error::FlightError,
        flight_service_server::{FlightService, FlightServiceServer},
        sql::{
            CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery,
            server::FlightSqlService,
        },
    };
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::{Credentials, Region};
    use futures::TryStreamExt;
    use lake_common::{
        ManagedStageDescriptor, Principal, PrincipalId, PrincipalRole, TableLocation, TableRef,
        TenantId, Version,
    };
    use lake_engine::TableEngineRef;
    use lake_engine_lance::LanceEngine;
    use lake_flight::{BearerPrincipalBinding, ClientSecurity, ServerSecurity};
    use lake_meta::{MetaStoreRef, RocksMeta};
    use lake_metasrv::{
        AppendResultGate, Metasrv, MetasrvServerConfig, election::LeaseElection,
        serve_with_config_and_crash,
    };
    use lake_objects::{
        LocalObjectStore, ManagedObjectStore, ObjectReader, Result as ObjectResult, S3ObjectStore,
        data_location_field, data_location_from_array,
    };
    use lake_query::{AsyncQueryConfig, QueryEngine, QueryServerConfig, QueryTicketKeyRing};
    use prost::Message;
    use rcgen::generate_simple_self_signed;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, ReadBuf},
        sync::Notify,
    };
    use tonic::{Request, Response, Status};

    use crate::{
        APPEND_RETRY_WINDOW, FileUpload, InsertValue, LakeClient, MAX_INSERT_BATCH_ROWS,
        MAX_INSERT_INPUT_METADATA_BYTES, MAX_SCHEMA_CACHE_CAPACITY, MAX_SCHEMA_CACHE_TTL,
        PresignedRead, SchemaCache, SchemaCacheConfig, SdkError, ambiguous_append_error,
        data_location, resume_pending_with, retry_ambiguous_append_with_window,
        validate_flight_payload_size,
    };

    #[derive(Clone)]
    struct CountingSchemaService {
        calls:              Arc<AtomicUsize>,
        failures_remaining: Arc<AtomicUsize>,
        schema:             Arc<Schema>,
        delay:              Duration,
    }

    #[tonic::async_trait]
    impl FlightSqlService for CountingSchemaService {
        type FlightService = Self;

        async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

        async fn get_flight_info_statement(
            &self,
            _query: CommandStatementQuery,
            request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<FlightInfo>, Status> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(self.delay).await;
            if self
                .failures_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
            {
                return Err(Status::unavailable("injected schema failure"));
            }
            let info = FlightInfo::new()
                .try_with_schema(&self.schema)
                .map_err(|error| Status::internal(error.to_string()))?
                .with_descriptor(request.into_inner());
            Ok(Response::new(info))
        }
    }

    async fn setup_counting_schema_client(
        root: &std::path::Path,
        capacity: u64,
        ttl: Duration,
        failures: usize,
    ) -> (LakeClient, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        let query_addr = free_addr();
        let service = CountingSchemaService {
            calls:              calls.clone(),
            failures_remaining: Arc::new(AtomicUsize::new(failures)),
            schema:             Arc::new(Schema::new(vec![Field::new(
                "episode_id",
                DataType::Utf8,
                false,
            )])),
            delay:              Duration::from_millis(20),
        };
        let socket = query_addr.parse().expect("query socket");
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve(socket)
                .await
                .expect("counting Flight SQL server");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let objects = LocalObjectStore::open(root.join("objects"))
            .await
            .expect("object store");
        let client = LakeClient::builder(format!("http://{query_addr}"))
            .with_schema_cache(capacity, ttl)
            .expect("cache config")
            .connect_with_store(objects)
            .await
            .expect("connected client");
        (client, calls)
    }

    #[derive(Clone)]
    struct EndpointQueryService {
        do_get_calls: Arc<AtomicUsize>,
        redeemed:     Arc<StdMutex<Vec<String>>>,
        schema:       Arc<Schema>,
    }

    impl EndpointQueryService {
        fn ticket(handle: impl Into<Vec<u8>>) -> Ticket {
            let ticket = TicketStatementQuery {
                statement_handle: handle.into().into(),
            };
            Ticket::new(ticket.as_any().encode_to_vec())
        }

        fn endpoint(handle: impl Into<Vec<u8>>) -> FlightEndpoint {
            FlightEndpoint::new().with_ticket(Self::ticket(handle))
        }

        fn response(&self, query: &str, descriptor: FlightDescriptor) -> FlightInfo {
            let endpoints = match query {
                "single" => vec![Self::endpoint(b"single")],
                "ordered" => vec![
                    Self::endpoint(b"first"),
                    Self::endpoint(b"second").with_location("arrow-flight-reuse-connection://?"),
                ],
                "missing-ticket" => vec![Self::endpoint(b"first"), FlightEndpoint::new()],
                "too-many-endpoints" => (0..257).map(|_| Self::endpoint(b"endpoint")).collect(),
                "oversized-ticket" => {
                    vec![FlightEndpoint::new().with_ticket(Ticket::new(vec![0_u8; 512 * 1024 + 1]))]
                }
                "too-many-ticket-bytes" => (0..17)
                    .map(|_| FlightEndpoint::new().with_ticket(Ticket::new(vec![0_u8; 512 * 1024])))
                    .collect(),
                "external-location" => vec![
                    Self::endpoint(b"first")
                        .with_location("arrow-flight-reuse-connection://?")
                        .with_location("https://capability.example.invalid/credential=secret"),
                ],
                "terminal-error" => vec![Self::endpoint(b"terminal-error")],
                _ => vec![Self::endpoint(b"single")],
            };
            FlightInfo::new()
                .try_with_schema(&self.schema)
                .expect("test schema encodes")
                .with_descriptor(descriptor)
                .with_endpoints(endpoints)
                .with_ordered(query == "ordered")
        }
    }

    #[tonic::async_trait]
    impl FlightSqlService for EndpointQueryService {
        type FlightService = Self;

        async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}

        async fn get_flight_info_statement(
            &self,
            query: CommandStatementQuery,
            request: Request<FlightDescriptor>,
        ) -> std::result::Result<Response<FlightInfo>, Status> {
            Ok(Response::new(
                self.response(&query.query, request.into_inner()),
            ))
        }

        async fn do_get_statement(
            &self,
            ticket: TicketStatementQuery,
            _request: Request<Ticket>,
        ) -> std::result::Result<Response<<Self as FlightService>::DoGetStream>, Status> {
            self.do_get_calls.fetch_add(1, Ordering::SeqCst);
            let handle = String::from_utf8(ticket.statement_handle.to_vec())
                .map_err(|_| Status::invalid_argument("test ticket must be UTF-8"))?;
            self.redeemed
                .lock()
                .expect("test redemption log lock")
                .push(handle.clone());
            let value = match handle.as_str() {
                "single" => 1,
                "first" => 2,
                "second" => 3,
                "terminal-error" => 4,
                _ => return Err(Status::invalid_argument("unknown test ticket")),
            };
            let batch = RecordBatch::try_from_iter(vec![(
                "value",
                Arc::new(arrow::array::Int64Array::from(vec![value])) as arrow::array::ArrayRef,
            )])
            .map_err(|error| Status::internal(error.to_string()))?;
            let schema = batch.schema();
            let mut batches = vec![Ok(batch)];
            if handle == "terminal-error" {
                batches.push(Err(FlightError::Tonic(Box::new(Status::internal(
                    "test terminal stream error",
                )))));
            }
            let stream = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .build(futures::stream::iter(batches))
                .map_err(Status::from);
            Ok(Response::new(Box::pin(stream)))
        }
    }

    async fn setup_endpoint_query_client(
        root: &std::path::Path,
    ) -> (LakeClient, Arc<AtomicUsize>, Arc<StdMutex<Vec<String>>>) {
        let do_get_calls = Arc::new(AtomicUsize::new(0));
        let redeemed = Arc::new(StdMutex::new(Vec::new()));
        let query_addr = free_addr();
        let service = EndpointQueryService {
            do_get_calls: do_get_calls.clone(),
            redeemed:     redeemed.clone(),
            schema:       Arc::new(Schema::new(vec![Field::new(
                "value",
                DataType::Int64,
                false,
            )])),
        };
        let socket = query_addr.parse().expect("query socket");
        tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(FlightServiceServer::new(service))
                .serve(socket)
                .await
                .expect("endpoint Flight SQL server");
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let objects = LocalObjectStore::open(root.join("objects"))
            .await
            .expect("object store");
        let client = LakeClient::builder(format!("http://{query_addr}"))
            .connect_with_store(objects)
            .await
            .expect("connected client");
        (client, do_get_calls, redeemed)
    }

    fn batch_value(batch: &RecordBatch) -> i64 {
        batch
            .column_by_name("value")
            .expect("test value column")
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .expect("test value array")
            .value(0)
    }

    #[tokio::test]
    async fn sdk_query_consumes_single_and_ordered_local_reuse_endpoints() {
        let root = tempdir().expect("fixture root");
        let (client, do_get_calls, redeemed) = setup_endpoint_query_client(root.path()).await;

        let single = client.query("single").await.expect("single endpoint query");
        let single = single
            .try_collect::<Vec<_>>()
            .await
            .expect("single endpoint stream");
        assert_eq!(single.iter().map(batch_value).collect::<Vec<_>>(), vec![1]);

        let ordered = client
            .query("ordered")
            .await
            .expect("ordered endpoint query");
        let ordered = ordered
            .try_collect::<Vec<_>>()
            .await
            .expect("ordered endpoint stream");
        assert_eq!(
            ordered.iter().map(batch_value).collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert_eq!(do_get_calls.load(Ordering::SeqCst), 3);
        assert_eq!(
            redeemed
                .lock()
                .expect("test redemption log lock")
                .as_slice(),
            ["single", "first", "second"]
        );
    }

    #[tokio::test]
    async fn sdk_query_rejects_missing_ticket_before_doget() {
        let root = tempdir().expect("fixture root");
        let (client, do_get_calls, _redeemed) = setup_endpoint_query_client(root.path()).await;

        assert!(matches!(
            client.query("missing-ticket").await,
            Err(SdkError::MissingQueryTicket)
        ));
        assert_eq!(do_get_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn sdk_query_rejects_excessive_endpoint_metadata_before_doget() {
        let root = tempdir().expect("fixture root");
        let (client, do_get_calls, _redeemed) = setup_endpoint_query_client(root.path()).await;

        for query in [
            "too-many-endpoints",
            "oversized-ticket",
            "too-many-ticket-bytes",
        ] {
            assert!(matches!(
                client.query(query).await,
                Err(SdkError::InvalidQueryResultEndpoint)
            ));
            assert_eq!(do_get_calls.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn sdk_query_rejects_external_location_before_doget() {
        let root = tempdir().expect("fixture root");
        let (client, do_get_calls, _redeemed) = setup_endpoint_query_client(root.path()).await;

        let Err(error) = client.query("external-location").await else {
            panic!("external endpoint location must fail");
        };
        assert!(matches!(error, SdkError::UnsupportedQueryResultLocation));
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(!display.contains("capability.example.invalid"));
        assert!(!display.contains("credential=secret"));
        assert!(!debug.contains("capability.example.invalid"));
        assert!(!debug.contains("credential=secret"));
        assert_eq!(do_get_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn sdk_query_result_stream_supports_try_stream_consumption() {
        let root = tempdir().expect("fixture root");
        let (client, _do_get_calls, _redeemed) = setup_endpoint_query_client(root.path()).await;

        let stream: crate::QueryResultStream =
            client.query("single").await.expect("single endpoint query");
        let batches = stream
            .try_collect::<Vec<_>>()
            .await
            .expect("normal stream consumption");
        assert_eq!(batches.iter().map(batch_value).collect::<Vec<_>>(), vec![1]);

        let stream: crate::QueryResultStream = client
            .query("terminal-error")
            .await
            .expect("terminal-error query starts");
        assert!(matches!(
            stream.try_collect::<Vec<_>>().await,
            Err(FlightError::Tonic(_))
        ));
    }

    #[tokio::test]
    async fn sdk_retries_lost_put_result_without_reupload_or_duplicate() {
        let root = tempdir().unwrap();
        let (client, metasrv, table, _meta, _engine) = setup_client(root.path()).await;
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"one uploaded video")
            .await
            .unwrap();
        let pending = client
            .prepare_insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-lost-result".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .expect("prepare and upload once");
        assert_eq!(object_count(&root.path().join("objects")).await, 1);
        let attempts = Arc::new(AtomicUsize::new(0));

        let result = retry_ambiguous_append_with_window(
            || {
                let attempts = attempts.clone();
                let messages = pending.messages.clone();
                let attempt_client = client.clone();
                async move {
                    let committed = attempt_client.put_append_once(messages).await?;
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(SdkError::Flight {
                            source: arrow_flight::error::FlightError::Tonic(Box::new(
                                tonic::Status::unavailable("response lost after commit"),
                            )),
                        })
                    } else {
                        Ok(committed)
                    }
                }
            },
            APPEND_RETRY_WINDOW,
        )
        .await
        .unwrap();

        assert_eq!(
            serde_json::from_slice::<Version>(&result.app_metadata).unwrap(),
            Version(2)
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
        let mut rows = client
            .query(
                "SELECT episode_id FROM lake.robots.episodes WHERE episode_id = \
                 'episode-lost-result'",
            )
            .await
            .unwrap();
        let mut row_count = 0;
        while let Some(batch) = rows.try_next().await.unwrap() {
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 1);
        assert_eq!(object_count(&root.path().join("objects")).await, 1);
    }

    #[tokio::test]
    async fn sdk_resumes_same_operation_after_retry_horizon() {
        let root = tempdir().unwrap();
        let (client, metasrv, table, _meta, _engine) = setup_client(root.path()).await;
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"one recoverable video")
            .await
            .unwrap();
        let pending = client
            .prepare_insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-recovered".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();
        let operation_id = pending.operation_id().clone();
        let broken = LakeClient {
            query:                 tonic::transport::Endpoint::from_static("http://127.0.0.1:9")
                .connect_lazy(),
            objects:               client.objects.clone(),
            security:              client.security.clone(),
            schema_cache:          client.schema_cache.clone(),
            upload_checkpoint_dir: client.upload_checkpoint_dir.clone(),
        };

        let error = broken
            .resume_append_with_window(pending, Duration::from_millis(5))
            .await
            .unwrap_err();
        let recovered = error
            .into_pending_append()
            .expect("retry expiry returns a recoverable append");
        assert_eq!(recovered.operation_id(), &operation_id);
        assert_eq!(object_count(&root.path().join("objects")).await, 1);

        assert_eq!(client.resume_append(recovered).await.unwrap(), Version(2));
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
        assert_eq!(object_count(&root.path().join("objects")).await, 1);
    }

    #[tokio::test]
    async fn durable_append_checkpoint_survives_client_restart() {
        let root = tempdir().unwrap();
        let (mut client, _metasrv, _table, _meta, _engine) = setup_client(root.path()).await;
        let checkpoints = root.path().join("checkpoints");
        tokio::fs::create_dir_all(&checkpoints).await.unwrap();
        client.upload_checkpoint_dir = Some(checkpoints.clone());
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"one durable video")
            .await
            .unwrap();

        let pending = client
            .prepare_insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-durable".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();
        let expected_messages = pending
            .messages
            .iter()
            .map(Message::encode_to_vec)
            .collect::<Vec<_>>();

        assert_eq!(
            client.pending_append_ids().await.unwrap(),
            vec![pending.operation_id().clone()]
        );
        let restarted = LakeClient {
            query:                 client.query.clone(),
            objects:               client.objects.clone(),
            security:              client.security.clone(),
            schema_cache:          SchemaCache::new(SchemaCacheConfig::default()),
            upload_checkpoint_dir: Some(checkpoints),
        };
        let recovered = restarted
            .load_pending_append(pending.operation_id())
            .await
            .unwrap();

        assert_eq!(recovered.operation_id(), pending.operation_id());
        assert_eq!(
            recovered
                .messages
                .iter()
                .map(Message::encode_to_vec)
                .collect::<Vec<_>>(),
            expected_messages
        );
        assert_eq!(object_count(&root.path().join("objects")).await, 1);
    }

    #[tokio::test]
    async fn durable_append_checkpoint_replays_post_commit_crash() {
        let root = tempdir().unwrap();
        let (mut client, metasrv, table, _meta, _engine) = setup_client(root.path()).await;
        let checkpoints = root.path().join("checkpoints");
        tokio::fs::create_dir_all(&checkpoints).await.unwrap();
        client.upload_checkpoint_dir = Some(checkpoints.clone());
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"one committed video")
            .await
            .unwrap();
        let pending = client
            .prepare_insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-committed".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();

        let committed = client
            .put_append_once(pending.messages.clone())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Version>(&committed.app_metadata).unwrap(),
            Version(2)
        );
        assert_eq!(client.pending_append_ids().await.unwrap().len(), 1);

        let restarted = LakeClient {
            query:                 client.query.clone(),
            objects:               client.objects.clone(),
            security:              client.security.clone(),
            schema_cache:          SchemaCache::new(SchemaCacheConfig::default()),
            upload_checkpoint_dir: Some(checkpoints),
        };
        assert_eq!(
            restarted
                .resume_pending_append(pending.operation_id())
                .await
                .unwrap(),
            Version(2)
        );
        assert!(restarted.pending_append_ids().await.unwrap().is_empty());
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
        assert_eq!(object_count(&root.path().join("objects")).await, 1);
    }

    #[tokio::test]
    async fn post_commit_response_decode_failure_retains_exact_checkpoint() {
        let root = tempdir().unwrap();
        let (mut client, metasrv, table, _meta, _engine) = setup_client(root.path()).await;
        let checkpoints = root.path().join("checkpoints");
        tokio::fs::create_dir_all(&checkpoints).await.unwrap();
        client.upload_checkpoint_dir = Some(checkpoints);
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"one response-ambiguous video")
            .await
            .unwrap();
        let pending = client
            .prepare_insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-response-decode".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();
        let operation_id = pending.operation_id().clone();
        let attempt_client = client.clone();
        let error = resume_pending_with(pending, Duration::from_millis(250), move |messages| {
            let attempt_client = attempt_client.clone();
            async move {
                attempt_client.put_append_once(messages).await?;
                Err(SdkError::Flight {
                    source: arrow_flight::error::FlightError::DecodeError(
                        "response truncated after commit".to_owned(),
                    ),
                })
            }
        })
        .await
        .unwrap_err();
        let recovered = error
            .into_pending_append()
            .expect("response decoding remains replayable");

        assert_eq!(recovered.operation_id(), &operation_id);
        assert_eq!(
            client.pending_append_ids().await.unwrap(),
            vec![operation_id]
        );
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
        assert_eq!(client.resume_append(recovered).await.unwrap(), Version(2));
        assert!(client.pending_append_ids().await.unwrap().is_empty());
        assert_eq!(object_count(&root.path().join("objects")).await, 1);
    }

    #[tokio::test]
    async fn post_commit_invalid_result_metadata_returns_pending_append() {
        let root = tempdir().unwrap();
        let (mut client, metasrv, table, _meta, _engine) = setup_client(root.path()).await;
        let checkpoints = root.path().join("checkpoints");
        tokio::fs::create_dir_all(&checkpoints).await.unwrap();
        client.upload_checkpoint_dir = Some(checkpoints);
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"one invalid-result video")
            .await
            .unwrap();
        let pending = client
            .prepare_insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-invalid-result".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();
        let operation_id = pending.operation_id().clone();
        let attempt_client = client.clone();
        let error = resume_pending_with(pending, APPEND_RETRY_WINDOW, move |messages| {
            let attempt_client = attempt_client.clone();
            async move {
                let mut result = attempt_client.put_append_once(messages).await?;
                result.app_metadata = b"not a version".to_vec().into();
                Ok(result)
            }
        })
        .await
        .unwrap_err();
        let recovered = error
            .into_pending_append()
            .expect("invalid post-commit result returns operation ownership");

        assert_eq!(recovered.operation_id(), &operation_id);
        assert_eq!(
            client.pending_append_ids().await.unwrap(),
            vec![operation_id]
        );
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
        assert_eq!(client.resume_append(recovered).await.unwrap(), Version(2));
        assert!(client.pending_append_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn durable_append_checkpoint_cleans_up_conclusive_outcomes() {
        let root = tempdir().unwrap();
        let (mut client, _metasrv, _table, _meta, _engine) = setup_client(root.path()).await;
        let checkpoints = root.path().join("checkpoints");
        tokio::fs::create_dir_all(&checkpoints).await.unwrap();
        client.upload_checkpoint_dir = Some(checkpoints.clone());
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"one retryable video")
            .await
            .unwrap();

        let pending = client
            .prepare_insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-ambiguous".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();
        let broken = LakeClient {
            query:                 tonic::transport::Endpoint::from_static("http://127.0.0.1:9")
                .connect_lazy(),
            objects:               client.objects.clone(),
            security:              client.security.clone(),
            schema_cache:          client.schema_cache.clone(),
            upload_checkpoint_dir: Some(checkpoints.clone()),
        };
        let retained = broken
            .resume_append_with_window(pending, Duration::from_millis(5))
            .await
            .unwrap_err()
            .into_pending_append()
            .expect("ambiguous expiry retains the append");
        assert_eq!(client.pending_append_ids().await.unwrap().len(), 1);

        let mut terminal = retained.clone();
        terminal.messages[0].flight_descriptor =
            Some(FlightDescriptor::new_cmd(b"not a FILE command".to_vec()));
        let error = client
            .resume_append_with_window(terminal, Duration::from_secs(1))
            .await
            .unwrap_err();
        assert!(error.into_pending_append().is_none());
        assert!(client.pending_append_ids().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn append_without_checkpoint_directory_remains_memory_only() {
        let root = tempdir().unwrap();
        let (client, _metasrv, _table, _meta, _engine) = setup_client(root.path()).await;
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"one in-memory video")
            .await
            .unwrap();

        let pending = client
            .prepare_insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-memory".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();

        assert!(pending.checkpoint.is_none());
        assert!(client.pending_append_ids().await.unwrap().is_empty());
        assert!(matches!(
            client.load_pending_append(pending.operation_id()).await,
            Err(SdkError::AppendCheckpointingDisabled)
        ));
        assert_eq!(client.resume_append(pending).await.unwrap(), Version(2));
    }

    #[tokio::test]
    async fn sdk_retries_same_insert_through_ungraceful_leader_failover() {
        let root = tempdir().unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::with_manifest_store(meta.clone()));
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![
            Field::new("episode_id", DataType::Utf8, false),
            data_location_field("video", false),
        ]));
        let bootstrap = Metasrv::new(meta.clone(), engine.clone());
        bootstrap
            .create_table(
                &table,
                TableLocation::new(root.path().join("tables/episodes.lance").to_string_lossy()),
                schema,
            )
            .await
            .unwrap();

        let addr_a = free_addr();
        let addr_b = free_addr();
        let gate_a = Arc::new(AppendResultGate::armed());
        let gate_b = Arc::new(AppendResultGate::armed());
        let (crash_a_tx, crash_a_rx) = tokio::sync::oneshot::channel();
        let (crash_b_tx, crash_b_rx) = tokio::sync::oneshot::channel();
        let mut crash_a_tx = Some(crash_a_tx);
        let mut crash_b_tx = Some(crash_b_tx);
        let server_a = tokio::spawn({
            let node = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
            let addr = addr_a.clone();
            let gate = gate_a.clone();
            async move {
                serve_with_config_and_crash(
                    node,
                    &addr,
                    MetasrvServerConfig::new().with_append_result_gate(gate),
                    async move {
                        let _ = crash_a_rx.await;
                    },
                )
                .await
            }
        });
        let server_b = tokio::spawn({
            let node = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
            let addr = addr_b.clone();
            let gate = gate_b.clone();
            async move {
                serve_with_config_and_crash(
                    node,
                    &addr,
                    MetasrvServerConfig::new().with_append_result_gate(gate),
                    async move {
                        let _ = crash_b_rx.await;
                    },
                )
                .await
            }
        });
        let mut server_a = Some(server_a);
        let mut server_b = Some(server_b);
        let observer = LeaseElection::new(meta.clone(), "observer", Duration::from_secs(10));
        let leader = tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                if let Some(leader) = observer.current_leader().await.unwrap()
                    && (leader == addr_a || leader == addr_b)
                {
                    break leader;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("metadata leader elected");
        let (follower, leader_gate) = if leader == addr_a {
            gate_b.disable();
            (addr_b.clone(), gate_a.clone())
        } else {
            gate_a.disable();
            (addr_a.clone(), gate_b.clone())
        };

        let query_addr = free_addr();
        tokio::spawn({
            let query = Arc::new(QueryEngine::new(meta.clone(), engine.clone()));
            let addr = query_addr.clone();
            let metadata = format!("http://{follower}");
            async move { lake_query::serve_with_metadata(query, &addr, &metadata).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let objects = DelegatingStore(
            LocalObjectStore::open(root.path().join("objects"))
                .await
                .unwrap(),
        );
        let client = LakeClient::connect_with_store(format!("http://{query_addr}"), objects)
            .await
            .unwrap();
        let source = root.path().join("failover.mp4");
        tokio::fs::write(&source, b"survives leader death")
            .await
            .unwrap();
        let insert_client = client.clone();
        let insert = tokio::spawn(async move {
            insert_client
                .insert(
                    "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                    vec![
                        InsertValue::Utf8("episode-failover".to_owned()),
                        InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                    ],
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), leader_gate.wait_until_blocked())
            .await
            .expect("leader committed and blocked its first result");

        if leader == addr_a {
            crash_a_tx.take().unwrap().send(()).unwrap();
            server_a.take().unwrap().await.unwrap().unwrap();
        } else {
            crash_b_tx.take().unwrap().send(()).unwrap();
            server_b.take().unwrap().await.unwrap().unwrap();
        }
        leader_gate.fail_blocked();
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if observer.current_leader().await.unwrap().as_deref() == Some(&follower) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("standby acquires the expired lease");
        let version = tokio::time::timeout(Duration::from_secs(25), insert)
            .await
            .expect("SDK retry spans lease expiry")
            .unwrap()
            .unwrap();
        assert_eq!(version, Version(2));
        assert_eq!(object_count(&root.path().join("objects")).await, 1);
        let mut rows = client
            .query(
                "SELECT episode_id FROM lake.robots.episodes WHERE episode_id = 'episode-failover'",
            )
            .await
            .unwrap();
        let mut row_count = 0;
        while let Some(batch) = rows.try_next().await.unwrap() {
            row_count += batch.num_rows();
        }
        assert_eq!(row_count, 1);

        if leader == addr_a {
            crash_b_tx.take().unwrap().send(()).unwrap();
            server_b.take().unwrap().await.unwrap().unwrap();
        } else {
            crash_a_tx.take().unwrap().send(()).unwrap();
            server_a.take().unwrap().await.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn schema_cache_coalesces_concurrent_lookups_across_clones() {
        let root = tempdir().expect("tempdir");
        let (client, calls) =
            setup_counting_schema_client(root.path(), 16, Duration::from_secs(1), 0).await;

        let resolved = futures::future::join_all((0..16).map(|_| {
            let client = client.clone();
            async move {
                client
                    .prepare_insert(
                        "INSERT INTO robots.episodes (episode_id) VALUES (?)",
                        vec![InsertValue::Utf8("episode-1".to_owned())],
                    )
                    .await
                    .expect("typed insert preparation")
            }
        }))
        .await;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(resolved.len(), 16);
        assert!(client.schema_cache.entry_count() <= 16);
    }

    #[tokio::test]
    async fn schema_cache_expiry_and_invalidation_refetch_without_caching_failures() {
        let root = tempdir().expect("tempdir");
        let (client, calls) =
            setup_counting_schema_client(root.path(), 2, Duration::from_millis(200), 1).await;
        let table = TableRef::new("robots", "episodes");
        let other = TableRef::new("robots", "other");
        let third = TableRef::new("robots", "third");
        let prepare = |table: &TableRef| {
            let sql = format!("INSERT INTO {table} (episode_id) VALUES (?)");
            let client = &client;
            async move {
                client
                    .prepare_insert(&sql, vec![InsertValue::Utf8("episode-1".to_owned())])
                    .await
            }
        };

        let failures = futures::future::join_all((0..8).map(|_| {
            let client = client.clone();
            async move {
                client
                    .prepare_insert(
                        "INSERT INTO robots.episodes (episode_id) VALUES (?)",
                        vec![InsertValue::Utf8("episode-1".to_owned())],
                    )
                    .await
            }
        }))
        .await;
        assert!(failures.iter().all(|result| matches!(
            result,
            Err(SdkError::Flight { source }) if matches!(
                source,
                arrow_flight::error::FlightError::Tonic(status)
                    if status.code() == tonic::Code::Unavailable
            )
        )));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        prepare(&table).await.expect("failure is not cached");
        prepare(&table).await.expect("cache hit");
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        prepare(&other).await.expect("unrelated table");
        client.invalidate_table_schema(&table).await;
        prepare(&table).await.expect("invalidated table");
        prepare(&other).await.expect("unrelated entry retained");
        assert_eq!(calls.load(Ordering::SeqCst), 4);

        prepare(&third).await.expect("capacity overflow");
        client.schema_cache.entries.run_pending_tasks().await;
        assert!(client.schema_cache.entry_count() <= 2);

        tokio::time::sleep(Duration::from_millis(250)).await;
        prepare(&table).await.expect("expired");
        let before_clear = calls.load(Ordering::SeqCst);
        client.clear_schema_cache();
        prepare(&other).await.expect("globally cleared");
        assert_eq!(calls.load(Ordering::SeqCst), before_clear + 1);
    }

    #[tokio::test]
    async fn schema_cache_invalidation_fences_in_flight_loader() {
        let cache = SchemaCache::new(SchemaCacheConfig {
            capacity: 2,
            ttl:      Duration::from_secs(1),
        });
        let table = TableRef::new("robots", "episodes");
        let old_schema = Arc::new(Schema::empty());
        let new_schema = Arc::new(Schema::new(vec![Field::new(
            "episode_id",
            DataType::Utf8,
            false,
        )]));
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let old_lookup = tokio::spawn({
            let cache = cache.clone();
            let table = table.clone();
            let old_schema = old_schema.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                cache
                    .resolve(table, || async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(old_schema)
                    })
                    .await
            }
        });
        entered.notified().await;

        cache.invalidate(&table).await;
        let current = tokio::time::timeout(
            Duration::from_millis(100),
            cache.resolve(table.clone(), || async { Ok(new_schema.clone()) }),
        )
        .await
        .expect("invalidation must detach the old loader")
        .expect("current schema");
        assert!(Arc::ptr_eq(&current, &new_schema));

        release.notify_one();
        assert!(Arc::ptr_eq(
            &old_lookup.await.expect("old task").expect("old caller"),
            &old_schema
        ));
        let retained = cache
            .resolve(table, || async { panic!("old loader repopulated cache") })
            .await
            .expect("new incarnation retained");
        assert!(Arc::ptr_eq(&retained, &new_schema));

        let other = TableRef::new("robots", "other");
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let old_lookup = tokio::spawn({
            let cache = cache.clone();
            let other = other.clone();
            let old_schema = old_schema.clone();
            let entered = entered.clone();
            let release = release.clone();
            async move {
                cache
                    .resolve(other, || async move {
                        entered.notify_one();
                        release.notified().await;
                        Ok(old_schema)
                    })
                    .await
            }
        });
        entered.notified().await;

        cache.clear();
        let current = tokio::time::timeout(
            Duration::from_millis(100),
            cache.resolve(other.clone(), || async { Ok(new_schema.clone()) }),
        )
        .await
        .expect("clear must detach the old loader")
        .expect("current schema after clear");
        assert!(Arc::ptr_eq(&current, &new_schema));
        release.notify_one();
        old_lookup.await.expect("old task").expect("old caller");
        let retained = cache
            .resolve(other, || async {
                panic!("old loader repopulated globally cleared cache")
            })
            .await
            .expect("new incarnation retained after clear");
        assert!(Arc::ptr_eq(&retained, &new_schema));
    }

    #[test]
    fn schema_cache_rejects_unbounded_configuration() {
        assert!(
            LakeClient::builder("http://127.0.0.1:9")
                .with_schema_cache(0, Duration::from_secs(1))
                .is_err()
        );
        assert!(
            LakeClient::builder("http://127.0.0.1:9")
                .with_schema_cache(MAX_SCHEMA_CACHE_CAPACITY + 1, Duration::from_secs(1))
                .is_err()
        );
        assert!(
            LakeClient::builder("http://127.0.0.1:9")
                .with_schema_cache(1, MAX_SCHEMA_CACHE_TTL + Duration::from_secs(1))
                .is_err()
        );
        let defaults = SchemaCacheConfig::default();
        assert_eq!(defaults.capacity, 1_024);
        assert_eq!(defaults.ttl, Duration::from_mins(1));
    }

    #[test]
    fn sdk_error_sources_remain_owned_public_types() {
        let flight = SdkError::Flight {
            source: arrow_flight::error::FlightError::Tonic(Box::new(tonic::Status::unavailable(
                "shape check",
            ))),
        };
        let SdkError::Flight {
            source: arrow_flight::error::FlightError::Tonic(status),
        } = flight
        else {
            panic!("owned FlightError source shape changed")
        };
        assert_eq!(status.code(), tonic::Code::Unavailable);

        let arrow = SdkError::Arrow {
            source: arrow::error::ArrowError::SchemaError("shape check".to_owned()),
        };
        let SdkError::Arrow {
            source: arrow::error::ArrowError::SchemaError(message),
        } = arrow
        else {
            panic!("owned ArrowError source shape changed")
        };
        assert_eq!(message, "shape check");
    }

    #[test]
    fn response_decode_and_protocol_failures_remain_ambiguous() {
        for source in [
            arrow_flight::error::FlightError::ProtocolError("post-commit response".to_owned()),
            arrow_flight::error::FlightError::DecodeError("truncated PutResult".to_owned()),
            arrow_flight::error::FlightError::Arrow(arrow::error::ArrowError::ParseError(
                "invalid response metadata".to_owned(),
            )),
        ] {
            assert!(ambiguous_append_error(&SdkError::Flight { source }));
        }
        assert!(!ambiguous_append_error(&SdkError::Flight {
            source: arrow_flight::error::FlightError::Tonic(Box::new(
                tonic::Status::invalid_argument("server rejected before commit"),
            )),
        }));
    }

    async fn object_count(path: &std::path::Path) -> usize {
        let mut entries = tokio::fs::read_dir(path).await.unwrap();
        let mut count = 0;
        while entries.next_entry().await.unwrap().is_some() {
            count += 1;
        }
        count
    }

    #[tokio::test]
    async fn unreachable_query_endpoint_fails_connect() {
        let root = tempdir().unwrap();
        let objects = LocalObjectStore::open(root.path().join("objects"))
            .await
            .unwrap();

        let result = LakeClient::connect_with_store("http://127.0.0.1:1", objects).await;

        assert!(
            result.is_err(),
            "an unreachable query endpoint must fail connect"
        );
    }

    #[tokio::test]
    async fn client_connects_only_to_query_for_file_insert() {
        let root = tempdir().unwrap();
        let (client, _metasrv, _table, meta, engine) = setup_client(root.path()).await;

        let source = root.path().join("episode.mp4");
        let expected = b"large video bytes streamed by the sdk";
        tokio::fs::write(&source, expected).await.unwrap();
        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-42".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();

        let query = QueryEngine::new(meta, engine);
        let mut batches = query
            .execute_sql("SELECT episode_id, video FROM lake.robots.episodes")
            .await
            .unwrap();
        let batch = batches.try_next().await.unwrap().expect("result batch");
        let episode_ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(episode_ids.value(0), "episode-42");
        let locations = batch
            .column(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let location = data_location_from_array(locations, 0).unwrap();
        assert_eq!(location.content_type, "video/mp4");
        assert_eq!(location.size_bytes, expected.len() as u64);
        assert_eq!(location.sha256, format!("{:x}", Sha256::digest(expected)));
        let mut reader = client.open(&location).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn sdk_resumable_file_insert_uses_checkpoint_directory() {
        let root = tempdir().unwrap();
        let seen = Arc::new(std::sync::Mutex::new(None));
        let objects = PathRecordingStore {
            inner: LocalObjectStore::open(root.path().join("objects"))
                .await
                .unwrap(),
            seen:  seen.clone(),
        };
        let (mut client, _metasrv, _table, _meta, _engine) =
            setup_client_with_store(root.path(), objects).await;
        let checkpoints = root.path().join("checkpoints");
        tokio::fs::create_dir_all(&checkpoints).await.unwrap();
        client.upload_checkpoint_dir = Some(checkpoints.clone());
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"resumable path bytes")
            .await
            .unwrap();

        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-resume".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();

        let checkpoint = seen
            .lock()
            .expect("recording mutex")
            .clone()
            .expect("path upload receives a checkpoint");
        assert_eq!(checkpoint.parent(), Some(checkpoints.as_path()));
        assert!(checkpoint.extension().is_some_and(|value| value == "json"));
        assert!(!checkpoint.exists());
    }

    #[tokio::test]
    async fn client_discovers_local_stage_from_query() {
        let root = tempdir().unwrap();
        let descriptor = ManagedStageDescriptor::local(
            root.path().join("objects").to_string_lossy().into_owned(),
        );
        let client = setup_client_with_descriptor(root.path(), descriptor).await;
        let expected = b"0123456789abcdef";

        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-discovery".to_owned()),
                    InsertValue::File(FileUpload::from_reader(
                        std::io::Cursor::new(expected),
                        "video/mp4",
                    )),
                ],
            )
            .await
            .unwrap();
        let mut results = client
            .query("SELECT video FROM lake.robots.episodes")
            .await
            .unwrap();
        let batch = results.try_next().await.unwrap().unwrap();
        let location = data_location(&batch, "video", 0).unwrap();
        let mut full_reader = client.open(&location).await.unwrap();
        let mut full = Vec::new();
        full_reader.read_to_end(&mut full).await.unwrap();
        let mut range_reader = client.open_range(&location, 4..10).await.unwrap();
        let mut range = Vec::new();
        range_reader.read_to_end(&mut range).await.unwrap();

        assert_eq!(full, expected);
        assert_eq!(range, b"456789");
    }

    #[tokio::test]
    async fn managed_stage_discovery_is_tenant_scoped() {
        let root = tempdir().unwrap();
        let base = ManagedStageDescriptor::local(
            root.path().join("objects").to_string_lossy().into_owned(),
        );
        let alpha_principal = Principal::try_new(
            PrincipalId::try_new("alpha-user").unwrap(),
            TenantId::try_new("alpha").unwrap(),
            PrincipalRole::User,
            ["alpha"],
        )
        .unwrap();
        let beta_principal = Principal::try_new(
            PrincipalId::try_new("beta-user").unwrap(),
            TenantId::try_new("beta").unwrap(),
            PrincipalRole::User,
            ["beta"],
        )
        .unwrap();
        let security = ServerSecurity::with_bearer_principals([
            BearerPrincipalBinding::new("alpha-token", alpha_principal).unwrap(),
            BearerPrincipalBinding::new("beta-token", beta_principal).unwrap(),
        ])
        .unwrap();
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let query_addr = free_addr();
        let server = tokio::spawn({
            let addr = query_addr.clone();
            let query = Arc::new(QueryEngine::new(meta, engine));
            let config = QueryServerConfig::new()
                .with_managed_stage(base.clone())
                .with_server_security(security);
            async move { lake_query::serve_with_config(query, &addr, config).await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let alpha = LakeClient::builder(format!("http://{query_addr}"))
            .with_bearer_token("alpha-token")
            .unwrap()
            .connect()
            .await
            .unwrap();
        let beta = LakeClient::builder(format!("http://{query_addr}"))
            .with_bearer_token("beta-token")
            .unwrap()
            .connect()
            .await
            .unwrap();
        assert_ne!(
            alpha.objects.stage_identity(),
            beta.objects.stage_identity()
        );
        let location = alpha
            .objects
            .put_reader(
                Box::pin(std::io::Cursor::new(b"tenant-owned-video".to_vec())),
                "video/mp4".to_owned(),
            )
            .await
            .unwrap();
        assert!(alpha.open(&location).await.is_ok());
        assert!(beta.open(&location).await.is_err());

        let alpha_wire = base
            .scope_to_tenant(&TenantId::try_new("alpha").unwrap())
            .to_wire()
            .unwrap();
        assert!(!String::from_utf8(alpha_wire).unwrap().contains("token"));
        server.abort();
    }

    #[tokio::test]
    async fn sdk_tls_bearer_roundtrip_reaches_secured_query_and_meta() {
        let root = tempdir().expect("fixture root");
        let certified =
            generate_simple_self_signed(vec!["localhost".to_owned()]).expect("test identity");
        let certificate = certified.cert.pem();
        let private_key = certified.key_pair.serialize_pem();
        let query_credential = "query-sdk-credential";
        let meta_credential = "query-to-meta-credential";

        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("meta")).expect("meta"));
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let table = TableRef::new("robots", "secure_episodes");
        metasrv
            .create_table(
                &table,
                TableLocation::new(
                    root.path()
                        .join("tables/secure-episodes.lance")
                        .to_string_lossy(),
                ),
                Arc::new(Schema::new(vec![
                    Field::new("episode_id", DataType::Utf8, false),
                    data_location_field("video", false),
                ])),
            )
            .await
            .expect("create table");

        let meta_addr = free_addr();
        let query_addr = free_addr();
        let meta_server_security = ServerSecurity::with_bearer_token(meta_credential)
            .expect("meta bearer")
            .with_tls_identity_pem(certificate.as_bytes(), private_key.as_bytes());
        let meta_client_security = ClientSecurity::new()
            .with_ca_certificate_pem(certificate.as_bytes().to_vec())
            .with_server_name("localhost")
            .with_bearer_token(meta_credential)
            .expect("meta client bearer");
        tokio::spawn({
            let addr = meta_addr.clone();
            let config = MetasrvServerConfig::new()
                .with_server_security(meta_server_security)
                .with_peer_security(meta_client_security.clone());
            async move { lake_metasrv::serve_with_config(metasrv, &addr, config).await }
        });

        let descriptor = ManagedStageDescriptor::local(
            root.path().join("objects").to_string_lossy().into_owned(),
        );
        let query_server_security = ServerSecurity::with_bearer_token(query_credential)
            .expect("query bearer")
            .with_tls_identity_pem(certificate.as_bytes(), private_key.as_bytes());
        tokio::spawn({
            let addr = query_addr.clone();
            let query = Arc::new(QueryEngine::new(meta, engine));
            let config = QueryServerConfig::new()
                .with_metadata(
                    format!(
                        "https://localhost:{}",
                        meta_addr.split(':').next_back().unwrap()
                    ),
                    meta_client_security,
                )
                .with_managed_stage(descriptor)
                .with_server_security(query_server_security);
            async move { lake_query::serve_with_config(query, &addr, config).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let client = LakeClient::builder(format!(
            "https://localhost:{}",
            query_addr.split(':').next_back().unwrap()
        ))
        .with_bearer_token(query_credential)
        .expect("SDK bearer")
        .with_ca_certificate_pem(certificate.as_bytes().to_vec())
        .with_server_name("localhost")
        .connect()
        .await
        .expect("secured SDK connect");
        let expected = b"secured direct object bytes";
        client
            .insert(
                "INSERT INTO robots.secure_episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("secure-episode".to_owned()),
                    InsertValue::File(FileUpload::from_reader(
                        std::io::Cursor::new(expected),
                        "video/mp4",
                    )),
                ],
            )
            .await
            .expect("secured FILE insert");
        let mut results = client
            .query("SELECT video FROM lake.robots.secure_episodes")
            .await
            .expect("secured SQL query");
        let batch = results.try_next().await.expect("stream").expect("batch");
        let location = data_location(&batch, "video", 0).expect("DataLocation");
        let mut reader = client.open(&location).await.expect("direct object reader");
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.expect("direct read");

        assert_eq!(actual, expected);
        assert!(location.uri.starts_with("file://"));
    }

    #[tokio::test]
    async fn sdk_queries_datalocation_and_opens_file() {
        let root = tempdir().unwrap();
        let (client, _metasrv, _table, _meta, _engine) = setup_client(root.path()).await;
        let source = root.path().join("episode.mp4");
        let expected = b"queried and opened through the public sdk";
        tokio::fs::write(&source, expected).await.unwrap();
        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-42".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                ],
            )
            .await
            .unwrap();

        let mut results = client
            .query("SELECT video FROM lake.robots.episodes")
            .await
            .unwrap();
        let batch = results.try_next().await.unwrap().unwrap();
        let location = data_location(&batch, "video", 0).unwrap();
        let mut reader = client.open(&location).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();

        assert_eq!(actual, expected);
        assert!(results.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn sdk_opens_range_from_queried_datalocation() {
        let root = tempdir().unwrap();
        let (client, _metasrv, _table, _meta, _engine) = setup_client(root.path()).await;
        let expected = b"0123456789abcdef";
        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-range".to_owned()),
                    InsertValue::File(FileUpload::from_reader(
                        std::io::Cursor::new(expected),
                        "video/mp4",
                    )),
                ],
            )
            .await
            .unwrap();
        let mut results = client
            .query("SELECT video FROM lake.robots.episodes")
            .await
            .unwrap();
        let batch = results.try_next().await.unwrap().unwrap();
        let location = data_location(&batch, "video", 0).unwrap();

        let mut reader = client.open_range(&location, 4..10).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();

        assert_eq!(actual, b"456789");
    }

    #[tokio::test]
    async fn client_accepts_managed_object_store_abstraction() {
        let root = tempdir().unwrap();
        let (client, _metasrv, _table, _meta, _engine) = setup_client(root.path()).await;
        let source = root.path().join("model.bin");
        let expected = b"managed store abstraction";
        tokio::fs::write(&source, expected).await.unwrap();

        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-store".to_owned()),
                    InsertValue::File(FileUpload::from_path(&source, "application/octet-stream")),
                ],
            )
            .await
            .unwrap();
        let mut results = client
            .query("SELECT video FROM lake.robots.episodes")
            .await
            .unwrap();
        let batch = results.try_next().await.unwrap().unwrap();
        let location = crate::data_location(&batch, "video", 0).unwrap();
        let mut reader = client.open(&location).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    #[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
    async fn sdk_file_insert_uses_s3_stage() {
        let Ok(endpoint) = std::env::var("LAKE_S3_ENDPOINT") else {
            return;
        };
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(endpoint)
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "localstack"))
            .force_path_style(true)
            .build();
        let s3 = aws_sdk_s3::Client::from_conf(config);
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let bucket = format!("lake-sdk-{suffix}");
        s3.create_bucket()
            .bucket(&bucket)
            .send()
            .await
            .expect("create LocalStack bucket");
        let stage = S3ObjectStore::new(s3, &bucket, "managed/files").expect("valid S3 stage");
        let root = tempdir().expect("temporary SDK fixture");
        let (client, _metasrv, _table, _meta, _engine) =
            setup_client_with_store(root.path(), stage).await;
        let expected = (0..(5 * 1024 * 1024 + 17))
            .map(|index| u8::try_from(index % 251).expect("bounded byte"))
            .collect::<Vec<_>>();

        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-s3".to_owned()),
                    InsertValue::File(FileUpload::from_reader(
                        std::io::Cursor::new(expected.clone()),
                        "video/mp4",
                    )),
                ],
            )
            .await
            .expect("SQL FILE insert over S3 stage");
        let mut results = client
            .query("SELECT video FROM lake.robots.episodes")
            .await
            .expect("query DataLocation");
        let batch = results
            .try_next()
            .await
            .expect("query stream")
            .expect("one batch");
        let location = data_location(&batch, "video", 0).expect("decode DataLocation");
        assert!(
            location
                .uri
                .starts_with(&format!("s3://{bucket}/managed/files/"))
        );
        let mut reader = client.open(&location).await.expect("direct S3 reader");
        let mut actual = Vec::new();
        reader
            .read_to_end(&mut actual)
            .await
            .expect("read S3 object");

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    #[ignore = "requires LocalStack S3; set LAKE_S3_ENDPOINT and run with --ignored"]
    async fn sdk_discovers_s3_stage_and_streams_directly_localstack() {
        let Ok(endpoint) = std::env::var("LAKE_S3_ENDPOINT") else {
            return;
        };
        let config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .endpoint_url(&endpoint)
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("test", "test", None, None, "localstack"))
            .force_path_style(true)
            .build();
        let s3 = aws_sdk_s3::Client::from_conf(config);
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let bucket = format!("lake-sdk-discovery-{suffix}");
        s3.create_bucket()
            .bucket(&bucket)
            .send()
            .await
            .expect("create LocalStack bucket");
        let descriptor = ManagedStageDescriptor::s3(
            &bucket,
            "managed/discovered",
            Some("us-east-1".to_owned()),
            Some(endpoint),
            true,
        );
        let root = tempdir().expect("temporary SDK fixture");
        let client = setup_client_with_descriptor(root.path(), descriptor).await;
        let expected = (0..(5 * 1024 * 1024 + 17))
            .map(|index| u8::try_from(index % 251).expect("bounded byte"))
            .collect::<Vec<_>>();

        client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-discovered-s3".to_owned()),
                    InsertValue::File(FileUpload::from_reader(
                        std::io::Cursor::new(expected.clone()),
                        "video/mp4",
                    )),
                ],
            )
            .await
            .expect("SQL FILE insert through discovered S3 stage");
        let mut results = client
            .query("SELECT video FROM lake.robots.episodes")
            .await
            .expect("query DataLocation");
        let batch = results
            .try_next()
            .await
            .expect("query stream")
            .expect("one batch");
        let location = data_location(&batch, "video", 0).expect("decode DataLocation");
        assert!(
            location
                .uri
                .starts_with(&format!("s3://{bucket}/managed/discovered/"))
        );
        let mut full_reader = client.open(&location).await.expect("direct S3 reader");
        let mut full = Vec::new();
        full_reader
            .read_to_end(&mut full)
            .await
            .expect("read S3 object");
        let mut range_reader = client
            .open_range(&location, 1024..2048)
            .await
            .expect("direct S3 range reader");
        let mut range = Vec::new();
        range_reader
            .read_to_end(&mut range)
            .await
            .expect("read S3 range");

        assert_eq!(full, expected);
        assert_eq!(range, expected[1024..2048]);
    }

    #[test]
    fn sdk_s3_stage_discovery_localstack_is_wired() {
        let integration = include_str!("../../../scripts/test-integration.ts");
        assert!(integration.contains("lake-sdk"));
        assert!(integration.contains("--run-ignored"));
    }

    #[test]
    fn sdk_file_insert_uses_s3_stage_is_wired() {
        let integration = include_str!("../../../scripts/test-integration.ts");
        assert!(integration.contains("lake-sdk"));
        assert!(integration.contains("--run-ignored"));
    }

    #[test]
    fn managed_file_example_queries_through_sdk() {
        let example = include_str!("../examples/managed_file.rs");

        assert!(example.contains("LakeClient::builder("));
        assert!(example.contains(".with_upload_checkpoint_dir("));
        assert!(!example.contains("connect_with_store"));
        assert!(example.contains(".query("));
        assert!(example.contains("data_location("));
        assert!(!example.contains(".execute_sql("));
    }

    #[test]
    fn managed_file_example_streams_direct_reads_to_sink() {
        let example = include_str!("../examples/managed_file.rs");

        assert!(example.contains("client.open(&location)"));
        assert!(example.contains("let mut sink = tokio::fs::File::create("));
        assert!(example.contains("tokio::io::copy(&mut reader, &mut sink)"));
        assert!(example.contains("assert_eq!(copied, location.size_bytes)"));
        assert!(!example.contains("read_to_end"));
    }

    #[tokio::test]
    async fn sdk_batch_insert_commits_multiple_files_as_one_version() {
        let root = tempdir().unwrap();
        let (client, metasrv, table, _meta, _engine) = setup_client(root.path()).await;
        let first = root.path().join("first.mp4");
        let second = root.path().join("second.mp4");
        tokio::fs::write(&first, b"first video bytes")
            .await
            .unwrap();
        tokio::fs::write(&second, b"second video bytes")
            .await
            .unwrap();

        let version = client
            .insert_many(
                "INSERT INTO robots.episodes (video, episode_id) VALUES (?, ?)",
                vec![
                    vec![
                        InsertValue::File(FileUpload::from_path(&first, "video/mp4")),
                        InsertValue::Utf8("episode-batch-1".to_owned()),
                    ],
                    vec![
                        InsertValue::File(FileUpload::from_path(&second, "video/mp4")),
                        InsertValue::Utf8("episode-batch-2".to_owned()),
                    ],
                ],
            )
            .await
            .unwrap();

        assert_eq!(version, Version(2));
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(2)
        );
        let mut results = client
            .query("SELECT episode_id, video FROM lake.robots.episodes ORDER BY episode_id")
            .await
            .unwrap();
        let batch = results.try_next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let expected = [
            b"first video bytes".as_slice(),
            b"second video bytes".as_slice(),
        ];
        for (row, bytes) in expected.into_iter().enumerate() {
            let location = data_location(&batch, "video", row).unwrap();
            let mut reader = client.open(&location).await.unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.unwrap();
            assert_eq!(actual, bytes);
        }
    }

    #[tokio::test]
    async fn sdk_batch_insert_validates_every_row_before_upload() {
        let root = tempdir().unwrap();
        let (client, metasrv, table, _meta, _engine) = setup_client(root.path()).await;
        let source = root.path().join("must-not-upload.mp4");
        tokio::fs::write(&source, b"must not upload").await.unwrap();

        let error = client
            .insert_many(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    vec![
                        InsertValue::Utf8("valid-first-row".to_owned()),
                        InsertValue::File(FileUpload::from_path(&source, "video/mp4")),
                    ],
                    vec![
                        InsertValue::Utf8("invalid-second-row".to_owned()),
                        InsertValue::Utf8("not a FILE".to_owned()),
                    ],
                ],
            )
            .await
            .unwrap_err();
        assert!(matches!(error, SdkError::TypeMismatch { .. }));
        assert_eq!(
            metasrv
                .resolve(&table)
                .await
                .unwrap()
                .unwrap()
                .current_version,
            Version(1)
        );
        assert_eq!(object_count(&root.path().join("objects")).await, 0);
    }

    #[tokio::test]
    async fn sdk_batch_insert_rejects_empty_and_excessive_batches() {
        let root = tempdir().unwrap();
        let client = LakeClient {
            query:                 tonic::transport::Endpoint::from_static("http://127.0.0.1:9")
                .connect_lazy(),
            objects:               Arc::new(
                LocalObjectStore::open(root.path().join("objects"))
                    .await
                    .unwrap(),
            ),
            security:              ClientSecurity::new(),
            schema_cache:          SchemaCache::new(SchemaCacheConfig::default()),
            upload_checkpoint_dir: None,
        };
        let sql = "INSERT INTO robots.episodes (episode_id) VALUES (?)";

        assert!(matches!(
            client.insert_many(sql, Vec::new()).await,
            Err(SdkError::BatchRowCount { actual: 0, .. })
        ));
        let excessive = (0..=MAX_INSERT_BATCH_ROWS).map(|_| Vec::new()).collect();
        assert!(matches!(
            client.insert_many(sql, excessive).await,
            Err(SdkError::BatchRowCount { actual, .. }) if actual == MAX_INSERT_BATCH_ROWS + 1
        ));
        let oversized = vec![vec![InsertValue::Utf8(
            "x".repeat(MAX_INSERT_INPUT_METADATA_BYTES + 1),
        )]];
        assert!(matches!(
            client.insert_many(sql, oversized).await,
            Err(SdkError::BatchMetadataSize { .. })
        ));
        assert_eq!(object_count(&root.path().join("objects")).await, 0);
    }

    #[tokio::test]
    async fn sdk_presigned_read_delegates_to_managed_store() {
        let seen = Arc::new(StdMutex::new(None));
        let overlong = Arc::new(AtomicBool::new(false));
        let client = LakeClient {
            query:                 tonic::transport::Endpoint::from_static("http://127.0.0.1:9")
                .connect_lazy(),
            objects:               Arc::new(SigningStore {
                seen:     seen.clone(),
                overlong: overlong.clone(),
            }),
            security:              ClientSecurity::new(),
            schema_cache:          SchemaCache::new(SchemaCacheConfig::default()),
            upload_checkpoint_dir: None,
        };
        let location = lake_common::DataLocation::builder()
            .uri("s3://managed/tenant/object")
            .content_type("video/mp4")
            .size_bytes(42)
            .sha256("unused")
            .build();

        assert!(matches!(
            client.presign_read(&location, Duration::ZERO).await,
            Err(SdkError::Object {
                source: lake_objects::ObjectError::InvalidPresignExpiration { .. },
            })
        ));
        assert!(seen.lock().unwrap().is_none());

        let capability = client
            .presign_read(&location, Duration::from_secs(90))
            .await
            .unwrap();

        assert_eq!(
            capability.url(),
            "https://example.invalid/redacted-capability"
        );
        assert_eq!(
            *seen.lock().unwrap(),
            Some((location.uri.clone(), Duration::from_secs(90)))
        );

        overlong.store(true, Ordering::SeqCst);
        assert!(matches!(
            client
                .presign_read(&location, Duration::from_secs(90))
                .await,
            Err(SdkError::Object {
                source: lake_objects::ObjectError::InvalidPresignedCapabilityLifetime,
            })
        ));
    }

    #[tokio::test]
    async fn sdk_open_verifies_datalocation_identity_without_query() {
        let expected = b"immutable managed object bytes";
        let opens = Arc::new(AtomicUsize::new(0));
        let client = LakeClient {
            query:                 tonic::transport::Endpoint::from_static("http://127.0.0.1:9")
                .connect_lazy(),
            objects:               Arc::new(StaticReadStore {
                bytes: expected.to_vec(),
                opens: opens.clone(),
            }),
            security:              ClientSecurity::new(),
            schema_cache:          SchemaCache::new(SchemaCacheConfig::default()),
            upload_checkpoint_dir: None,
        };
        let mut location = lake_common::DataLocation::builder()
            .uri("s3://managed/tenant/object")
            .content_type("application/octet-stream")
            .size_bytes(expected.len() as u64)
            .sha256(format!("{:x}", Sha256::digest(expected)))
            .build();

        let mut reader = client.open(&location).await.unwrap();
        let mut actual = Vec::new();
        reader.read_to_end(&mut actual).await.unwrap();
        assert_eq!(actual, expected);

        location.sha256 = format!("{:x}", Sha256::digest(vec![b'x'; expected.len()]));
        let mut reader = client.open(&location).await.unwrap();
        let mut corrupt = Vec::new();
        let error = reader.read_to_end(&mut corrupt).await.unwrap_err();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
        assert!(matches!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<crate::ObjectIntegrityError>()),
            Some(crate::ObjectIntegrityError::Sha256Mismatch { .. })
        ));
        assert_eq!(corrupt, expected);
        assert_eq!(opens.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn sdk_batch_insert_flight_bound_uses_protobuf_size() {
        let within = FlightData {
            data_body: vec![0; 28].into(),
            ..FlightData::default()
        };
        let maximum = within.encoded_len();
        validate_flight_payload_size(std::slice::from_ref(&within), maximum).unwrap();

        let over = FlightData {
            data_body: vec![0; 29].into(),
            ..FlightData::default()
        };
        assert!(over.encoded_len() > maximum);
        assert!(matches!(
            validate_flight_payload_size(&[over], maximum),
            Err(SdkError::BatchMetadataSize { actual, maximum: limit })
                if actual > limit
        ));
    }

    #[tokio::test]
    async fn failed_upload_does_not_publish_a_table_version() {
        let root = tempdir().unwrap();
        let (client, metasrv, table, _meta, _engine) = setup_client(root.path()).await;
        let before = metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version;

        let error = client
            .insert(
                "INSERT INTO robots.episodes (episode_id, video) VALUES (?, ?)",
                vec![
                    InsertValue::Utf8("episode-43".to_owned()),
                    InsertValue::File(FileUpload::from_reader(
                        FailingReader { emitted: false },
                        "video/mp4",
                    )),
                ],
            )
            .await;

        assert!(error.is_err(), "a missing source file must fail the INSERT");
        let after = metasrv
            .resolve(&table)
            .await
            .unwrap()
            .unwrap()
            .current_version;
        assert_eq!(
            after, before,
            "a failed upload must not append a table version"
        );
        let mut objects = tokio::fs::read_dir(root.path().join("objects"))
            .await
            .unwrap();
        assert!(
            objects.next_entry().await.unwrap().is_none(),
            "a failed upload must remove its staging object"
        );
    }

    #[tokio::test]
    async fn unsupported_insert_syntax_never_starts_an_upload() {
        let root = tempdir().unwrap();
        let (client, _metasrv, _table, _meta, _engine) = setup_client(root.path()).await;
        let source = root.path().join("episode.mp4");
        tokio::fs::write(&source, b"must not be uploaded")
            .await
            .unwrap();

        let error = client
            .insert(
                "INSERT INTO robots.episodes VALUES (?)",
                vec![InsertValue::File(FileUpload::from_path(
                    &source,
                    "video/mp4",
                ))],
            )
            .await;

        assert!(error.is_err(), "unsupported SQL must fail before upload");
        let mut objects = tokio::fs::read_dir(root.path().join("objects"))
            .await
            .unwrap();
        assert!(objects.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn sdk_async_query_roundtrip_uses_poll_flight_info() {
        assert_sdk_query_async_roundtrip().await;
    }

    #[tokio::test]
    async fn sdk_query_async_delegates_to_restart_safe_handle() {
        assert_sdk_query_async_roundtrip().await;
    }

    async fn assert_sdk_query_async_roundtrip() {
        let root = tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("catalog")).unwrap());
        let state: MetaStoreRef =
            Arc::new(RocksMeta::open(root.path().join("async-state")).unwrap());
        let results = Arc::new(
            LocalObjectStore::open(root.path().join("async-results"))
                .await
                .unwrap(),
        );
        let engine = Arc::new(QueryEngine::new(catalog, Arc::new(LanceEngine::new())));
        let address = free_addr();
        let server = tokio::spawn({
            let address = address.clone();
            async move {
                lake_query::serve_with_config(
                    engine,
                    &address,
                    QueryServerConfig::new().with_async_queries(
                        AsyncQueryConfig::new(state, results)
                            .with_scan_interval(Duration::from_millis(10)),
                    ),
                )
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let stage = LocalObjectStore::open(root.path().join("client-stage"))
            .await
            .unwrap();
        let client = LakeClient::connect_with_store(format!("http://{address}"), stage)
            .await
            .unwrap();
        let batches = client
            .query_async("SELECT CAST(42 AS BIGINT) AS answer")
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(batches.len(), 1);
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42);
        server.abort();
    }

    #[tokio::test]
    async fn sdk_resumes_async_query_after_client_restart() {
        let root = tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("catalog")).unwrap());
        let state: MetaStoreRef =
            Arc::new(RocksMeta::open(root.path().join("async-state")).unwrap());
        let results = Arc::new(
            LocalObjectStore::open(root.path().join("async-results"))
                .await
                .unwrap(),
        );
        let keys = QueryTicketKeyRing::try_new(
            b"sdk-resume-shared-ticket-key-material-000001",
            std::iter::empty(),
        )
        .unwrap();
        let first_address = free_addr();
        let second_address = free_addr();
        let first_server = tokio::spawn({
            let address = first_address.clone();
            let engine = Arc::new(QueryEngine::new(
                catalog.clone(),
                Arc::new(LanceEngine::new()),
            ));
            let config = QueryServerConfig::new()
                .with_ticket_keys(keys.clone())
                .with_async_queries(
                    AsyncQueryConfig::new(state.clone(), results.clone())
                        .with_scan_interval(Duration::from_millis(10)),
                );
            async move { lake_query::serve_with_config(engine, &address, config).await }
        });
        let second_server = tokio::spawn({
            let address = second_address.clone();
            let engine = Arc::new(QueryEngine::new(catalog, Arc::new(LanceEngine::new())));
            let config = QueryServerConfig::new()
                .with_ticket_keys(keys)
                .with_async_queries(
                    AsyncQueryConfig::new(state, results)
                        .with_scan_interval(Duration::from_millis(10)),
                );
            async move { lake_query::serve_with_config(engine, &address, config).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;

        let first_stage = LocalObjectStore::open(root.path().join("first-client-stage"))
            .await
            .unwrap();
        let first = LakeClient::connect_with_store(format!("http://{first_address}"), first_stage)
            .await
            .unwrap();
        let encoded = first
            .submit_async("SELECT CAST(42 AS BIGINT) AS answer")
            .await
            .unwrap()
            .to_json()
            .unwrap();
        drop(first);

        let second_stage = LocalObjectStore::open(root.path().join("second-client-stage"))
            .await
            .unwrap();
        let second =
            LakeClient::connect_with_store(format!("http://{second_address}"), second_stage)
                .await
                .unwrap();
        let batches = second
            .resume_async(crate::AsyncQueryHandle::from_json(&encoded).unwrap())
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let values = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 42);
        first_server.abort();
        second_server.abort();
    }

    #[tokio::test]
    async fn sdk_cancels_resumed_async_query_idempotently() {
        let root = tempdir().unwrap();
        let catalog: MetaStoreRef = Arc::new(RocksMeta::open(root.path().join("catalog")).unwrap());
        let state: MetaStoreRef =
            Arc::new(RocksMeta::open(root.path().join("async-state")).unwrap());
        let results = Arc::new(
            LocalObjectStore::open(root.path().join("async-results"))
                .await
                .unwrap(),
        );
        let engine = Arc::new(QueryEngine::new(catalog, Arc::new(LanceEngine::new())));
        let address = free_addr();
        let server = tokio::spawn({
            let address = address.clone();
            async move {
                lake_query::serve_with_config(
                    engine,
                    &address,
                    QueryServerConfig::new().with_async_queries(
                        AsyncQueryConfig::new(state, results)
                            .with_scan_interval(Duration::from_mins(1)),
                    ),
                )
                .await
            }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let stage = LocalObjectStore::open(root.path().join("client-stage"))
            .await
            .unwrap();
        let client = LakeClient::connect_with_store(format!("http://{address}"), stage)
            .await
            .unwrap();
        let encoded = client
            .submit_async("SELECT 1")
            .await
            .unwrap()
            .to_json()
            .unwrap();
        let restored = crate::AsyncQueryHandle::from_json(&encoded).unwrap();

        assert_eq!(
            client.cancel_async(&restored).await.unwrap(),
            CancelStatus::Cancelled
        );
        assert_eq!(
            client.cancel_async(&restored).await.unwrap(),
            CancelStatus::Cancelled
        );
        assert!(matches!(
            client.poll_async(&restored).await,
            Err(SdkError::Flight {
                source: FlightError::Tonic(status),
            }) if status.code() == tonic::Code::Cancelled
        ));
        server.abort();
    }

    #[test]
    fn async_query_handle_roundtrips_without_disclosing_payload() {
        let opaque = vec![0x5a; 192];
        let handle = crate::AsyncQueryHandle::try_new(opaque.clone(), 2_000_000_000).unwrap();
        let encoded = handle.to_json().unwrap();
        let restored = crate::AsyncQueryHandle::from_json(&encoded).unwrap();

        assert_eq!(restored, handle);
        assert_eq!(restored.expires_at_unix_seconds(), 2_000_000_000);
        let debug = format!("{handle:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("5a"));
        assert!(
            !encoded
                .windows(32)
                .any(|window| window.iter().all(|byte| *byte == b'Z'))
        );
        assert!(crate::AsyncQueryHandle::try_new(Vec::new(), 2_000_000_000).is_err());
        assert!(crate::AsyncQueryHandle::try_new(vec![0; 16 * 1024 + 1], 2_000_000_000).is_err());
    }

    fn free_addr() -> String {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().to_string()
    }

    async fn setup_client(
        root: &std::path::Path,
    ) -> (
        LakeClient,
        Arc<Metasrv>,
        TableRef,
        MetaStoreRef,
        TableEngineRef,
    ) {
        let objects = DelegatingStore(LocalObjectStore::open(root.join("objects")).await.unwrap());
        setup_client_with_store(root, objects).await
    }

    async fn setup_client_with_store<S>(
        root: &std::path::Path,
        objects: S,
    ) -> (
        LakeClient,
        Arc<Metasrv>,
        TableRef,
        MetaStoreRef,
        TableEngineRef,
    )
    where
        S: ManagedObjectStore + 'static,
    {
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let table = TableRef::new("robots", "episodes");
        let schema = Arc::new(Schema::new(vec![
            Field::new("episode_id", DataType::Utf8, false),
            data_location_field("video", false),
        ]));
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.join("tables/episodes.lance").to_string_lossy()),
                schema,
            )
            .await
            .unwrap();
        let meta_addr = free_addr();
        let query_addr = free_addr();
        tokio::spawn({
            let metasrv = metasrv.clone();
            let addr = meta_addr.clone();
            async move { lake_metasrv::serve(metasrv, &addr).await }
        });
        tokio::spawn({
            let query = Arc::new(QueryEngine::new(meta.clone(), engine.clone()));
            let addr = query_addr.clone();
            let metadata = format!("http://{meta_addr}");
            async move { lake_query::serve_with_metadata(query, &addr, &metadata).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        let client = LakeClient::connect_with_store(format!("http://{query_addr}"), objects)
            .await
            .unwrap();
        (client, metasrv, table, meta, engine)
    }

    async fn setup_client_with_descriptor(
        root: &std::path::Path,
        stage: ManagedStageDescriptor,
    ) -> LakeClient {
        let meta: MetaStoreRef = Arc::new(RocksMeta::open(root.join("meta")).unwrap());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let metasrv = Arc::new(Metasrv::new(meta.clone(), engine.clone()));
        let table = TableRef::new("robots", "episodes");
        metasrv
            .create_table(
                &table,
                TableLocation::new(root.join("tables/episodes.lance").to_string_lossy()),
                Arc::new(Schema::new(vec![
                    Field::new("episode_id", DataType::Utf8, false),
                    data_location_field("video", false),
                ])),
            )
            .await
            .unwrap();
        let meta_addr = free_addr();
        let query_addr = free_addr();
        tokio::spawn({
            let addr = meta_addr.clone();
            async move { lake_metasrv::serve(metasrv, &addr).await }
        });
        tokio::spawn({
            let query = Arc::new(QueryEngine::new(meta, engine));
            let addr = query_addr.clone();
            let metadata = format!("http://{meta_addr}");
            async move {
                lake_query::serve_with_metadata_and_stage(query, &addr, &metadata, stage).await
            }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        LakeClient::connect(format!("http://{query_addr}"))
            .await
            .unwrap()
    }

    struct DelegatingStore(LocalObjectStore);

    struct SigningStore {
        seen:     Arc<StdMutex<Option<(String, Duration)>>>,
        overlong: Arc<AtomicBool>,
    }

    struct StaticReadStore {
        bytes: Vec<u8>,
        opens: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ManagedObjectStore for StaticReadStore {
        async fn put_reader(
            &self,
            _input: ObjectReader,
            _content_type: String,
        ) -> ObjectResult<lake_common::DataLocation> {
            panic!("verified reads must not upload")
        }

        async fn open_reader(
            &self,
            _location: &lake_common::DataLocation,
        ) -> ObjectResult<ObjectReader> {
            self.opens.fetch_add(1, Ordering::SeqCst);
            Ok(Box::pin(std::io::Cursor::new(self.bytes.clone())))
        }

        async fn open_range(
            &self,
            _location: &lake_common::DataLocation,
            _range: std::ops::Range<u64>,
        ) -> ObjectResult<ObjectReader> {
            panic!("verified full reads must not use range I/O")
        }
    }

    #[async_trait::async_trait]
    impl ManagedObjectStore for SigningStore {
        async fn put_reader(
            &self,
            _input: ObjectReader,
            _content_type: String,
        ) -> ObjectResult<lake_common::DataLocation> {
            panic!("presigning must not upload")
        }

        async fn open_reader(
            &self,
            _location: &lake_common::DataLocation,
        ) -> ObjectResult<ObjectReader> {
            panic!("presigning must not GET the object")
        }

        async fn open_range(
            &self,
            _location: &lake_common::DataLocation,
            _range: std::ops::Range<u64>,
        ) -> ObjectResult<ObjectReader> {
            panic!("presigning must not range-GET the object")
        }

        async fn presign_read(
            &self,
            location: &lake_common::DataLocation,
            expires_in: Duration,
        ) -> ObjectResult<PresignedRead> {
            *self.seen.lock().unwrap() = Some((location.uri.clone(), expires_in));
            let extra = if self.overlong.load(Ordering::SeqCst) {
                Duration::from_mins(1)
            } else {
                Duration::ZERO
            };
            Ok(PresignedRead::new(
                "https://example.invalid/redacted-capability",
                Vec::new(),
                SystemTime::now() + expires_in + extra,
            ))
        }
    }

    struct PathRecordingStore {
        inner: LocalObjectStore,
        seen:  Arc<std::sync::Mutex<Option<PathBuf>>>,
    }

    #[async_trait::async_trait]
    impl ManagedObjectStore for PathRecordingStore {
        fn stage_identity(&self) -> String { "recording://stage".to_owned() }

        async fn put_reader(
            &self,
            input: ObjectReader,
            content_type: String,
        ) -> ObjectResult<lake_common::DataLocation> {
            self.inner.put_reader(input, content_type).await
        }

        async fn put_path(
            &self,
            path: PathBuf,
            content_type: String,
            checkpoint: Option<PathBuf>,
        ) -> ObjectResult<lake_common::DataLocation> {
            *self.seen.lock().expect("recording mutex") = checkpoint.clone();
            <LocalObjectStore as ManagedObjectStore>::put_path(
                &self.inner,
                path,
                content_type,
                checkpoint,
            )
            .await
        }

        async fn open_reader(
            &self,
            location: &lake_common::DataLocation,
        ) -> ObjectResult<ObjectReader> {
            Ok(Box::pin(self.inner.open_reader(location).await?))
        }

        async fn open_range(
            &self,
            location: &lake_common::DataLocation,
            range: std::ops::Range<u64>,
        ) -> ObjectResult<ObjectReader> {
            Ok(Box::pin(self.inner.open_range(location, range).await?))
        }
    }

    #[async_trait::async_trait]
    impl ManagedObjectStore for DelegatingStore {
        async fn put_reader(
            &self,
            input: ObjectReader,
            content_type: String,
        ) -> ObjectResult<lake_common::DataLocation> {
            self.0.put_reader(input, content_type).await
        }

        async fn open_reader(
            &self,
            location: &lake_common::DataLocation,
        ) -> ObjectResult<ObjectReader> {
            Ok(Box::pin(self.0.open_reader(location).await?))
        }

        async fn open_range(
            &self,
            location: &lake_common::DataLocation,
            range: std::ops::Range<u64>,
        ) -> ObjectResult<ObjectReader> {
            Ok(Box::pin(self.0.open_range(location, range).await?))
        }
    }

    struct FailingReader {
        emitted: bool,
    }

    impl AsyncRead for FailingReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            if !self.emitted {
                self.emitted = true;
                buf.put_slice(b"first chunk");
                return Poll::Ready(Ok(()));
            }
            Poll::Ready(Err(io::Error::other("source stream interrupted")))
        }
    }
}
