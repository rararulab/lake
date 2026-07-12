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

use std::{
    collections::BTreeMap,
    fmt,
    ops::Range,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use arrow::{
    array::{ArrayRef, StringArray, StructArray},
    datatypes::{DataType, Schema, SchemaRef},
    error::ArrowError,
    record_batch::RecordBatch,
};
use arrow_flight::{
    Action, FlightClient, FlightData, FlightDescriptor, PutResult, decode::FlightRecordBatchStream,
    encode::FlightDataEncoderBuilder, sql::client::FlightSqlServiceClient,
};
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Region;
use futures::TryStreamExt;
use lake_common::{
    AppendOperationId, DataLocation, FILE_APPEND_TYPE_URL, FileAppendRequest,
    MANAGED_STAGE_DISCOVERY_ACTION, ManagedStageBackend, ManagedStageDescriptor, TableRef, Version,
};
use lake_flight::{ClientSecurity, append_flight_payload_digest};
use lake_objects::{
    LocalObjectStore, ManagedObjectStore, ObjectReader, S3ObjectStore, data_location_array,
    data_location_field, data_location_from_array,
};
use moka::{future::Cache, ops::compute::Op};
use prost::Message;
use prost_types::Any;
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

fn ambiguous_append_error(error: &SdkError) -> bool {
    match error {
        SdkError::MissingAppendResult => true,
        SdkError::Flight { source }
            if matches!(
                source.as_ref(),
                arrow_flight::error::FlightError::Tonic(status) if matches!(status.code(),
                tonic::Code::Cancelled
                    | tonic::Code::Unknown
                    | tonic::Code::DeadlineExceeded
                    | tonic::Code::Internal
                    | tonic::Code::Unavailable
                )
            ) =>
        {
            true
        }
        SdkError::Flight { source }
            if matches!(
                source.as_ref(),
                arrow_flight::error::FlightError::ExternalError(_)
            ) =>
        {
            true
        }
        _ => false,
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

/// Errors raised by the typed Rust SDK.
#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum SdkError {
    #[snafu(display("unsupported INSERT SQL: {message}"))]
    InvalidSql { message: String },

    #[snafu(display("invalid SDK schema cache configuration: {message}"))]
    InvalidSchemaCacheConfig { message: String },

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
        #[snafu(source(from(arrow_flight::error::FlightError, Arc::new)))]
        source: Arc<arrow_flight::error::FlightError>,
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

    #[snafu(display("query result column '{column}' is missing"))]
    MissingResultColumn { column: String },

    #[snafu(display("query result column '{column}' is not a FILE value"))]
    InvalidFileColumn { column: String },

    #[snafu(display("query result row {row} is outside the batch of {rows} rows"))]
    RowOutOfBounds { row: usize, rows: usize },

    #[snafu(display("query returned an invalid FILE append version"))]
    InvalidAppendResult { source: serde_json::Error },

    #[snafu(display("managed object operation failed"))]
    Object { source: lake_objects::ObjectError },

    #[snafu(display("could not open FILE upload source {path:?}"))]
    SourceFile {
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("could not build INSERT record batch"))]
    Arrow {
        #[snafu(source(from(ArrowError, Arc::new)))]
        source: Arc<ArrowError>,
    },

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
            Self::AppendRetryExpired { pending, .. } => Some(pending),
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
    Flight(Arc<arrow_flight::error::FlightError>),
    Arrow(Arc<ArrowError>),
}

impl SchemaLoadError {
    fn into_sdk_error(self) -> SdkError {
        match self {
            Self::Flight(source) => SdkError::Flight { source },
            Self::Arrow(source) => SdkError::Arrow { source },
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

    /// Persist resumable path-upload checkpoints in this local directory.
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
        let pending = self.prepare_insert(sql, values).await?;
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
        let result = retry_ambiguous_append_with_window(
            || self.put_append_once(pending.messages.clone()),
            window,
        )
        .await;
        let result = match result {
            Ok(result) => result,
            Err(AppendRetryFailure::Sdk(error)) => return Err(error),
            Err(AppendRetryFailure::Expired) => {
                return Err(SdkError::AppendRetryExpired { window, pending });
            }
        };
        serde_json::from_slice(&result.app_metadata).context(InvalidAppendResultSnafu)
    }

    async fn prepare_insert(&self, sql: &str, values: Vec<InsertValue>) -> Result<PendingAppend> {
        let insert = parse_insert(sql)?;
        if insert.columns.len() != values.len() {
            return Err(SdkError::ParameterCount {
                expected: insert.columns.len(),
                actual:   values.len(),
            });
        }
        let schema = self.table_schema(&insert.table).await?;
        validate_bindings(&schema, &insert.columns, &values)?;

        let mut bindings = insert
            .columns
            .into_iter()
            .zip(values)
            .collect::<BTreeMap<_, _>>();
        let mut arrays = Vec::<ArrayRef>::with_capacity(schema.fields().len());
        for field in schema.fields() {
            let value = bindings
                .remove(field.name())
                .ok_or_else(|| SdkError::MissingColumn {
                    column: field.name().to_owned(),
                })?;
            arrays.push(self.upload_and_encode(field.data_type(), value).await?);
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
        Ok(PendingAppend {
            operation_id,
            messages,
        })
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

    /// Execute read-only SQL through the query endpoint and stream Arrow
    /// record batches as they arrive.
    pub async fn query(&self, sql: &str) -> Result<FlightRecordBatchStream> {
        let mut client = FlightSqlServiceClient::new(self.query.clone());
        self.security.apply_to_sql_client(&mut client);
        let info = client
            .execute(sql.to_owned(), None)
            .await
            .context(FlightSnafu)?;
        let endpoint = info
            .endpoint
            .into_iter()
            .next()
            .context(MissingQueryEndpointSnafu)?;
        let ticket = endpoint.ticket.context(MissingQueryTicketSnafu)?;
        client.do_get(ticket).await.context(FlightSnafu)
    }

    /// Open a direct storage reader for an immutable `DataLocation`.
    pub async fn open(&self, location: &DataLocation) -> Result<ObjectReader> {
        self.objects
            .open_reader(location)
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
            .map_err(|source| SchemaLoadError::Flight(Arc::new(source)))?;
        let schema =
            Schema::try_from(info).map_err(|source| SchemaLoadError::Arrow(Arc::new(source)))?;
        Ok(Arc::new(schema))
    }

    async fn upload_and_encode(
        &self,
        data_type: &DataType,
        value: InsertValue,
    ) -> Result<ArrayRef> {
        match (data_type, value) {
            (DataType::Utf8, InsertValue::Utf8(value)) => {
                Ok(Arc::new(StringArray::from(vec![value])))
            }
            (data_type, InsertValue::File(file))
                if data_type == data_location_field("ignored", false).data_type() =>
            {
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
                Ok(Arc::new(data_location_array(&[location])))
            }
            (..) => Err(SdkError::TypeMismatch {
                column: "bound value".to_owned(),
            }),
        }
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

#[cfg(test)]
mod tests {
    use std::{
        io,
        path::PathBuf,
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        task::{Context, Poll},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use arrow::{
        array::{Array, StringArray, StructArray},
        datatypes::{DataType, Field, Schema},
    };
    use arrow_flight::{
        FlightDescriptor, FlightInfo,
        flight_service_server::FlightServiceServer,
        sql::{CommandStatementQuery, SqlInfo, server::FlightSqlService},
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
    use lake_query::{QueryEngine, QueryServerConfig};
    use rcgen::generate_simple_self_signed;
    use sha2::{Digest, Sha256};
    use tempfile::tempdir;
    use tokio::{
        io::{AsyncRead, AsyncReadExt, ReadBuf},
        sync::Notify,
    };
    use tonic::{Request, Response, Status};

    use crate::{
        APPEND_RETRY_WINDOW, FileUpload, InsertValue, LakeClient, MAX_SCHEMA_CACHE_CAPACITY,
        MAX_SCHEMA_CACHE_TTL, SchemaCache, SchemaCacheConfig, SdkError, data_location,
        retry_ambiguous_append_with_window,
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
                            source: Arc::new(arrow_flight::error::FlightError::Tonic(Box::new(
                                tonic::Status::unavailable("response lost after commit"),
                            ))),
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
                source.as_ref(),
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
        let batches = query
            .execute_sql("SELECT episode_id, video FROM lake.robots.episodes")
            .await
            .unwrap();
        let episode_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(episode_ids.value(0), "episode-42");
        let locations = batches[0]
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
