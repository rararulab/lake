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

//! The query layer: stateless SQL compute.
//!
//! [`QueryEngine`] wires a DataFusion [`SessionContext`] to a
//! [`LakeCatalog`], so plain SQL over `lake.<namespace>.<table>` reads
//! straight from the storage engine's data files. It holds no durable state:
//! scale it by running many instances behind a load balancer. It caches the
//! catalog with bounded staleness and refresh coalescing, shielding the
//! metadata authority from the per-query hot path.
//!
//! `execute_sql` runs SQL in-process; [`serve`] exposes the same engine over
//! the Arrow Flight SQL wire (see `flight`).

mod flight;
mod telemetry;
mod ticket;

use std::{
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::{
    arrow::array::RecordBatch,
    catalog::{CatalogProvider, MemoryCatalogProvider, MemorySchemaProvider, SchemaProvider},
    dataframe::DataFrame,
    error::DataFusionError,
    execution::{memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder},
    prelude::{SQLOptions, SessionConfig, SessionContext},
};
use lake_catalog::{
    CatalogGeneration, CatalogRefreshHealth, LakeCatalog, ProviderLoadError, TableSnapshot,
};
use lake_common::{ManagedStageDescriptor, TableRef};
use lake_engine::TableEngineRef;
use lake_flight::{ClientSecurity, ServerSecurity};
use lake_meta::MetaStoreRef;
use snafu::{ResultExt, Snafu};
pub use ticket::{QueryTicketError, QueryTicketKeyRing};
use tokio_util::sync::CancellationToken;
use tonic_health::{ServingStatus, server::health_reporter};

use crate::flight::FlightSqlServiceImpl;

/// Maximum age of the in-memory catalog listing used on the query hot path.
const CATALOG_MAX_AGE: Duration = Duration::from_secs(5);
const FLIGHT_SERVICE_NAME: &str = "arrow.flight.protocol.FlightService";

fn read_only_sql_options() -> SQLOptions {
    SQLOptions::new()
        .with_allow_ddl(false)
        .with_allow_dml(false)
        .with_allow_statements(false)
}

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QueryError {
    #[snafu(display("catalog refresh failed"))]
    Refresh { source: lake_meta::MetaError },

    #[snafu(display("query execution failed: {source}"))]
    Execute { source: DataFusionError },

    #[snafu(display("failed to resolve snapshot for {table}: {source}"))]
    SnapshotResolution {
        table:  TableRef,
        source: lake_meta::MetaError,
    },

    #[snafu(display("table {table} has no pinnable incarnation"))]
    UnpinnableTable { table: TableRef },

    #[snafu(display("failed to load snapshot for {table}: {source}"))]
    SnapshotProvider {
        table:  TableRef,
        source: Arc<ProviderLoadError>,
    },

    #[snafu(display("invalid listen address {addr:?}"))]
    Address {
        addr:   String,
        source: std::net::AddrParseError,
    },

    #[snafu(display("Flight SQL server failed"))]
    Serve { source: tonic::transport::Error },

    #[snafu(display("invalid Flight security configuration"))]
    Security {
        source: lake_flight::FlightSecurityError,
    },

    #[snafu(display("invalid Query limits: {message}"))]
    InvalidLimits { message: String },

    #[snafu(display("invalid Query resources: {message}"))]
    InvalidResources { message: String },

    #[snafu(display("invalid Query statement-ticket configuration"))]
    InvalidTicketConfiguration,

    #[snafu(display("failed to initialize Query execution resources"))]
    Runtime { source: DataFusionError },

    #[snafu(display("failed to prepare Query spill directory {path:?}"))]
    SpillDirectory {
        path:   PathBuf,
        source: std::io::Error,
    },

    #[snafu(display("Flight SQL connections did not drain within {grace:?}"))]
    DrainTimeout { grace: Duration },

    #[snafu(display("Query background task '{task}' failed"))]
    BackgroundTask {
        task:   &'static str,
        source: tokio::task::JoinError,
    },
}

pub type Result<T> = std::result::Result<T, QueryError>;

/// Per-request row and batch bounds for Flight SQL metadata discovery.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DiscoveryLimits {
    max_rows:   usize,
    batch_rows: usize,
}

impl DiscoveryLimits {
    /// Validate finite, non-zero discovery bounds.
    pub fn try_new(max_rows: usize, batch_rows: usize) -> Result<Self> {
        for (valid, message) in [
            (max_rows > 0, "max_rows must be greater than zero"),
            (batch_rows > 0, "batch_rows must be greater than zero"),
            (
                batch_rows <= max_rows,
                "batch_rows must not exceed max_rows",
            ),
        ] {
            if !valid {
                return Err(QueryError::InvalidLimits {
                    message: message.to_owned(),
                });
            }
        }
        Ok(Self {
            max_rows,
            batch_rows,
        })
    }

    /// Maximum matching rows returned by one discovery request.
    #[must_use]
    pub const fn max_rows(&self) -> usize { self.max_rows }

    /// Maximum rows allocated in one discovery record batch.
    #[must_use]
    pub const fn batch_rows(&self) -> usize { self.batch_rows }
}

impl Default for DiscoveryLimits {
    fn default() -> Self {
        Self {
            max_rows:   10_000,
            batch_rows: 256,
        }
    }
}

/// Per-replica admission and request limits for stateless Query execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct QueryLimits {
    max_concurrent: usize,
    queue_wait:     Duration,
    execution_time: Duration,
    max_sql_bytes:  usize,
}

impl QueryLimits {
    /// Validate finite, non-zero Query limits.
    pub fn try_new(
        max_concurrent: usize,
        queue_wait: Duration,
        execution_time: Duration,
        max_sql_bytes: usize,
    ) -> Result<Self> {
        for (valid, message) in [
            (
                max_concurrent > 0,
                "max_concurrent must be greater than zero",
            ),
            (
                !queue_wait.is_zero(),
                "queue_wait must be greater than zero",
            ),
            (
                !execution_time.is_zero(),
                "execution_time must be greater than zero",
            ),
            (max_sql_bytes > 0, "max_sql_bytes must be greater than zero"),
        ] {
            if !valid {
                return Err(QueryError::InvalidLimits {
                    message: message.to_owned(),
                });
            }
        }
        Ok(Self {
            max_concurrent,
            queue_wait,
            execution_time,
            max_sql_bytes,
        })
    }

    #[must_use]
    pub const fn max_concurrent(&self) -> usize { self.max_concurrent }

    #[must_use]
    pub const fn queue_wait(&self) -> Duration { self.queue_wait }

    #[must_use]
    pub const fn execution_time(&self) -> Duration { self.execution_time }

    #[must_use]
    pub const fn max_sql_bytes(&self) -> usize { self.max_sql_bytes }
}

impl Default for QueryLimits {
    fn default() -> Self {
        Self {
            max_concurrent: 64,
            queue_wait:     Duration::from_millis(100),
            execution_time: Duration::from_mins(30),
            max_sql_bytes:  1024 * 1024,
        }
    }
}

const DEFAULT_QUERY_MEMORY_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_QUERY_SPILL_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_STATEMENT_TICKET_TTL: Duration = Duration::from_mins(5);
const STATEMENT_TICKET_AUDIENCE: &str = "lake-query-statement-v1";
const MIN_QUERY_MEMORY_BYTES: usize = 16 * 1024 * 1024;
const MIN_QUERY_SPILL_BYTES: u64 = 16 * 1024 * 1024;

/// Process-wide DataFusion execution resources for one Query replica.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QueryResources {
    memory_bytes: usize,
    spill_bytes:  u64,
    spill_root:   PathBuf,
}

impl QueryResources {
    /// Validate finite execution-memory and local-spill budgets.
    pub fn try_new(
        memory_bytes: usize,
        spill_bytes: u64,
        spill_root: impl Into<PathBuf>,
    ) -> Result<Self> {
        let spill_root = spill_root.into();
        for (valid, message) in [
            (
                memory_bytes >= MIN_QUERY_MEMORY_BYTES,
                "memory_bytes must be at least 16777216",
            ),
            (
                spill_bytes >= MIN_QUERY_SPILL_BYTES,
                "spill_bytes must be at least 16777216",
            ),
            (
                !spill_root.as_os_str().is_empty(),
                "spill_root must not be empty",
            ),
        ] {
            if !valid {
                return Err(QueryError::InvalidResources {
                    message: message.to_owned(),
                });
            }
        }
        Ok(Self {
            memory_bytes,
            spill_bytes,
            spill_root,
        })
    }

    #[must_use]
    pub const fn memory_bytes(&self) -> usize { self.memory_bytes }

    #[must_use]
    pub const fn spill_bytes(&self) -> u64 { self.spill_bytes }

    #[must_use]
    pub fn spill_root(&self) -> &Path { &self.spill_root }

    fn sort_spill_reservation_bytes(&self) -> usize {
        const MAX_RESERVATION: usize = 10 * 1024 * 1024;
        (self.memory_bytes / 32).min(MAX_RESERVATION)
    }
}

impl Default for QueryResources {
    fn default() -> Self {
        Self {
            memory_bytes: DEFAULT_QUERY_MEMORY_BYTES,
            spill_bytes:  DEFAULT_QUERY_SPILL_BYTES,
            spill_root:   std::env::temp_dir().join("lake-query-spill"),
        }
    }
}

/// Complete network configuration for one stateless Query server.
#[derive(Clone, Debug)]
pub struct QueryServerConfig {
    metadata_endpoint: Option<String>,
    metadata_security: ClientSecurity,
    managed_stage:     Option<ManagedStageDescriptor>,
    server_security:   ServerSecurity,
    allow_insecure:    bool,
    limits:            QueryLimits,
    discovery_limits:  DiscoveryLimits,
    ticket_keys:       Option<QueryTicketKeyRing>,
    ticket_ttl:        Duration,
    shutdown_grace:    Duration,
}

impl QueryServerConfig {
    /// Explicit loopback development configuration.
    #[must_use]
    pub fn new() -> Self {
        Self {
            metadata_endpoint: None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            server_security:   ServerSecurity::insecure(),
            allow_insecure:    false,
            limits:            QueryLimits::default(),
            discovery_limits:  DiscoveryLimits::default(),
            ticket_keys:       None,
            ticket_ttl:        DEFAULT_STATEMENT_TICKET_TTL,
            shutdown_grace:    Duration::from_secs(30),
        }
    }

    /// Configure stateless FILE append forwarding through Metasrv.
    #[must_use]
    pub fn with_metadata(mut self, endpoint: impl Into<String>, security: ClientSecurity) -> Self {
        self.metadata_endpoint = Some(endpoint.into());
        self.metadata_security = security;
        self
    }

    /// Advertise one immutable, credential-free managed stage.
    #[must_use]
    pub fn with_managed_stage(mut self, stage: ManagedStageDescriptor) -> Self {
        self.managed_stage = Some(stage);
        self
    }

    /// Authenticate inbound RPCs and optionally enable server TLS.
    #[must_use]
    pub fn with_server_security(mut self, security: ServerSecurity) -> Self {
        self.server_security = security;
        self
    }

    /// Explicit deployment escape hatch for service-mesh or isolated-network
    /// environments terminating security before Lake.
    #[must_use]
    pub const fn allow_insecure(mut self, allow: bool) -> Self {
        self.allow_insecure = allow;
        self
    }

    /// Apply immutable per-replica admission and request limits.
    #[must_use]
    pub const fn with_limits(mut self, limits: QueryLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Apply immutable metadata discovery row and batch bounds.
    #[must_use]
    pub const fn with_discovery_limits(mut self, limits: DiscoveryLimits) -> Self {
        self.discovery_limits = limits;
        self
    }

    /// Install the shared active/verification key ring used by every Query
    /// replica behind one Flight endpoint.
    #[must_use]
    pub fn with_ticket_keys(mut self, keys: QueryTicketKeyRing) -> Self {
        self.ticket_keys = Some(keys);
        self
    }

    /// Set statement-ticket validity. Values outside `1s..=1h` fail before
    /// the server warms the catalog or binds its listener.
    #[must_use]
    pub const fn with_ticket_ttl(mut self, ttl: Duration) -> Self {
        self.ticket_ttl = ttl;
        self
    }

    /// Bound tonic's drain window after shutdown begins.
    #[must_use]
    pub const fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }
}

impl Default for QueryServerConfig {
    fn default() -> Self { Self::new() }
}

/// A stateless SQL execution context over the lake catalog.
pub struct QueryEngine {
    ctx:     SessionContext,
    catalog: LakeCatalog,
}

impl QueryEngine {
    /// Build a query engine registering the lake catalog under `lake`.
    pub fn new(meta: MetaStoreRef, engine: TableEngineRef) -> Self {
        Self::try_with_resources(meta, engine, QueryResources::default())
            .expect("default Query resources must initialize")
    }

    /// Build a query engine with explicit process-wide execution resources.
    pub fn try_with_resources(
        meta: MetaStoreRef,
        engine: TableEngineRef,
        resources: QueryResources,
    ) -> Result<Self> {
        let catalog = LakeCatalog::new(meta, engine);
        let sort_spill_reservation_bytes = resources.sort_spill_reservation_bytes();
        std::fs::create_dir_all(&resources.spill_root).context(SpillDirectorySnafu {
            path: resources.spill_root.clone(),
        })?;
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::new(FairSpillPool::new(resources.memory_bytes)))
            .with_temp_file_path(resources.spill_root)
            .with_max_temp_directory_size(resources.spill_bytes)
            .build()
            .context(RuntimeSnafu)?;
        let session =
            SessionConfig::new().with_sort_spill_reservation_bytes(sort_spill_reservation_bytes);
        let ctx = SessionContext::new_with_config_rt(session, Arc::new(runtime));
        ctx.register_catalog("lake", Arc::new(catalog.clone()));
        Ok(Self { ctx, catalog })
    }

    /// Force a reload of the catalog's listing snapshot from the registry.
    pub async fn refresh(&self) -> Result<()> { self.catalog.refresh().await.context(RefreshSnafu) }

    /// Return process-local catalog refresh health without metadata I/O.
    #[must_use]
    pub fn catalog_refresh_health(&self) -> CatalogRefreshHealth { self.catalog.refresh_health() }

    /// Refresh the listing only after its bounded staleness window.
    pub(crate) async fn refresh_if_stale(&self) -> Result<()> {
        self.catalog
            .refresh_if_stale(CATALOG_MAX_AGE)
            .await
            .context(RefreshSnafu)
    }

    pub(crate) async fn invalidate_registration(&self, table: &lake_common::TableRef) {
        self.catalog.invalidate_registration(table).await;
    }

    /// Resolve each authorized SQL name to one immutable physical generation.
    pub(crate) async fn resolve_snapshots(
        &self,
        tables: &[TableRef],
    ) -> Result<Vec<TableSnapshot>> {
        self.refresh_if_stale().await?;
        let mut snapshots = Vec::with_capacity(tables.len());
        for table in tables {
            let snapshot = self
                .catalog
                .resolve_snapshot(table)
                .await
                .map_err(|source| QueryError::SnapshotResolution {
                    table: table.clone(),
                    source,
                })?
                .ok_or_else(|| QueryError::UnpinnableTable {
                    table: table.clone(),
                })?;
            snapshots.push(snapshot);
        }
        Ok(snapshots)
    }

    /// Plan SQL using only the exact immutable providers in `snapshots`.
    /// This path never resolves current registry pointers.
    pub(crate) async fn plan_sql_at(
        &self,
        sql: &str,
        snapshots: &[TableSnapshot],
    ) -> Result<DataFrame> {
        let ctx =
            SessionContext::new_with_config_rt(self.ctx.copied_config(), self.ctx.runtime_env());
        let catalog = Arc::new(MemoryCatalogProvider::new());
        ctx.register_catalog("lake", catalog.clone());
        for snapshot in snapshots {
            let namespace = &snapshot.table().namespace.0;
            let schema = if let Some(schema) = catalog.schema(namespace) {
                schema
            } else {
                let schema: Arc<dyn SchemaProvider> = Arc::new(MemorySchemaProvider::new());
                catalog
                    .register_schema(namespace, schema.clone())
                    .context(ExecuteSnafu)?;
                schema
            };
            let provider =
                self.catalog
                    .provider_for_snapshot(snapshot)
                    .await
                    .map_err(|source| QueryError::SnapshotProvider {
                        table: snapshot.table().clone(),
                        source,
                    })?;
            schema
                .register_table(snapshot.table().name.0.clone(), provider)
                .context(ExecuteSnafu)?;
        }
        ctx.sql_with_options(sql, read_only_sql_options())
            .await
            .context(ExecuteSnafu)
    }

    async fn shutdown_catalog_revalidation(&self) { self.catalog.shutdown_revalidation().await; }

    pub(crate) fn cached_catalog_generation(&self) -> Arc<CatalogGeneration> {
        self.catalog.cached_generation()
    }

    /// Execute a SQL statement and collect the results.
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        let df = self.plan_sql(sql).await?;
        df.collect().await.context(ExecuteSnafu)
    }

    /// Validate and plan a statement through the public read-only SQL surface.
    pub(crate) async fn plan_sql(&self, sql: &str) -> Result<DataFrame> {
        self.refresh_if_stale().await?;
        self.ctx
            .sql_with_options(sql, read_only_sql_options())
            .await
            .context(ExecuteSnafu)
    }

    pub(crate) fn context(&self) -> &SessionContext { &self.ctx }
}

/// Run the Arrow Flight SQL server, serving SQL from `engine` over `addr`.
///
/// Warms the catalog, then binds a tonic server exposing the Flight SQL
/// statement path. Runs until the server stops or the process is killed.
pub async fn serve(engine: Arc<QueryEngine>, addr: &str) -> Result<()> {
    serve_with_config(engine, addr, QueryServerConfig::new()).await
}

/// Run the Flight SQL server with stateless FILE-write forwarding.
///
/// `metadata_addr` is a complete tonic endpoint URI such as
/// `http://127.0.0.1:50052`.
pub async fn serve_with_metadata(
    engine: Arc<QueryEngine>,
    addr: &str,
    metadata_addr: &str,
) -> Result<()> {
    let config = QueryServerConfig::new().with_metadata(metadata_addr, ClientSecurity::new());
    serve_with_config(engine, addr, config).await
}

/// Run the Flight SQL server with FILE-write forwarding and managed-stage
/// discovery.
pub async fn serve_with_metadata_and_stage(
    engine: Arc<QueryEngine>,
    addr: &str,
    metadata_addr: &str,
    managed_stage: ManagedStageDescriptor,
) -> Result<()> {
    let config = QueryServerConfig::new()
        .with_metadata(metadata_addr, ClientSecurity::new())
        .with_managed_stage(managed_stage);
    serve_with_config(engine, addr, config).await
}

/// Run Query with an explicit production or loopback security configuration.
pub async fn serve_with_config(
    engine: Arc<QueryEngine>,
    addr: &str,
    config: QueryServerConfig,
) -> Result<()> {
    serve_with_config_and_shutdown(engine, addr, config, std::future::pending()).await
}

/// Run Query until `shutdown` fires, then cancel owned background work and
/// drain Flight connections within the configured grace period.
pub async fn serve_with_config_and_shutdown<F>(
    engine: Arc<QueryEngine>,
    addr: &str,
    config: QueryServerConfig,
    shutdown: F,
) -> Result<()>
where
    F: std::future::Future<Output = ()> + Send + 'static,
{
    let socket = addr.parse().context(AddressSnafu { addr })?;
    config
        .server_security
        .validate_exposure(socket, config.allow_insecure)
        .context(SecuritySnafu)?;
    let ticket_codec =
        statement_ticket_codec_for_listener(socket, config.ticket_keys, config.ticket_ttl)?;
    let mut server = tonic::transport::Server::builder();
    if let Some(tls) = config.server_security.tls_config() {
        server = server.tls_config(tls).context(ServeSnafu)?;
    }

    telemetry::describe();
    telemetry::ready(false);
    if let Err(error) = engine.refresh().await {
        telemetry::catalog_refresh("initial", "error");
        return Err(error);
    }
    telemetry::catalog_refresh("initial", "success");
    telemetry::ready(true);
    let (health_reporter, health_service) = health_reporter();
    health_reporter
        .set_service_status(FLIGHT_SERVICE_NAME, ServingStatus::Serving)
        .await;
    let cancellation = CancellationToken::new();
    let refresher = tokio::spawn(run_catalog_refresh_loop(
        engine.clone(),
        cancellation.clone(),
    ));
    let mut background = QueryBackgroundGuard {
        cancellation: cancellation.clone(),
        engine:       engine.clone(),
        refresher:    Some(refresher),
    };
    let service =
        FlightServiceServer::new(flight::TracedFlightSqlService::new(FlightSqlServiceImpl {
            engine: engine.clone(),
            metadata_addr: config.metadata_endpoint,
            metadata_security: config.metadata_security,
            managed_stage: config.managed_stage,
            admission: flight::QueryAdmission::new(config.limits),
            discovery_limits: config.discovery_limits,
            ticket_codec,
        }));

    tracing::info!(%addr, "Flight SQL server ready");
    let server_shutdown = cancellation.clone();
    let server = server
        .layer(tonic::service::InterceptorLayer::new(
            config.server_security.interceptor(),
        ))
        .add_service(service)
        .add_service(health_service)
        .serve_with_shutdown(socket, async move {
            server_shutdown.cancelled().await;
        });
    tokio::pin!(server);
    tokio::pin!(shutdown);

    let server_result = tokio::select! {
        result = &mut server => result.context(ServeSnafu),
        () = &mut shutdown => {
            telemetry::ready(false);
            health_reporter
                .set_service_status(FLIGHT_SERVICE_NAME, ServingStatus::NotServing)
                .await;
            health_reporter
                .set_service_status("", ServingStatus::NotServing)
                .await;
            cancellation.cancel();
            match tokio::time::timeout(config.shutdown_grace, &mut server).await {
                Ok(result) => result.context(ServeSnafu),
                Err(_) => Err(QueryError::DrainTimeout { grace: config.shutdown_grace }),
            }
        }
    };
    telemetry::ready(false);
    cancellation.cancel();
    engine.shutdown_catalog_revalidation().await;
    let refresher_result = background
        .refresher
        .take()
        .expect("background refresher exists")
        .await
        .map_err(|source| QueryError::BackgroundTask {
            task: "catalog-refresh",
            source,
        });
    server_result?;
    refresher_result?;
    Ok(())
}

fn statement_ticket_codec_for_listener(
    socket: SocketAddr,
    keys: Option<QueryTicketKeyRing>,
    ttl: Duration,
) -> Result<ticket::StatementTicketCodec> {
    let keys = match keys {
        Some(keys) => keys,
        None if socket.ip().is_loopback() => {
            QueryTicketKeyRing::ephemeral().map_err(|_| QueryError::InvalidTicketConfiguration)?
        }
        None => return Err(QueryError::InvalidTicketConfiguration),
    };
    ticket::StatementTicketCodec::try_new(keys, ttl, STATEMENT_TICKET_AUDIENCE)
        .map_err(|_| QueryError::InvalidTicketConfiguration)
}

struct QueryBackgroundGuard {
    cancellation: CancellationToken,
    engine:       Arc<QueryEngine>,
    refresher:    Option<tokio::task::JoinHandle<()>>,
}

impl Drop for QueryBackgroundGuard {
    fn drop(&mut self) {
        self.cancellation.cancel();
        self.engine.catalog.abort_revalidation();
        if let Some(refresher) = self.refresher.take() {
            refresher.abort();
        }
    }
}

async fn run_catalog_refresh_loop(engine: Arc<QueryEngine>, shutdown: CancellationToken) {
    let mut interval = tokio::time::interval(CATALOG_MAX_AGE);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    // Consume the immediate first tick: serve just warmed the catalog.
    interval.tick().await;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => return,
            _ = interval.tick() => {
                tokio::select! {
                    () = shutdown.cancelled() => return,
                    result = engine.refresh() => {
                        match result {
                            Ok(()) => telemetry::catalog_refresh("background", "success"),
                            Err(err) => {
                                telemetry::catalog_refresh("background", "error");
                                tracing::warn!(error = %err, "background catalog refresh failed");
                            }
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        time::Duration,
    };

    use arrow_flight::{
        Action, IpcMessage, SchemaAsIpc, flight_service_client::FlightServiceClient,
        sql::client::FlightSqlServiceClient,
    };
    use async_trait::async_trait;
    use datafusion::{
        arrow::{
            array::Int64Array,
            datatypes::{DataType, Field, Schema, SchemaRef},
            ipc::writer::IpcWriteOptions,
            record_batch::RecordBatch,
        },
        catalog::{TableProvider, streaming::StreamingTable},
        error::DataFusionError,
        execution::TaskContext,
        physical_plan::{
            SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
        },
    };
    use futures::{StreamExt, TryStreamExt};
    use lake_common::{
        AppendOperation, MANAGED_STAGE_DISCOVERY_ACTION, ManagedStageDescriptor, Principal,
        PrincipalId, PrincipalRole, TableLocation, TenantId, Version,
    };
    use lake_engine::{
        ObjectReferencePage, ObjectReferenceRequest, TableEngine, TableHandle, TableHandleRef,
    };
    use lake_engine_lance::LanceEngine;
    use lake_flight::{BearerPrincipalBinding, ClientSecurity, ServerSecurity};
    use lake_meta::{MetaStore, MetaStoreRef, registry::TableRegistration};
    use rcgen::generate_simple_self_signed;
    use tokio::sync::Notify;
    use tonic::Request;
    use tonic_health::pb::{
        HealthCheckRequest, health_check_response::ServingStatus as WireServingStatus,
        health_client::HealthClient,
    };

    use super::*;

    #[test]
    fn remote_query_requires_shared_ticket_keys_before_startup() {
        let remote = "0.0.0.0:50051".parse().unwrap();
        let loopback = "127.0.0.1:50051".parse().unwrap();
        let keys = QueryTicketKeyRing::try_new(
            b"shared-query-ticket-key-material-00001",
            std::iter::empty(),
        )
        .unwrap();

        assert!(matches!(
            statement_ticket_codec_for_listener(remote, None, DEFAULT_STATEMENT_TICKET_TTL),
            Err(QueryError::InvalidTicketConfiguration)
        ));
        assert!(
            statement_ticket_codec_for_listener(loopback, None, DEFAULT_STATEMENT_TICKET_TTL)
                .is_ok()
        );
        assert!(
            statement_ticket_codec_for_listener(
                remote,
                Some(keys.clone()),
                DEFAULT_STATEMENT_TICKET_TTL
            )
            .is_ok()
        );
        assert!(matches!(
            statement_ticket_codec_for_listener(remote, Some(keys), Duration::ZERO),
            Err(QueryError::InvalidTicketConfiguration)
        ));
    }

    #[derive(Default)]
    struct CountingMeta {
        scans: AtomicUsize,
    }

    struct PausingRefreshMeta {
        scans:   AtomicUsize,
        pause:   AtomicBool,
        entered: Notify,
        release: Notify,
    }

    struct ShutdownMeta {
        registration: Vec<u8>,
    }

    struct ShutdownEngine {
        location: TableLocation,
        provider: Arc<dyn TableProvider>,
    }

    struct ShutdownHandle {
        provider: Arc<dyn TableProvider>,
    }

    #[async_trait]
    impl TableEngine for ShutdownEngine {
        fn kind(&self) -> &'static str { "shutdown-test" }

        async fn create(
            &self,
            _location: &TableLocation,
            _schema: SchemaRef,
        ) -> lake_engine::Result<TableHandleRef> {
            unreachable!()
        }

        async fn open(
            &self,
            location: &TableLocation,
        ) -> lake_engine::Result<Option<TableHandleRef>> {
            Ok((location == &self.location).then(|| {
                Arc::new(ShutdownHandle {
                    provider: self.provider.clone(),
                }) as TableHandleRef
            }))
        }

        async fn remove(&self, _location: &TableLocation) -> lake_engine::Result<()> {
            unreachable!()
        }

        async fn maintain(
            &self,
            _location: &TableLocation,
            _version: Version,
        ) -> lake_engine::Result<Option<Version>> {
            unreachable!()
        }

        async fn retained_object_references(
            &self,
            _location: &TableLocation,
            _request: ObjectReferenceRequest,
        ) -> lake_engine::Result<ObjectReferencePage> {
            unreachable!()
        }
    }

    #[async_trait]
    impl TableHandle for ShutdownHandle {
        fn schema(&self) -> SchemaRef { self.provider.schema() }

        fn current_version(&self) -> Version { Version(1) }

        async fn table_provider(
            &self,
            version: Version,
        ) -> lake_engine::Result<Arc<dyn TableProvider>> {
            assert_eq!(version, Version(1));
            Ok(self.provider.clone())
        }

        async fn append(
            &self,
            _operation: &AppendOperation,
            _batches: SendableRecordBatchStream,
        ) -> lake_engine::Result<Version> {
            unreachable!()
        }

        async fn reconcile_append(
            &self,
            _operation: &AppendOperation,
        ) -> lake_engine::Result<Option<Version>> {
            unreachable!()
        }
    }

    #[async_trait]
    impl MetaStore for ShutdownMeta {
        async fn get(&self, key: &str) -> lake_meta::Result<Option<Vec<u8>>> {
            Ok((key == "tbl/robots/shutdown_stream").then(|| self.registration.clone()))
        }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            unreachable!()
        }

        async fn list_prefix(&self, prefix: &str) -> lake_meta::Result<Vec<String>> {
            Ok(if prefix == "tbl/" {
                vec!["robots/shutdown_stream".to_owned()]
            } else {
                Vec::new()
            })
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> {
            unreachable!()
        }
    }

    #[async_trait]
    impl MetaStore for PausingRefreshMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> { Ok(None) }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            unreachable!()
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            unreachable!()
        }

        async fn scan_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<(String, Vec<u8>)>> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            if self.pause.load(Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            Ok(Vec::new())
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> {
            unreachable!()
        }
    }

    #[derive(Debug)]
    struct ShutdownPartition {
        schema:  SchemaRef,
        release: Arc<Notify>,
    }

    impl PartitionStream for ShutdownPartition {
        fn schema(&self) -> &SchemaRef { &self.schema }

        fn execute(&self, _context: Arc<TaskContext>) -> SendableRecordBatchStream {
            let first = RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vec![1]))],
            )
            .expect("first batch");
            let second = RecordBatch::try_new(
                self.schema.clone(),
                vec![Arc::new(Int64Array::from(vec![2]))],
            )
            .expect("second batch");
            let release = self.release.clone();
            let batches = futures::stream::once(async move { Ok(first) }).chain(
                futures::stream::once(async move {
                    release.notified().await;
                    Ok::<_, DataFusionError>(second)
                }),
            );
            Box::pin(RecordBatchStreamAdapter::new(self.schema.clone(), batches))
        }
    }

    fn shutdown_query_engine(release: Arc<Notify>) -> Arc<QueryEngine> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let table = StreamingTable::try_new(
            schema.clone(),
            vec![Arc::new(ShutdownPartition { schema, release })],
        )
        .expect("streaming table");
        let location = TableLocation::new("mem://robots/shutdown-stream/incarnation");
        let IpcMessage(schema_ipc) = SchemaAsIpc::new(&table.schema(), &IpcWriteOptions::default())
            .try_into()
            .expect("encode shutdown schema");
        let registration = TableRegistration::new(
            location.clone(),
            "shutdown-test",
            Version(1),
            schema_ipc.to_vec(),
        );
        let meta: MetaStoreRef = Arc::new(ShutdownMeta {
            registration: serde_json::to_vec(&registration).expect("encode registration"),
        });
        let storage: TableEngineRef = Arc::new(ShutdownEngine {
            location,
            provider: Arc::new(table),
        });
        Arc::new(QueryEngine::new(meta, storage))
    }

    fn free_addr() -> std::net::SocketAddr {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port");
        listener.local_addr().expect("listen address")
    }

    async fn connect_sql(
        addr: std::net::SocketAddr,
    ) -> FlightSqlServiceClient<tonic::transport::Channel> {
        let endpoint = format!("http://{addr}");
        let channel = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(channel) = ClientSecurity::new().connect(endpoint.clone()).await {
                    break channel;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Query starts");
        FlightSqlServiceClient::new(channel)
    }

    async fn open_shutdown_stream(
        client: &mut FlightSqlServiceClient<tonic::transport::Channel>,
    ) -> arrow_flight::decode::FlightRecordBatchStream {
        let info = client
            .execute("SELECT * FROM lake.robots.shutdown_stream".to_owned(), None)
            .await
            .expect("FlightInfo");
        let ticket = info
            .endpoint
            .into_iter()
            .next()
            .expect("endpoint")
            .ticket
            .expect("ticket");
        client.do_get(ticket).await.expect("DoGet")
    }

    #[async_trait]
    impl MetaStore for CountingMeta {
        async fn get(&self, _key: &str) -> lake_meta::Result<Option<Vec<u8>>> { Ok(None) }

        async fn cas(
            &self,
            _key: &str,
            _expected: Option<&[u8]>,
            _new: &[u8],
        ) -> lake_meta::Result<bool> {
            Ok(true)
        }

        async fn list_prefix(&self, _prefix: &str) -> lake_meta::Result<Vec<String>> {
            self.scans.fetch_add(1, Ordering::Relaxed);
            Ok(Vec::new())
        }

        async fn delete(&self, _key: &str, _expected: &[u8]) -> lake_meta::Result<bool> { Ok(true) }
    }

    #[tokio::test]
    async fn repeated_queries_do_not_rescan_the_registry() {
        let meta = Arc::new(CountingMeta::default());
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let query = QueryEngine::new(meta_ref, engine);

        query.execute_sql("SELECT 1").await.unwrap();
        let after_first = meta.scans.load(Ordering::Relaxed);
        query.execute_sql("SELECT 2").await.unwrap();

        assert_eq!(after_first, 1, "the first query warms the listing cache");
        assert_eq!(
            meta.scans.load(Ordering::Relaxed),
            after_first,
            "a warm catalog must shield the registry from the SQL hot path"
        );
    }

    #[tokio::test]
    async fn warm_sql_planning_continues_during_slow_catalog_refresh() {
        tokio::time::pause();
        let meta = Arc::new(PausingRefreshMeta {
            scans:   AtomicUsize::new(0),
            pause:   AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let meta_ref: MetaStoreRef = meta.clone();
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let query = Arc::new(QueryEngine::new(meta_ref, engine));
        query.execute_sql("SELECT 1").await.unwrap();
        tokio::time::advance(CATALOG_MAX_AGE + Duration::from_millis(1)).await;
        meta.pause.store(true, Ordering::SeqCst);
        let entered = meta.entered.notified();
        let planner = {
            let query = query.clone();
            tokio::spawn(async move { query.execute_sql("SELECT 2").await })
        };

        entered.await;
        assert!(query.catalog_refresh_health().refreshing());
        tokio::task::yield_now().await;
        assert!(
            planner.is_finished(),
            "warm SQL planning must not await the paused authority scan"
        );
        meta.release.notify_one();
        planner.await.unwrap().unwrap();
        assert_eq!(meta.scans.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn public_sql_surface_is_read_only() {
        let meta: MetaStoreRef = Arc::new(CountingMeta::default());
        let engine: TableEngineRef = Arc::new(LanceEngine::new());
        let query = QueryEngine::new(meta, engine);

        query.execute_sql("SELECT 1").await.unwrap();
        query.execute_sql("EXPLAIN SELECT 1").await.unwrap();

        query
            .context()
            .sql("CREATE TABLE sink (value BIGINT)")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let dml = query
            .execute_sql("INSERT INTO sink VALUES (1)")
            .await
            .unwrap_err();
        assert!(
            dml.to_string().contains("DML not supported"),
            "public SQL must reject data mutation: {dml}"
        );

        let ddl = query
            .execute_sql(
                "CREATE EXTERNAL TABLE arbitrary STORED AS PARQUET LOCATION \
                 's3://untrusted-bucket/private/'",
            )
            .await
            .unwrap_err();
        assert!(
            ddl.to_string().contains("DDL not supported"),
            "public SQL must reject arbitrary object-store registration: {ddl}"
        );
    }

    #[tokio::test]
    async fn secured_query_rejects_anonymous_discovery() {
        let secret = "query-test-credential";
        let certified =
            generate_simple_self_signed(vec!["localhost".to_owned()]).expect("test identity");
        let certificate = certified.cert.pem();
        let private_key = certified.key_pair.serialize_pem();
        let server_security = ServerSecurity::with_bearer_token(secret)
            .expect("bearer")
            .with_tls_identity_pem(certificate.as_bytes(), private_key.as_bytes());
        let descriptor = ManagedStageDescriptor::local("/tmp/lake-secured-stage");
        let meta: MetaStoreRef = Arc::new(CountingMeta::default());
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta, storage));

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port");
        let addr = listener.local_addr().expect("listen address");
        drop(listener);
        let config = QueryServerConfig::new()
            .with_managed_stage(descriptor.clone())
            .with_server_security(server_security);
        let server =
            tokio::spawn(async move { serve_with_config(engine, &addr.to_string(), config).await });
        tokio::time::sleep(Duration::from_millis(100)).await;

        let endpoint = format!("https://localhost:{}", addr.port());
        let transport = ClientSecurity::new()
            .with_ca_certificate_pem(certificate.as_bytes().to_vec())
            .with_server_name("localhost");
        let channel = transport
            .connect(endpoint.clone())
            .await
            .expect("TLS connect");
        let mut anonymous = FlightServiceClient::new(channel);
        let action = Action {
            r#type: MANAGED_STAGE_DISCOVERY_ACTION.to_owned(),
            body:   Vec::new().into(),
        };
        let error = anonymous
            .do_action(Request::new(action.clone()))
            .await
            .expect_err("anonymous request rejected");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);

        let authenticated = transport.with_bearer_token(secret).expect("client bearer");
        let channel = authenticated.connect(endpoint).await.expect("TLS connect");
        let mut client = FlightServiceClient::new(channel);
        let results = client
            .do_action(authenticated.authorize_request(Request::new(action)))
            .await
            .expect("authenticated discovery")
            .into_inner()
            .try_collect::<Vec<_>>()
            .await
            .expect("discovery results");
        assert_eq!(results.len(), 1);
        assert_eq!(
            ManagedStageDescriptor::from_wire(&results[0].body).expect("descriptor"),
            descriptor.scope_to_tenant(&TenantId::try_new("deployment").unwrap())
        );

        server.abort();
    }

    #[tokio::test]
    async fn tls_statement_ticket_rejects_cross_principal_replay() {
        let alice_token = "alice-query-token";
        let bob_token = "bob-query-token";
        let principal = |id: &str| {
            Principal::try_new(
                PrincipalId::try_new(id).unwrap(),
                TenantId::try_new("tenant-alpha").unwrap(),
                PrincipalRole::User,
                ["alpha"],
            )
            .unwrap()
        };
        let certified = generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let certificate = certified.cert.pem();
        let private_key = certified.key_pair.serialize_pem();
        let server_security = ServerSecurity::with_bearer_principals([
            BearerPrincipalBinding::new(alice_token, principal("alice")).unwrap(),
            BearerPrincipalBinding::new(bob_token, principal("bob")).unwrap(),
        ])
        .unwrap()
        .with_tls_identity_pem(certificate.as_bytes(), private_key.as_bytes());
        let ticket_keys = QueryTicketKeyRing::try_new(
            b"shared-tls-ticket-key-material-000001",
            std::iter::empty(),
        )
        .unwrap();
        let engine = Arc::new(QueryEngine::new(
            Arc::new(CountingMeta::default()),
            Arc::new(LanceEngine::new()),
        ));
        let addr = free_addr();
        let config = QueryServerConfig::new()
            .with_server_security(server_security)
            .with_ticket_keys(ticket_keys);
        let server =
            tokio::spawn(async move { serve_with_config(engine, &addr.to_string(), config).await });

        let endpoint = format!("https://localhost:{}", addr.port());
        let transport = ClientSecurity::new()
            .with_ca_certificate_pem(certificate.as_bytes().to_vec())
            .with_server_name("localhost");
        let alice_security = transport.clone().with_bearer_token(alice_token).unwrap();
        let alice_channel = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(channel) = alice_security.connect(endpoint.clone()).await {
                    break channel;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Query starts");
        let mut alice = FlightSqlServiceClient::new(alice_channel);
        alice_security.apply_to_sql_client(&mut alice);
        let info = alice.execute("SELECT 1".to_owned(), None).await.unwrap();
        let ticket = info.endpoint[0].ticket.clone().unwrap();
        assert!(!ticket.ticket.windows(8).any(|window| window == b"SELECT 1"));

        let bob_security = transport.with_bearer_token(bob_token).unwrap();
        let bob_channel = bob_security.connect(endpoint).await.unwrap();
        let mut bob = FlightSqlServiceClient::new(bob_channel);
        bob_security.apply_to_sql_client(&mut bob);
        let replay = bob
            .do_get(ticket.clone())
            .await
            .expect_err("cross-principal replay must fail");
        let replay: tonic::Status = replay.into();
        assert_eq!(replay.code(), tonic::Code::Unauthenticated);
        assert_eq!(replay.message(), "invalid statement ticket");

        assert!(alice.do_get(ticket).await.is_ok());
        server.abort();
    }

    #[tokio::test]
    async fn query_grpc_health_requires_auth_and_reports_serving() {
        let secret = "query-health-credential";
        let certified =
            generate_simple_self_signed(vec!["localhost".to_owned()]).expect("test identity");
        let certificate = certified.cert.pem();
        let private_key = certified.key_pair.serialize_pem();
        let server_security = ServerSecurity::with_bearer_token(secret)
            .expect("bearer")
            .with_tls_identity_pem(certificate.as_bytes(), private_key.as_bytes());
        let engine = Arc::new(QueryEngine::new(
            Arc::new(CountingMeta::default()),
            Arc::new(LanceEngine::new()),
        ));
        let addr = free_addr();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_with_config_and_shutdown(
                engine,
                &addr.to_string(),
                QueryServerConfig::new().with_server_security(server_security),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });
        let endpoint = format!("https://localhost:{}", addr.port());
        let transport = ClientSecurity::new()
            .with_ca_certificate_pem(certificate.as_bytes().to_vec())
            .with_server_name("localhost");
        let channel = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(channel) = transport.connect(endpoint.clone()).await {
                    break channel;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Query starts");
        let error = HealthClient::new(channel)
            .check(Request::new(HealthCheckRequest {
                service: String::new(),
            }))
            .await
            .expect_err("anonymous health rejected");
        assert_eq!(error.code(), tonic::Code::Unauthenticated);

        let authenticated = transport.with_bearer_token(secret).expect("client bearer");
        let channel = authenticated.connect(endpoint).await.expect("TLS connect");
        let mut client = HealthClient::new(channel);
        for service in ["", FLIGHT_SERVICE_NAME] {
            let response = client
                .check(
                    authenticated.authorize_request(Request::new(HealthCheckRequest {
                        service: service.to_owned(),
                    })),
                )
                .await
                .expect("authenticated health")
                .into_inner();
            assert_eq!(response.status, WireServingStatus::Serving as i32);
        }

        shutdown_tx.send(()).expect("trigger shutdown");
        server
            .await
            .expect("serve task")
            .expect("graceful shutdown");
    }

    #[tokio::test]
    async fn query_health_watch_observes_not_serving_on_shutdown() {
        let engine = Arc::new(QueryEngine::new(
            Arc::new(CountingMeta::default()),
            Arc::new(LanceEngine::new()),
        ));
        let addr = free_addr();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_with_config_and_shutdown(
                engine,
                &addr.to_string(),
                QueryServerConfig::new().with_shutdown_grace(Duration::from_secs(1)),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });
        let channel = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(channel) = ClientSecurity::new()
                    .connect(format!("http://{addr}"))
                    .await
                {
                    break channel;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Query starts");
        let mut stream = HealthClient::new(channel)
            .watch(Request::new(HealthCheckRequest {
                service: FLIGHT_SERVICE_NAME.to_owned(),
            }))
            .await
            .expect("health watch")
            .into_inner();
        assert_eq!(
            stream
                .message()
                .await
                .expect("watch message")
                .expect("initial")
                .status,
            WireServingStatus::Serving as i32
        );

        shutdown_tx.send(()).expect("trigger shutdown");
        assert_eq!(
            tokio::time::timeout(Duration::from_millis(500), stream.message())
                .await
                .expect("withdrawal deadline")
                .expect("watch message")
                .expect("withdrawal")
                .status,
            WireServingStatus::NotServing as i32
        );
        drop(stream);
        server
            .await
            .expect("serve task")
            .expect("graceful shutdown");
    }

    #[tokio::test]
    async fn query_shutdown_releases_listener_and_joins_refresher() {
        let meta = Arc::new(CountingMeta::default());
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta_ref, storage));
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("ephemeral port");
        let addr = listener.local_addr().expect("listen address");
        drop(listener);
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_with_config_and_shutdown(
                engine,
                &addr.to_string(),
                QueryServerConfig::new().with_shutdown_grace(Duration::from_millis(250)),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });

        let endpoint = format!("http://{addr}");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if ClientSecurity::new()
                    .connect(endpoint.clone())
                    .await
                    .is_ok()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("Query starts");
        shutdown_tx.send(()).expect("trigger shutdown");
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("serve returns")
            .expect("serve task")
            .expect("graceful shutdown");

        let rebound = std::net::TcpListener::bind(addr).expect("listen address released");
        drop(rebound);
        assert_eq!(
            Arc::strong_count(&meta),
            1,
            "serve must join the refresher and drop its QueryEngine"
        );
    }

    #[tokio::test]
    async fn startup_configuration_failure_does_not_leak_refresher() {
        let meta = Arc::new(CountingMeta::default());
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta_ref, storage));

        let error = serve_with_config_and_shutdown(
            engine.clone(),
            "not-a-socket-address",
            QueryServerConfig::new(),
            std::future::pending(),
        )
        .await
        .unwrap_err();
        assert!(matches!(error, QueryError::Address { .. }));
        drop(engine);
        tokio::task::yield_now().await;

        assert_eq!(
            Arc::strong_count(&meta),
            1,
            "fallible startup must finish before spawning the refresher"
        );
    }

    #[tokio::test]
    async fn aborting_server_future_releases_refresh_tasks() {
        let meta = Arc::new(PausingRefreshMeta {
            scans:   AtomicUsize::new(0),
            pause:   AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        });
        let meta_ref: MetaStoreRef = meta.clone();
        let storage: TableEngineRef = Arc::new(LanceEngine::new());
        let engine = Arc::new(QueryEngine::new(meta_ref, storage));
        let addr = free_addr();
        let server_engine = engine.clone();
        let server = tokio::spawn(async move {
            serve_with_config_and_shutdown(
                server_engine,
                &addr.to_string(),
                QueryServerConfig::new(),
                std::future::pending(),
            )
            .await
        });
        let _client = connect_sql(addr).await;
        meta.pause.store(true, Ordering::SeqCst);
        let entered = meta.entered.notified();
        engine
            .catalog
            .refresh_if_stale(Duration::ZERO)
            .await
            .unwrap();
        entered.await;

        server.abort();
        let _ = server.await;
        drop(engine);
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }

        assert_eq!(
            Arc::strong_count(&meta),
            1,
            "aborting serve must cancel scheduled and request-triggered refresh tasks"
        );
    }

    #[tokio::test]
    async fn query_shutdown_drains_inflight_stream_within_grace() {
        let release = Arc::new(Notify::new());
        let engine = shutdown_query_engine(release.clone());
        let addr = free_addr();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_with_config_and_shutdown(
                engine,
                &addr.to_string(),
                QueryServerConfig::new().with_shutdown_grace(Duration::from_millis(500)),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });
        let mut client = connect_sql(addr).await;
        let mut stream = open_shutdown_stream(&mut client).await;
        assert!(stream.try_next().await.expect("first batch").is_some());

        shutdown_tx.send(()).expect("trigger shutdown");
        tokio::time::sleep(Duration::from_millis(25)).await;
        release.notify_waiters();
        assert!(stream.try_next().await.expect("second batch").is_some());
        assert!(
            stream
                .try_next()
                .await
                .expect("stream completion")
                .is_none()
        );
        drop(stream);
        drop(client);
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("serve returns")
            .expect("serve task")
            .expect("drained inside grace");
    }

    #[tokio::test]
    async fn query_shutdown_reports_drain_timeout() {
        let release = Arc::new(Notify::new());
        let engine = shutdown_query_engine(release.clone());
        let addr = free_addr();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            serve_with_config_and_shutdown(
                engine,
                &addr.to_string(),
                QueryServerConfig::new().with_shutdown_grace(Duration::from_millis(50)),
                async move {
                    let _ = shutdown_rx.await;
                },
            )
            .await
        });
        let mut client = connect_sql(addr).await;
        let mut stream = open_shutdown_stream(&mut client).await;
        assert!(stream.try_next().await.expect("first batch").is_some());

        shutdown_tx.send(()).expect("trigger shutdown");
        let error = tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .expect("serve returns")
            .expect("serve task")
            .expect_err("stuck stream exceeds grace");
        assert!(matches!(error, QueryError::DrainTimeout { .. }));
        let rebound = std::net::TcpListener::bind(addr).expect("listen address released");
        drop(rebound);
        release.notify_waiters();
    }

    #[test]
    fn query_resources_reject_invalid_budgets() {
        let spill_root = tempfile::tempdir().expect("spill root");
        assert!(QueryResources::try_new(0, 1024, spill_root.path()).is_err());
        assert!(QueryResources::try_new(1024, 0, spill_root.path()).is_err());

        let file = tempfile::NamedTempFile::new().expect("spill root file");
        let resources = QueryResources::try_new(16 * 1024 * 1024, 16 * 1024 * 1024, file.path())
            .expect("valid budgets");
        let result = QueryEngine::try_with_resources(
            Arc::new(CountingMeta::default()),
            Arc::new(LanceEngine::new()),
            resources,
        );
        assert!(matches!(result, Err(QueryError::SpillDirectory { .. })));
    }

    #[test]
    fn query_engine_uses_bounded_fair_spill_runtime() {
        let spill_root = tempfile::tempdir().expect("spill root");
        let resources =
            QueryResources::try_new(16 * 1024 * 1024, 16 * 1024 * 1024, spill_root.path())
                .expect("valid resources");
        let engine = QueryEngine::try_with_resources(
            Arc::new(CountingMeta::default()),
            Arc::new(LanceEngine::new()),
            resources.clone(),
        )
        .expect("bounded Query runtime");
        let runtime = engine.context().runtime_env();

        assert!(format!("{:?}", runtime.memory_pool).starts_with("FairSpillPool"));
        assert!(matches!(
            runtime.memory_pool.memory_limit(),
            datafusion::execution::memory_pool::MemoryLimit::Finite(limit)
                if limit == resources.memory_bytes()
        ));
        assert_eq!(
            runtime.disk_manager.max_temp_directory_size(),
            resources.spill_bytes()
        );
        assert!(
            runtime
                .disk_manager
                .temp_dir_paths()
                .iter()
                .all(|path| path.starts_with(spill_root.path()))
        );
    }

    #[tokio::test]
    async fn memory_intensive_sort_spills_and_cleans_up() {
        use datafusion::{
            arrow::array::StringArray,
            datasource::MemTable,
            physical_plan::{ExecutionPlan, collect},
        };

        fn spill_count(plan: &Arc<dyn ExecutionPlan>) -> usize {
            let own = plan
                .metrics()
                .and_then(|metrics| metrics.spill_count())
                .unwrap_or(0);
            own + plan.children().into_iter().map(spill_count).sum::<usize>()
        }

        let spill_root = tempfile::tempdir().expect("spill root");
        let resources =
            QueryResources::try_new(16 * 1024 * 1024, 64 * 1024 * 1024, spill_root.path())
                .expect("valid resources");
        let engine = QueryEngine::try_with_resources(
            Arc::new(CountingMeta::default()),
            Arc::new(LanceEngine::new()),
            resources,
        )
        .expect("bounded Query runtime");
        let runtime = engine.context().runtime_env();
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_sort_spill_reservation_bytes(512 * 1024),
            runtime.clone(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batches = (0..256)
            .map(|batch| {
                let values = (0..1024)
                    .rev()
                    .map(|row| format!("{:08}-{}", batch * 1024 + row, "x".repeat(64)))
                    .collect::<Vec<_>>();
                RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])
                    .expect("input batch")
            })
            .collect::<Vec<_>>();
        context
            .register_table(
                "spill_input",
                Arc::new(MemTable::try_new(schema, vec![batches]).expect("memory table")),
            )
            .expect("register input");

        let frame = context
            .sql("SELECT value FROM spill_input ORDER BY value")
            .await
            .expect("plan sort");
        let plan = frame.create_physical_plan().await.expect("physical sort");
        let result = collect(plan.clone(), context.task_ctx())
            .await
            .expect("external sort");
        assert_eq!(
            result.iter().map(RecordBatch::num_rows).sum::<usize>(),
            256 * 1024
        );
        let mut previous = None::<String>;
        for batch in &result {
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("sorted string column");
            for value in values.iter().flatten() {
                if let Some(previous) = previous.as_deref() {
                    assert!(previous <= value, "sort result must be ordered");
                }
                previous = Some(value.to_owned());
            }
        }
        assert!(
            spill_count(&plan) > 0,
            "sort must spill under the pool budget"
        );
        assert_eq!(runtime.memory_pool.reserved(), 0);
        assert_eq!(runtime.disk_manager.spilling_progress().current_bytes, 0);
        assert!(runtime.disk_manager.temp_dir_paths().iter().all(|path| {
            std::fs::read_dir(path)
                .expect("read DataFusion spill directory")
                .next()
                .is_none()
        }));

        drop(result);
        drop(plan);
        drop(context);
        drop(engine);
        drop(runtime);
        assert!(
            std::fs::read_dir(spill_root.path())
                .expect("read spill root")
                .next()
                .is_none()
        );
    }

    #[tokio::test]
    async fn spill_budget_error_does_not_poison_runtime() {
        use std::io::Write;

        use datafusion::{arrow::array::StringArray, datasource::MemTable, physical_plan::collect};

        let spill_root = tempfile::tempdir().expect("spill root");
        let resources =
            QueryResources::try_new(16 * 1024 * 1024, 16 * 1024 * 1024, spill_root.path())
                .expect("valid resources");
        let engine = QueryEngine::try_with_resources(
            Arc::new(CountingMeta::default()),
            Arc::new(LanceEngine::new()),
            resources,
        )
        .expect("bounded Query runtime");
        let runtime = engine.context().runtime_env();
        let context = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_sort_spill_reservation_bytes(512 * 1024),
            runtime.clone(),
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Utf8,
            false,
        )]));
        let batches = (0..256)
            .map(|batch| {
                let values = (0..1024)
                    .rev()
                    .map(|row| format!("{:08}-{}", batch * 1024 + row, "x".repeat(64)))
                    .collect::<Vec<_>>();
                RecordBatch::try_new(schema.clone(), vec![Arc::new(StringArray::from(values))])
                    .expect("input batch")
            })
            .collect::<Vec<_>>();
        context
            .register_table(
                "over_budget",
                Arc::new(MemTable::try_new(schema, vec![batches]).expect("memory table")),
            )
            .expect("register input");
        let frame = context
            .sql("SELECT value FROM over_budget ORDER BY value")
            .await
            .expect("plan sort");
        let plan = frame.create_physical_plan().await.expect("physical sort");
        let error = collect(plan.clone(), context.task_ctx())
            .await
            .expect_err("spill must exceed disk budget");
        assert!(error.to_string().contains("allowable limit"));
        assert_eq!(runtime.disk_manager.spilling_progress().current_bytes, 0);
        assert!(runtime.disk_manager.temp_dir_paths().iter().all(|path| {
            std::fs::read_dir(path)
                .expect("read DataFusion spill directory")
                .next()
                .is_none()
        }));

        let mut quota_probe = runtime
            .disk_manager
            .create_tmp_file("quota recovery probe")
            .expect("disk quota remains usable");
        quota_probe
            .inner()
            .as_file()
            .write_all(b"recovered")
            .expect("write quota probe");
        quota_probe
            .update_disk_usage()
            .expect("reserve quota after rejection");
        drop(quota_probe);
        assert_eq!(runtime.disk_manager.spilling_progress().current_bytes, 0);
        let result = context
            .sql("SELECT 42 AS answer")
            .await
            .expect("plan recovery query")
            .collect()
            .await
            .expect("same runtime remains usable");
        assert_eq!(result.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    }
}
