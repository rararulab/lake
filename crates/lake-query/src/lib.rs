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

use std::{sync::Arc, time::Duration};

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::{
    arrow::array::RecordBatch,
    dataframe::DataFrame,
    error::DataFusionError,
    prelude::{SQLOptions, SessionContext},
};
use lake_catalog::LakeCatalog;
use lake_common::ManagedStageDescriptor;
use lake_engine::TableEngineRef;
use lake_flight::{ClientSecurity, ServerSecurity};
use lake_meta::MetaStoreRef;
use snafu::{ResultExt, Snafu};

use crate::flight::FlightSqlServiceImpl;

/// Maximum age of the in-memory catalog listing used on the query hot path.
const CATALOG_MAX_AGE: Duration = Duration::from_secs(5);

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
}

pub type Result<T> = std::result::Result<T, QueryError>;

/// Complete network configuration for one stateless Query server.
#[derive(Clone, Debug)]
pub struct QueryServerConfig {
    metadata_endpoint: Option<String>,
    metadata_security: ClientSecurity,
    managed_stage:     Option<ManagedStageDescriptor>,
    server_security:   ServerSecurity,
    allow_insecure:    bool,
}

impl QueryServerConfig {
    /// Explicit loopback development configuration.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            metadata_endpoint: None,
            metadata_security: ClientSecurity::new(),
            managed_stage:     None,
            server_security:   ServerSecurity::insecure(),
            allow_insecure:    false,
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
        let catalog = LakeCatalog::new(meta, engine);
        let ctx = SessionContext::new();
        ctx.register_catalog("lake", Arc::new(catalog.clone()));
        Self { ctx, catalog }
    }

    /// Force a reload of the catalog's listing snapshot from the registry.
    pub async fn refresh(&self) -> Result<()> { self.catalog.refresh().await.context(RefreshSnafu) }

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
    engine.refresh().await?;
    let refresher = engine.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(CATALOG_MAX_AGE);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Consume the immediate first tick: `serve` just warmed the catalog.
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(err) = refresher.refresh_if_stale().await {
                tracing::warn!(error = %err, "background catalog refresh failed");
            }
        }
    });

    let socket = addr.parse().context(AddressSnafu { addr })?;
    config
        .server_security
        .validate_exposure(socket, config.allow_insecure)
        .context(SecuritySnafu)?;
    let service = FlightServiceServer::new(FlightSqlServiceImpl {
        engine,
        metadata_addr: config.metadata_endpoint,
        metadata_security: config.metadata_security,
        managed_stage: config.managed_stage,
    });

    tracing::info!(%addr, "Flight SQL server ready");
    let mut server = tonic::transport::Server::builder();
    if let Some(tls) = config.server_security.tls_config() {
        server = server.tls_config(tls).context(ServeSnafu)?;
    }
    server
        .layer(tonic::service::InterceptorLayer::new(
            config.server_security.interceptor(),
        ))
        .add_service(service)
        .serve(socket)
        .await
        .context(ServeSnafu)
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    use arrow_flight::{Action, flight_service_client::FlightServiceClient};
    use async_trait::async_trait;
    use futures::TryStreamExt;
    use lake_common::{MANAGED_STAGE_DISCOVERY_ACTION, ManagedStageDescriptor};
    use lake_engine_lance::LanceEngine;
    use lake_flight::{ClientSecurity, ServerSecurity};
    use lake_meta::{MetaStore, MetaStoreRef};
    use rcgen::generate_simple_self_signed;
    use tonic::Request;

    use super::*;

    #[derive(Default)]
    struct CountingMeta {
        scans: AtomicUsize,
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
            descriptor
        );

        server.abort();
    }
}
