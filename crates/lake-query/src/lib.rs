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
use lake_engine::TableEngineRef;
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
}

pub type Result<T> = std::result::Result<T, QueryError>;

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
    serve_inner(engine, addr, None).await
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
    serve_inner(engine, addr, Some(metadata_addr.to_owned())).await
}

async fn serve_inner(
    engine: Arc<QueryEngine>,
    addr: &str,
    metadata_addr: Option<String>,
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
    let service = FlightServiceServer::new(FlightSqlServiceImpl {
        engine,
        metadata_addr,
    });

    tracing::info!(%addr, "Flight SQL server ready");
    tonic::transport::Server::builder()
        .add_service(service)
        .serve(socket)
        .await
        .context(ServeSnafu)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use lake_engine_lance::LanceEngine;
    use lake_meta::{MetaStore, MetaStoreRef};

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
}
