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
//! catalog and refreshes on demand, shielding the metadata authority.
//!
//! ponytail: v0 exposes an in-process `execute_sql`; the Arrow Flight SQL
//! wire surface (`serve`) is a v1 skeleton.

use std::sync::Arc;

use datafusion::{arrow::array::RecordBatch, error::DataFusionError, prelude::SessionContext};
use lake_catalog::LakeCatalog;
use lake_engine::TableEngineRef;
use lake_meta::MetaStoreRef;
use snafu::{ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum QueryError {
    #[snafu(display("catalog refresh failed"))]
    Refresh { source: lake_meta::MetaError },

    #[snafu(display("query execution failed"))]
    Execute { source: DataFusionError },
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

    /// Reload the catalog's listing snapshot from the registry. Call before
    /// executing so newly-created tables are visible.
    pub async fn refresh(&self) -> Result<()> { self.catalog.refresh().await.context(RefreshSnafu) }

    /// Execute a SQL statement and collect the results.
    pub async fn execute_sql(&self, sql: &str) -> Result<Vec<RecordBatch>> {
        self.refresh().await?;
        let df = self.ctx.sql(sql).await.context(ExecuteSnafu)?;
        df.collect().await.context(ExecuteSnafu)
    }

    pub fn context(&self) -> &SessionContext { &self.ctx }
}

/// Run the query server. ponytail: v0 has no Flight SQL wire — this holds a
/// warmed engine alive so the process form exists; the Arrow Flight SQL
/// endpoint lands in v1.
pub async fn serve(engine: Arc<QueryEngine>, addr: &str) -> Result<()> {
    engine.refresh().await?;
    tracing::info!(%addr, "query server ready (in-process execute_sql; Flight SQL wire is v1)");
    // ponytail: replace with an arrow-flight FlightSqlService in v1.
    std::future::pending::<()>().await;
    Ok(())
}
