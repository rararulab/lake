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

//! The metadata layer: the stateful registry authority.
//!
//! [`Metasrv`] owns the write path for the db→table registry — create,
//! resolve, list, and (future) commit coordination. It is a bounded,
//! leader-elected tier, NOT a fan-out one: the query layer shields it behind
//! a cache, so it sees only cache-miss and write traffic. See
//! `docs/architecture.md`.
//!
//! ponytail: v0 is a single instance with no leader election and an
//! in-process API. The gRPC wire surface and lease-in-KV election are v1/v2
//! — the authority logic here is what they will wrap.

//! [`election`] adds the lease-in-KV leader election that gives this tier HA
//! (leader + standby) over the [`MetaStore`](lake_meta::MetaStore) CAS
//! primitive — no self-built consensus.

pub mod election;

use std::sync::Arc;

use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use lake_catalog::create_table;
use lake_common::{Namespace, TableLocation, TableName, TableRef, Version};
use lake_engine::TableEngineRef;
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use snafu::{OptionExt, ResultExt, Snafu};

#[derive(Debug, Snafu)]
#[snafu(visibility(pub))]
pub enum MetasrvError {
    #[snafu(display("registry error"))]
    Registry { source: lake_meta::MetaError },

    #[snafu(display("create-table failed"))]
    Create { source: lake_catalog::CatalogError },

    #[snafu(display("engine error"))]
    Engine { source: lake_engine::EngineError },

    #[snafu(display("table '{table}' not found"))]
    NotFound { table: String },
}

pub type Result<T> = std::result::Result<T, MetasrvError>;

/// The registry authority. Holds the durable metastore and the storage
/// engine used to materialize new tables.
#[derive(Clone)]
pub struct Metasrv {
    meta:   MetaStoreRef,
    engine: TableEngineRef,
}

impl Metasrv {
    pub fn new(meta: MetaStoreRef, engine: TableEngineRef) -> Self { Self { meta, engine } }

    /// Create a table: materialize the dataset via the engine, then register
    /// it (dataset-first, so a registry entry never points at nothing).
    pub async fn create_table(
        &self,
        table: &TableRef,
        location: TableLocation,
        schema: SchemaRef,
    ) -> Result<()> {
        create_table(&self.meta, &self.engine, table, location, schema)
            .await
            .context(CreateSnafu)
    }

    /// Append rows to a table under the commit protocol: the engine writes a
    /// new immutable version, then the registry pointer is CAS-advanced to
    /// it. A lost CAS race surfaces as a registry conflict for the caller to
    /// retry.
    pub async fn append(
        &self,
        table: &TableRef,
        batches: SendableRecordBatchStream,
    ) -> Result<Version> {
        let reg = self.resolve(table).await?.context(NotFoundSnafu {
            table: table.to_string(),
        })?;
        let handle = self
            .engine
            .open(&reg.location)
            .await
            .context(EngineSnafu)?
            .context(NotFoundSnafu {
                table: table.to_string(),
            })?;
        let new_version = handle.append(batches).await.context(EngineSnafu)?;
        registry::set_version(self.meta.as_ref(), table, &reg, new_version)
            .await
            .context(RegistrySnafu)?;
        Ok(new_version)
    }

    /// Resolve a table to its current registration.
    pub async fn resolve(&self, table: &TableRef) -> Result<Option<TableRegistration>> {
        registry::get(self.meta.as_ref(), table)
            .await
            .context(RegistrySnafu)
    }

    /// List the tables in a namespace.
    pub async fn list_tables(&self, namespace: &Namespace) -> Result<Vec<TableName>> {
        registry::list(self.meta.as_ref(), namespace)
            .await
            .context(RegistrySnafu)
    }

    /// List all namespaces.
    pub async fn list_namespaces(&self) -> Result<Vec<Namespace>> {
        registry::list_namespaces(self.meta.as_ref())
            .await
            .context(RegistrySnafu)
    }

    pub fn meta(&self) -> &MetaStoreRef { &self.meta }

    pub fn engine(&self) -> &TableEngineRef { &self.engine }
}

/// Run the metadata server. ponytail: v0 has no gRPC wire and no election —
/// this holds the authority alive so the process form exists; the network
/// surface (tonic) and lease-election land in v1/v2.
pub async fn serve(metasrv: Arc<Metasrv>, addr: &str) -> Result<()> {
    let namespaces = metasrv.list_namespaces().await?;
    tracing::info!(
        %addr,
        namespaces = namespaces.len(),
        "metasrv ready (in-process authority; gRPC wire is v1)"
    );
    // ponytail: replace with a tonic server + graceful shutdown in v1.
    std::future::pending::<()>().await;
    Ok(())
}
