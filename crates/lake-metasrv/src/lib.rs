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
//! primitive — no self-built consensus. [`control`] wraps the authority in an
//! Arrow Flight `DoAction` wire surface, and [`serve`] runs it alongside a
//! background [`leadership`] campaign so writes gate on the lease.

pub mod election;

mod control;
mod leadership;
mod maintenance;

use std::{net::AddrParseError, sync::Arc, time::Duration};

use arrow_flight::flight_service_server::FlightServiceServer;
use datafusion::{arrow::datatypes::SchemaRef, execution::SendableRecordBatchStream};
use lake_catalog::create_table;
use lake_common::{Namespace, TableLocation, TableName, TableRef, Version};
use lake_engine::TableEngineRef;
use lake_meta::{MetaStoreRef, registry, registry::TableRegistration};
use snafu::{OptionExt, ResultExt, Snafu};
use tonic::transport::Server;

use crate::{
    control::MetasrvFlightService,
    election::LeaseElection,
    leadership::{Leadership, run_campaign_loop},
    maintenance::run_maintenance_loop,
};

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

    #[snafu(display("invalid listen address {addr:?}"))]
    Address {
        addr:   String,
        source: AddrParseError,
    },

    #[snafu(display("metasrv control plane server failed"))]
    Serve { source: tonic::transport::Error },
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

    /// Drop a table: delete its data via the engine, then remove the registry
    /// entry. Idempotent — dropping an absent table is a no-op. Data-first so a
    /// crash leaves at worst orphaned data (reclaimable by GC), never a
    /// registry entry pointing at deleted data.
    ///
    /// ponytail: query-layer caches self-heal (a dropped table's dataset is
    /// gone, so `open` returns `None`) and refresh drops it from listings; a
    /// push-based cache invalidation across instances is a v2 concern.
    pub async fn drop_table(&self, table: &TableRef) -> Result<()> {
        let Some(reg) = self.resolve(table).await? else {
            return Ok(());
        };
        self.engine
            .remove(&reg.location)
            .await
            .context(EngineSnafu)?;
        registry::delete(self.meta.as_ref(), table)
            .await
            .context(RegistrySnafu)?;
        Ok(())
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

/// Run the metadata server: the Arrow Flight control plane plus a background
/// leader-election campaign.
///
/// Spawns a campaign loop that renews the lease and publishes leadership into
/// shared state, a leader-only maintenance sweep, then binds a tonic server
/// exposing the control-plane
/// [`FlightService`](arrow_flight::flight_service_server::FlightService) over
/// `DoAction`. Writes that land on a follower are forwarded to the current
/// leader; reads are always served locally. The node id is `addr`, unique
/// enough per instance in dev. Runs until the server stops or the process is
/// killed.
pub async fn serve(metasrv: Arc<Metasrv>, addr: &str) -> Result<()> {
    let election = LeaseElection::new(metasrv.meta().clone(), addr, Duration::from_secs(10));
    let leadership = Arc::new(Leadership::new());
    // The maintenance sweep gates on the same leader flag the campaign loop
    // publishes, so only the current leader does housekeeping.
    tokio::spawn(run_maintenance_loop(
        metasrv.clone(),
        leadership.is_leader_flag(),
    ));
    tokio::spawn(run_campaign_loop(election, leadership.clone()));

    let svc = MetasrvFlightService {
        metasrv,
        leadership,
        own_addr: addr.to_string(),
    };

    let socket = addr.parse().context(AddressSnafu { addr })?;
    tracing::info!(
        %addr,
        "metasrv control plane ready (Flight do_action; writes gated on leadership)"
    );
    Server::builder()
        .add_service(FlightServiceServer::new(svc))
        .serve(socket)
        .await
        .context(ServeSnafu)?;
    Ok(())
}
